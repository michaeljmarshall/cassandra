/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.io.compress;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.Optional;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.storage.StorageProvider;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.utils.Throwables.merge;

public class CompressedSequentialWriter extends SequentialWriter
{
    protected static final Logger logger = LoggerFactory.getLogger(CompressedSequentialWriter.class);

    private final ChecksumWriter crcMetadata;

    // holds offset in the file where current chunk should be written
    // changed only by flush() method where data buffer gets compressed and stored to the file
    private long chunkOffset = 0;

    // index file writer (random I/O)
    private final CompressionMetadata.Writer metadataWriter;
    private final ICompressor compressor;

    // used to store compressed data
    private ByteBuffer compressed;

    // holds a number of already written chunks
    private int chunkCount = 0;

    private final MetadataCollector sstableMetadataCollector;

    private final ByteBuffer crcCheckBuffer = ByteBuffer.allocate(4);
    private final Optional<File> digestFile;

    private final int maxCompressedLength;

    /**
     * When corruption is found, the file writer is reset to previous data point but we can't reset the CRC checksum.
     * So we have to recompute digest value.
     */
    private boolean recomputeChecksum = false;

    /**
     * Create CompressedSequentialWriter without digest file.
     *
     * @param file File to write
     * @param offsetsPath File name to write compression metadata
     * @param digestFile File to write digest
     * @param option Write option (buffer size and type will be set the same as compression params)
     * @param parameters Compression mparameters
     * @param sstableMetadataCollector Metadata collector
     */
    public CompressedSequentialWriter(File file,
                                      File offsetsPath,
                                      File digestFile,
                                      SequentialWriterOption option,
                                      CompressionParams parameters,
                                      MetadataCollector sstableMetadataCollector)
    {
        super(file, SequentialWriterOption.newBuilder()
                                          .bufferSize(option.bufferSize())
                                          .bufferType(option.bufferType())
                                          .bufferSize(parameters.chunkLength())
                                          .bufferType(parameters.getSstableCompressor().preferredBufferType())
                                          .finishOnClose(option.finishOnClose())
                                          .build());
        this.compressor = parameters.getSstableCompressor();
        this.digestFile = Optional.ofNullable(digestFile);

        // buffer for compression should be the same size as buffer itself
        compressed = compressor.preferredBufferType().allocate(compressor.initialCompressedBufferLength(buffer.capacity()));

        maxCompressedLength = parameters.maxCompressedLength();

        /* Index File (-CompressionInfo.db component) and it's header */
        metadataWriter = CompressionMetadata.Writer.open(parameters, offsetsPath);

        this.sstableMetadataCollector = sstableMetadataCollector;
        crcMetadata = new ChecksumWriter(new DataOutputStream(Channels.newOutputStream(channel)));
    }

    @Override
    public long getOnDiskFilePointer()
    {
        try
        {
            return fchannel.position();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getFile());
        }
    }

    /**
     * Get a quick estimation on how many bytes have been written to disk
     *
     * It should for the most part be exactly the same as getOnDiskFilePointer()
     */
    @Override
    public long getEstimatedOnDiskBytesWritten()
    {
        return chunkOffset;
    }

    @Override
    public void flush()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void flushData()
    {
        seekToChunkStart(); // why is this necessary? seems like it should always be at chunk start in normal operation

        if (buffer.limit() == 0)
        {
            // nothing to compress
            if (runPostFlush != null)
                runPostFlush.run();

            return;
        }

        try
        {
            // compressing data with buffer re-use
            buffer.flip();
            compressed.clear();
            compressor.compress(buffer, compressed);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Compression exception", e); // shouldn't happen
        }

        int uncompressedLength = buffer.position();
        int compressedLength = compressed.position();
        ByteBuffer toWrite = compressed;
        if (compressedLength >= maxCompressedLength)
        {
            toWrite = buffer;
            if (uncompressedLength >= maxCompressedLength)
            {
                compressedLength = uncompressedLength;
            }
            else
            {
                // Pad the uncompressed data so that it reaches the max compressed length.
                // This could make the chunk appear longer, but this path is only reached at the end of the file, where
                // we use the file size to limit the buffer on reading.
                assert maxCompressedLength <= buffer.capacity();   // verified by CompressionParams.validate
                buffer.limit(maxCompressedLength);
                ByteBufferUtil.writeZeroes(buffer, maxCompressedLength - uncompressedLength);
                compressedLength = maxCompressedLength;
            }
        }

        try
        {
            // write an offset of the newly written chunk to the index file
            metadataWriter.addOffset(chunkOffset);
            chunkCount++;

            // write out the compressed data
            toWrite.flip();
            channel.write(toWrite);

            // write corresponding checksum
            toWrite.rewind();
            crcMetadata.appendDirect(toWrite, true);
            lastFlushOffset += uncompressedLength;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getFile());
        }
        if (toWrite == buffer)
            buffer.position(uncompressedLength);

        // next chunk should be written right after current + length of the checksum (int)
        chunkOffset += compressedLength + 4;
        if (runPostFlush != null)
            runPostFlush.run();
    }

    public CompressionMetadata open(long overrideLength)
    {
        if (overrideLength <= 0)
            overrideLength = lastFlushOffset;
        return metadataWriter.open(overrideLength, chunkOffset);
    }

    @Override
    public DataPosition mark()
    {
        if (!buffer.hasRemaining())
            doFlush(0);
        return new CompressedFileWriterMark(chunkOffset, current(), buffer.position(), chunkCount + 1);
    }

    @Override
    public synchronized void resetAndTruncate(DataPosition mark)
    {
        try
        {
            doResetAndTruncate(mark);
        }
        catch (Throwable t)
        {
            CompressedFileWriterMark realMark = mark instanceof CompressedFileWriterMark ? (CompressedFileWriterMark) mark : null;
            logger.error("Failed to reset and truncate {} at chunk offset {} because of {}", file.name(),
                         realMark == null ? -1 : realMark.chunkOffset, t.getMessage());
            throw t;
        }
    }

    private synchronized void doResetAndTruncate(DataPosition mark)
    {
        assert mark instanceof CompressedFileWriterMark;

        CompressedFileWriterMark realMark = (CompressedFileWriterMark) mark;

        // reset position
        long truncateTarget = realMark.uncDataOffset;

        if (realMark.chunkOffset == chunkOffset)
        {
            // simply drop bytes to the right of our mark
            buffer.position(realMark.validBufferBytes);
            return;
        }

        // synchronize current buffer with disk - we don't want any data loss
        syncInternal();

        chunkOffset = realMark.chunkOffset;

        // compressed chunk size (- 4 bytes reserved for checksum)
        int chunkSize = (int) (metadataWriter.chunkOffsetBy(realMark.nextChunkIndex) - chunkOffset - 4);
        if (compressed.capacity() < chunkSize)
        {
            FileUtils.clean(compressed);
            compressed = compressor.preferredBufferType().allocate(chunkSize);
        }

        try(FileChannel readChannel = StorageProvider.instance.writeTimeReadFileChannelFor(getFile()))
        {
            compressed.clear();
            compressed.limit(chunkSize);
            readChannel.position(chunkOffset);
            readChannel.read(compressed);

            try
            {
                // Repopulate buffer from compressed data
                buffer.clear();
                compressed.flip();
                if (chunkSize < maxCompressedLength)
                    compressor.uncompress(compressed, buffer);
                else
                    buffer.put(compressed);
            }
            catch (IOException e)
            {
                throw new CorruptBlockException(getFile().toString(), chunkOffset, chunkSize, e);
            }

            CRC32 checksum = new CRC32();
            compressed.rewind();
            checksum.update(compressed);

            crcCheckBuffer.clear();
            readChannel.read(crcCheckBuffer);
            crcCheckBuffer.flip();
            int storedChecksum = crcCheckBuffer.getInt();
            int computedChecksum = (int) checksum.getValue();
            if (storedChecksum != computedChecksum)
                throw new CorruptBlockException(getFile().toString(), chunkOffset, chunkSize, storedChecksum, computedChecksum);
        }
        catch (CorruptBlockException e)
        {
            throw new CorruptSSTableException(e, getFile());
        }
        catch (EOFException e)
        {
            throw new CorruptSSTableException(new CorruptBlockException(getFile().toString(), chunkOffset, chunkSize), getFile());
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getFile());
        }

        // Mark as dirty so we can guarantee the newly buffered bytes won't be lost on a rebuffer
        buffer.position(realMark.validBufferBytes);

        bufferOffset = truncateTarget - buffer.position();
        chunkCount = realMark.nextChunkIndex - 1;

        // truncate data and index file. Unfortunately we can't reset and truncate CRC value, we have to recompute
        // the CRC value otherwise it won't match the actual file checksum
        recomputeChecksum = true;
        truncate(chunkOffset, bufferOffset);
        metadataWriter.resetAndTruncate(realMark.nextChunkIndex - 1);

        logger.info("reset and truncated {} to {}", file, chunkOffset);
    }

    private void truncate(long toFileSize, long toBufferOffset)
    {
        try
        {
            fchannel.truncate(toFileSize);
            lastFlushOffset = toBufferOffset;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getFile());
        }
    }

    /**
     * Seek to the offset where next compressed data chunk should be stored.
     */
    private void seekToChunkStart()
    {
        if (getOnDiskFilePointer() != chunkOffset)
        {
            try
            {
                fchannel.position(chunkOffset);
            }
            catch (IOException e)
            {
                throw new FSReadError(e, getFile());
            }
        }
    }

    // Page management using chunk boundaries

    @Override
    public int maxBytesInPage()
    {
        return buffer.capacity();
    }

    @Override
    public void padToPageBoundary() throws IOException
    {
        if (buffer.position() == 0)
            return;

        int padLength = buffer.remaining();

        // Flush as much as we have
        doFlush(0);
        // But pretend we had a whole chunk
        bufferOffset += padLength;
        lastFlushOffset += padLength;
    }

    @Override
    public int bytesLeftInPage()
    {
        return buffer.remaining();
    }

    @Override
    public long paddedPosition()
    {
        return position() + (buffer.position() == 0 ? 0 : buffer.remaining());
    }

    protected class TransactionalProxy extends SequentialWriter.TransactionalProxy
    {
        @Override
        protected Throwable doCommit(Throwable accumulate)
        {
            return super.doCommit(metadataWriter.commit(accumulate));
        }

        @Override
        protected Throwable doAbort(Throwable accumulate)
        {
            return super.doAbort(metadataWriter.abort(accumulate));
        }

        @Override
        protected void doPrepare()
        {
            syncInternal();
            maybeWriteChecksum();

            sstableMetadataCollector.addCompressionRatio(chunkOffset, lastFlushOffset);
            metadataWriter.finalizeLength(current(), chunkCount).prepareToCommit();
        }

        @Override
        protected Throwable doPreCleanup(Throwable accumulate)
        {
            accumulate = super.doPreCleanup(accumulate);
            if (compressed != null)
            {
                try { FileUtils.clean(compressed); }
                catch (Throwable t) { accumulate = merge(accumulate, t); }
                compressed = null;
            }

            return accumulate;
        }
    }

    private void maybeWriteChecksum()
    {
        if (digestFile.isEmpty())
            return;

        File digest = digestFile.get();
        if (recomputeChecksum)
        {
            logger.info("Rescanning data file to populate digest into {} because file writer has been reset and truncated", digest);
            try (FileChannel fileChannel = StorageProvider.instance.writeTimeReadFileChannelFor(file);
                 InputStream stream = Channels.newInputStream(fileChannel))
            {
                CRC32 checksum = new CRC32();
                try (CheckedInputStream checkedInputStream = new CheckedInputStream(stream, checksum))
                {
                    byte[] chunk = new byte[64 * 1024];
                    while (checkedInputStream.read(chunk) >= 0) {}

                    long digestValue = checkedInputStream.getChecksum().getValue();
                    ChecksumWriter.writeFullChecksum(digest, digestValue);
                }
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, digest);
            }
            logger.info("Successfully recomputed checksum for {}", digest);
        }
        else
        {
            crcMetadata.writeFullChecksum(digest);
        }
    }

    @Override
    protected SequentialWriter.TransactionalProxy txnProxy()
    {
        return new TransactionalProxy();
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class CompressedFileWriterMark implements DataPosition
    {
        // chunk offset in the compressed file
        final long chunkOffset;
        // uncompressed data offset (real data offset)
        final long uncDataOffset;

        final int validBufferBytes;
        final int nextChunkIndex;

        public CompressedFileWriterMark(long chunkOffset, long uncDataOffset, int validBufferBytes, int nextChunkIndex)
        {
            this.chunkOffset = chunkOffset;
            this.uncDataOffset = uncDataOffset;
            this.validBufferBytes = validBufferBytes;
            this.nextChunkIndex = nextChunkIndex;
        }
    }
}
