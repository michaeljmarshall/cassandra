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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.io.IndexInput;
import org.apache.cassandra.index.sai.disk.v1.postings.MergePostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.disk.v1.postings.ScanningPostingsReader;
import org.apache.cassandra.index.sai.disk.v1.trie.TrieTermsDictionaryReader;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.apache.cassandra.index.sai.utils.SAICodecUtils.validate;

/**
 * Synchronous reader of terms dictionary and postings lists to produce a {@link PostingList} with matching row ids.
 *
 * {@link #exactMatch(ByteComparable, QueryEventListener.TrieIndexEventListener, QueryContext)} does:
 * <ul>
 * <li>{@link TermQuery#lookupTermDictionary(ByteComparable)}: does term dictionary lookup to find the posting list file
 * position</li>
 * <li>{@link TermQuery#getPostingReader(long)}: reads posting list block summary and initializes posting read which
 * reads the first block of the posting list into memory</li>
 * </ul>
 */
public class TermsReader implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final IndexContext indexContext;
    private final FileHandle termDictionaryFile;
    private final FileHandle postingsFile;
    private final long termDictionaryRoot;

    public TermsReader(IndexContext indexContext,
                       FileHandle termsData,
                       FileHandle postingLists,
                       long root,
                       long termsFooterPointer) throws IOException
    {
        this.indexContext = indexContext;
        termDictionaryFile = termsData;
        postingsFile = postingLists;
        termDictionaryRoot = root;

        try (final IndexInput indexInput = IndexFileUtils.instance.openInput(termDictionaryFile))
        {
            // if the pointer is -1 then this is a previous version of the index
            // use the old way to validate the footer
            // the footer pointer is used due to encrypted indexes padding extra bytes
            if (termsFooterPointer == -1)
            {
                validate(indexInput);
            }
            else
            {
                validate(indexInput, termsFooterPointer);
            }
        }

        try (final IndexInput indexInput = IndexFileUtils.instance.openInput(postingsFile))
        {
            validate(indexInput);
        }
    }

    @Override
    public void close()
    {
        try
        {
            termDictionaryFile.close();
        }
        finally
        {
            postingsFile.close();
        }
    }

    public TermsIterator allTerms(long segmentOffset)
    {
        // blocking, since we use it only for segment merging for now
        return new TermsScanner(segmentOffset);
    }

    public PostingList exactMatch(ByteComparable term, QueryEventListener.TrieIndexEventListener perQueryEventListener, QueryContext context)
    {
        perQueryEventListener.onSegmentHit();
        return new TermQuery(term, perQueryEventListener, context).execute();
    }

    /**
     * Range query that uses the expression to find the posting lists for the terms that fall within the range.
     * Intended for use with composite types when the value wasn't encoded correctly, so the expression is tested
     * against the value.
     */
    public PostingList rangeMatch(Expression exp, QueryEventListener.TrieIndexEventListener perQueryEventListener, QueryContext context)
    {
        perQueryEventListener.onSegmentHit();
        return new RangeQuery(exp, perQueryEventListener, context).execute();
    }

    /**
     * Range query that uses the lower and upper bounds to find the posting lists for the terms that fall within the
     * range.
     */
    public PostingList rangeMatch(ByteComparable lower, ByteComparable upper, QueryEventListener.TrieIndexEventListener perQueryEventListener, QueryContext context)
    {
        perQueryEventListener.onSegmentHit();
        return new RangeQuery(lower, upper, perQueryEventListener, context).execute();
    }

    @VisibleForTesting
    public class TermQuery
    {
        private final IndexInput postingsInput;
        private final IndexInput postingsSummaryInput;
        private final QueryEventListener.TrieIndexEventListener listener;
        private final long lookupStartTime;
        private final QueryContext context;

        private ByteComparable term;

        TermQuery(ByteComparable term, QueryEventListener.TrieIndexEventListener listener, QueryContext context)
        {
            this.listener = listener;
            postingsInput = IndexFileUtils.instance.openInput(postingsFile);
            postingsSummaryInput = IndexFileUtils.instance.openInput(postingsFile);
            this.term = term;
            lookupStartTime = System.nanoTime();
            this.context = context;
        }

        public PostingList execute()
        {
            try
            {
                long postingOffset = lookupTermDictionary(term);
                if (postingOffset == PostingList.OFFSET_NOT_FOUND)
                {
                    FileUtils.closeQuietly(postingsInput);
                    FileUtils.closeQuietly(postingsSummaryInput);
                    return null;
                }

                context.checkpoint();

                // when posting is found, resources will be closed when posting reader is closed.
                return getPostingReader(postingOffset);
            }
            catch (Throwable e)
            {
                //TODO Is there an equivalent of AOE in OS?
                if (!(e instanceof AbortedOperationException))
                    logger.error(indexContext.logMessage("Failed to execute term query"), e);

                closeOnException();
                throw Throwables.cleaned(e);
            }
        }

        private void closeOnException()
        {
            FileUtils.closeQuietly(postingsInput);
            FileUtils.closeQuietly(postingsSummaryInput);
        }

        public long lookupTermDictionary(ByteComparable term)
        {
            try (TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(), termDictionaryRoot))
            {
                final long offset = reader.exactMatch(term);

                listener.onTraversalComplete(System.nanoTime() - lookupStartTime, TimeUnit.NANOSECONDS);

                if (offset == TrieTermsDictionaryReader.NOT_FOUND)
                    return PostingList.OFFSET_NOT_FOUND;

                return offset;
            }
        }

        public PostingsReader getPostingReader(long offset) throws IOException
        {
            PostingsReader.BlocksSummary header = new PostingsReader.BlocksSummary(postingsSummaryInput, offset);

            return new PostingsReader(postingsInput, header, listener.postingListEventListener());
        }
    }

    public class RangeQuery
    {
        private final QueryEventListener.TrieIndexEventListener listener;
        private final long lookupStartTime;
        private final QueryContext context;

        private final Expression exp;
        private final ByteComparable lower;
        private final ByteComparable upper;

        // When provided with the lower and the upper, we do not need to do any validation afterwards
        RangeQuery(ByteComparable lower,
                   ByteComparable upper,
                   QueryEventListener.TrieIndexEventListener listener,
                   QueryContext context)
        {
            this.listener = listener;
            this.exp = null;
            lookupStartTime = System.nanoTime();
            this.context = context;
            this.lower = lower;
            this.upper = upper;
        }

        RangeQuery(Expression exp, QueryEventListener.TrieIndexEventListener listener, QueryContext context)
        {
            this.listener = listener;
            this.exp = exp;
            lookupStartTime = System.nanoTime();
            this.context = context;
            // This works by creating an iterator over all the map entries for a given key and then filtering
            // the results in the materializeResults method.
            this.lower = exp.lower != null ? ByteComparable.fixedLength(exp.getLowerBound()) : null;
            this.upper = exp.upper != null ? ByteComparable.fixedLength(exp.getUpperBound()) : null;
        }

        public PostingList execute()
        {
            // Note: we always pass true for include start because we use the ByteComparable terminator above
            // to selectively determine when we have a match on the first/last term. This is probably part of the API
            // that could change, but it's been there for a bit, so we'll leave it for now.
            try (TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(),
                                                                                  termDictionaryRoot,
                                                                                  lower,
                                                                                  upper,
                                                                                  true,
                                                                                  exp != null))
            {
                if (!reader.hasNext())
                    return PostingList.EMPTY;

                context.checkpoint();
                PostingList postings = exp == null
                                       ? readAndMergePostings(reader)
                                       : readValidateValueAndMergePostings(reader);

                listener.onTraversalComplete(System.nanoTime() - lookupStartTime, TimeUnit.NANOSECONDS);

                return postings;
            }
            catch (Throwable e)
            {
                if (!(e instanceof AbortedOperationException))
                    logger.error(indexContext.logMessage("Failed to execute term query"), e);

                throw Throwables.cleaned(e);
            }
        }

        /**
         * Reads the posting lists for the matching terms and merges them into a single posting list.
         * It assumes that the posting list for each term is sorted.
         *
         * @return the posting lists for the terms matching the query.
         */
        private PostingList readAndMergePostings(TrieTermsDictionaryReader reader) throws IOException
        {
            assert reader.hasNext();
            ArrayList<PostingList.PeekablePostingList> postingLists = new ArrayList<>();

            // index inputs will be closed with the onClose method of the returned merged posting list
            IndexInput postingsInput = IndexFileUtils.instance.openInput(postingsFile);
            IndexInput postingsSummaryInput = IndexFileUtils.instance.openInput(postingsFile);

            do
            {
                long postingsOffset = reader.nextAsLong();
                var currentReader = currentReader(postingsInput, postingsSummaryInput, postingsOffset);

                if (!currentReader.isEmpty())
                    postingLists.add(new PostingList.PeekablePostingList(currentReader));
                else
                    FileUtils.close(currentReader);
            } while (reader.hasNext());

            return MergePostingList.merge(postingLists)
                                   .onClose(() -> FileUtils.close(postingsInput, postingsSummaryInput));
        }

        /**
         * Reads the posting lists for the matching terms and merges them into a single posting list.
         * It assumes that the posting list for each term is sorted.
         *
         * @return the posting lists for the terms matching the query.
         */
        private PostingList readValidateValueAndMergePostings(TrieTermsDictionaryReader reader) throws IOException
        {
            assert reader.hasNext();
            ArrayList<PostingList.PeekablePostingList> postingLists = new ArrayList<>();

            // index inputs will be closed with the onClose method of the returned merged posting list
            IndexInput postingsInput = IndexFileUtils.instance.openInput(postingsFile);
            IndexInput postingsSummaryInput = IndexFileUtils.instance.openInput(postingsFile);

            do
            {
                Pair<ByteComparable, Long> nextTriePair = reader.next();
                ByteSource mapEntry = nextTriePair.left.asComparableBytes(ByteComparable.Version.OSS41);
                long postingsOffset = nextTriePair.right;
                byte[] nextBytes = ByteSourceInverse.readBytes(mapEntry);

                if (exp.isSatisfiedBy(ByteBuffer.wrap(nextBytes)))
                {
                    var currentReader = currentReader(postingsInput, postingsSummaryInput, postingsOffset);

                    if (!currentReader.isEmpty())
                        postingLists.add(new PostingList.PeekablePostingList(currentReader));
                    else
                        FileUtils.close(currentReader);
                }
            } while (reader.hasNext());

            return MergePostingList.merge(postingLists)
                                   .onClose(() -> FileUtils.close(postingsInput, postingsSummaryInput));
        }

        private PostingsReader currentReader(IndexInput postingsInput,
                                             IndexInput postingsSummaryInput,
                                             long postingsOffset) throws IOException
        {
            var blocksSummary = new PostingsReader.BlocksSummary(postingsSummaryInput,
                                                                 postingsOffset,
                                                                 PostingsReader.InputCloser.NOOP);
            return new PostingsReader(postingsInput,
                                      blocksSummary,
                                      listener.postingListEventListener(),
                                      PostingsReader.InputCloser.NOOP);
        }
    }

    // currently only used for testing
    private class TermsScanner implements TermsIterator
    {
        private final long segmentOffset;
        private final TrieTermsDictionaryReader termsDictionaryReader;
        private final ByteBuffer minTerm, maxTerm;
        private Pair<ByteComparable, Long> entry;

        private TermsScanner(long segmentOffset)
        {
            this.termsDictionaryReader = new TrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(), termDictionaryRoot);
            this.minTerm = ByteBuffer.wrap(ByteSourceInverse.readBytes(termsDictionaryReader.getMinTerm().asComparableBytes(ByteComparable.Version.OSS41)));
            this.maxTerm = ByteBuffer.wrap(ByteSourceInverse.readBytes(termsDictionaryReader.getMaxTerm().asComparableBytes(ByteComparable.Version.OSS41)));
            this.segmentOffset = segmentOffset;
        }

        @Override
        @SuppressWarnings("resource")
        public PostingList postings() throws IOException
        {
            assert entry != null;
            final IndexInput input = IndexFileUtils.instance.openInput(postingsFile);
            return new OffsetPostingList(segmentOffset, new ScanningPostingsReader(input, new PostingsReader.BlocksSummary(input, entry.right)));
        }

        @Override
        public void close()
        {
            termsDictionaryReader.close();
        }

        @Override
        public ByteBuffer getMinTerm()
        {
            return minTerm;
        }

        @Override
        public ByteBuffer getMaxTerm()
        {
            return maxTerm;
        }

        @Override
        public ByteComparable next()
        {
            if (termsDictionaryReader.hasNext())
            {
                entry = termsDictionaryReader.next();
                return entry.left;
            }
            return null;
        }

        @Override
        public boolean hasNext()
        {
            return termsDictionaryReader.hasNext();
        }
    }

    private class OffsetPostingList implements PostingList
    {
        private final long offset;
        private final PostingList wrapped;

        OffsetPostingList(long offset, PostingList postingList)
        {
            this.offset = offset;
            this.wrapped = postingList;
        }

        @Override
        public long nextPosting() throws IOException
        {
            long next = wrapped.nextPosting();
            if (next == PostingList.END_OF_STREAM)
                return next;
            return next + offset;
        }

        @Override
        public long size()
        {
            return wrapped.size();
        }

        @Override
        public long advance(long targetRowID) throws IOException
        {
            long next = wrapped.advance(targetRowID);
            if (next == PostingList.END_OF_STREAM)
                return next;
            return next + offset;
        }
    }
}
