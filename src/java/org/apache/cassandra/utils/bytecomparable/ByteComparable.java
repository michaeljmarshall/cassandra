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

package org.apache.cassandra.utils.bytecomparable;

import java.nio.ByteBuffer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Interface indicating a value can be represented/identified by a comparable {@link ByteSource}.
 *
 * All Cassandra types that can be used as part of a primary key have a corresponding byte-comparable translation,
 * detailed in ByteComparable.md. Byte-comparable representations are used in some memtable as well as primary and
 * secondary index implementations.
 */
public interface ByteComparable
{
    /**
     * Returns a source that generates the byte-comparable representation of the value byte by byte.
     */
    ByteSource asComparableBytes(Version version);

    default ByteSource.Peekable asPeekableBytes(Version version)
    {
        return ByteSource.peekable(asComparableBytes(version));
    }

    enum Version
    {
        LEGACY,
        OSS41,  // CASSANDRA 4.1 encoding, used in trie-based indices
        OSS50,  // CASSANDRA 5.0 encoding, used by the trie memtable
    }

    ByteComparable EMPTY = (Version version) -> ByteSource.EMPTY;

    /**
     * Construct a human-readable string from the byte-comparable representation. Used for debugging.
     */
    default String byteComparableAsString(Version version)
    {
        StringBuilder builder = new StringBuilder();
        ByteSource stream = asComparableBytes(version);
        if (stream == null)
            return "null";
        for (int b = stream.next(); b != ByteSource.END_OF_STREAM; b = stream.next())
            builder.append(Integer.toHexString((b >> 4) & 0xF)).append(Integer.toHexString(b & 0xF));
        return builder.toString();
    }

    /**
     * Returns the full byte-comparable representation of the value as a byte array.
     */
    default byte[] asByteComparableArray(Version version)
    {
        return ByteSourceInverse.readBytes(asComparableBytes(version));
    }

    // Simple factories used for testing

    @VisibleForTesting
    static ByteComparable of(String s)
    {
        // Note: This is not prefix-free
        return v -> ByteSource.of(s, v);
    }

    static ByteComparable of(long value)
    {
        return v -> ByteSource.of(value);
    }

    static ByteComparable of(int value)
    {
        return v -> ByteSource.of(value);
    }

    private static void checkVersion(Version expected, Version actual)
    {
        Preconditions.checkState(actual == expected,
                                 "Preprocessed byte-source at version %s queried at version %s",
                                 actual,
                                 expected);
    }

    /**
     * A ByteComparable value that is already encoded for a specific version. Requesting the source with a different
     * version will result in an exception.
     */
    static ByteComparable preencoded(Version version, ByteBuffer bytes)
    {
        return v -> {
            checkVersion(version, v);
            return ByteSource.preencoded(bytes);
        };
    }

    /**
     * A ByteComparable value that is already encoded for a specific version. Requesting the source with a different
     * version will result in an exception.
     */
    static ByteComparable preencoded(Version version, byte[] bytes)
    {
        return v -> {
            checkVersion(version, v);
            return ByteSource.preencoded(bytes);
        };
    }

    /**
     * A ByteComparable value that is already encoded for a specific version. Requesting the source with a different
     * version will result in an exception.
     */
    static ByteComparable preencoded(Version version, byte[] bytes, int offset, int len)
    {
        return v -> {
            checkVersion(version, v);
            return ByteSource.preencoded(bytes, offset, len);
        };
    }

    /**
     * Returns a separator for two byte sources, i.e. something that is definitely > prevMax, and <= currMin, assuming
     * prevMax < currMin.
     * This returns the shortest prefix of currMin that is greater than prevMax.
     */
    static ByteComparable separatorPrefix(ByteComparable prevMax, ByteComparable currMin)
    {
        return version -> ByteSource.separatorPrefix(prevMax.asComparableBytes(version), currMin.asComparableBytes(version));
    }

    /**
     * Returns a separator for two byte comparable, i.e. something that is definitely > prevMax, and <= currMin, assuming
     * prevMax < currMin.
     * This is a stream of length 1 longer than the common prefix of the two streams, with last byte one higher than the
     * prevMax stream.
     */
    static ByteComparable separatorGt(ByteComparable prevMax, ByteComparable currMin)
    {
        return version -> ByteSource.separatorGt(prevMax.asComparableBytes(version), currMin.asComparableBytes(version));
    }

    static ByteComparable cut(ByteComparable src, int cutoff)
    {
        return version -> ByteSource.cut(src.asComparableBytes(version), cutoff);
    }

    /**
     * Return the length of a byte comparable, not including the terminator byte.
     */
    static int length(ByteComparable src, Version version)
    {
        int l = 0;
        ByteSource s = src.asComparableBytes(version);
        while (s.next() != ByteSource.END_OF_STREAM)
            ++l;
        return l;
    }

    /**
     * Compare two byte-comparable values by their byte-comparable representation. Used for tests.
     *
     * @return the result of the lexicographic unsigned byte comparison of the byte-comparable representations of the
     *         two arguments
     */
    static int compare(ByteComparable bytes1, ByteComparable bytes2, Version version)
    {
        ByteSource s1 = bytes1.asComparableBytes(version);
        ByteSource s2 = bytes2.asComparableBytes(version);

        if (s1 == null || s2 == null)
            return Boolean.compare(s1 != null, s2 != null);

        while (true)
        {
            int b1 = s1.next();
            int b2 = s2.next();
            int cmp = Integer.compare(b1, b2);
            if (cmp != 0)
                return cmp;
            if (b1 == ByteSource.END_OF_STREAM)
                return 0;
        }
    }

    /**
     * Returns the length of the minimum prefix that differentiates the two given byte-comparable representations.
     */
    static int diffPoint(ByteComparable bytes1, ByteComparable bytes2, Version version)
    {
        ByteSource s1 = bytes1.asComparableBytes(version);
        ByteSource s2 = bytes2.asComparableBytes(version);
        int pos = 1;
        int b;
        while ((b = s1.next()) == s2.next() && b != ByteSource.END_OF_STREAM)
            ++pos;
        return pos;
    }
}
