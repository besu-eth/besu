/*
 * Copyright contributors to Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.trie;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.MutableBytes;

/**
 * Joins byte sequences for trie node locations and storage keys. {@link
 * Bytes#concatenate(Bytes...)} sizes its result with a stream, and on the state read and root paths
 * it runs for every key and every decoded branch child, often enough that its allocations leave
 * threads queueing for heap.
 */
public final class BytesConcatenation {

  private BytesConcatenation() {}

  /**
   * The bytes of {@code first} followed by those of {@code second}.
   *
   * @param first the leading bytes
   * @param second the trailing bytes
   * @return a new sequence holding both
   */
  public static Bytes concatenate(final Bytes first, final Bytes second) {
    final int firstSize = first.size();
    final MutableBytes result = MutableBytes.create(firstSize + second.size());
    first.copyTo(result, 0);
    second.copyTo(result, firstSize);
    return result;
  }

  /**
   * The bytes of {@code prefix} followed by one more byte, such as a branch child's nibble.
   *
   * @param prefix the leading bytes
   * @param last the byte to append
   * @return a new sequence one byte longer than {@code prefix}
   */
  public static Bytes append(final Bytes prefix, final byte last) {
    final int prefixSize = prefix.size();
    final MutableBytes result = MutableBytes.create(prefixSize + 1);
    prefix.copyTo(result, 0);
    result.set(prefixSize, last);
    return result;
  }
}
