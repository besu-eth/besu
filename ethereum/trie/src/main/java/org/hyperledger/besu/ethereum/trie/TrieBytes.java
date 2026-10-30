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
 * Concatenation for trie locations and state keys. {@link Bytes#concatenate(Bytes...)} sizes its
 * result with a stream, which allocates a pipeline on every call; these copy directly.
 */
public final class TrieBytes {

  private TrieBytes() {}

  /**
   * The bytes of {@code first} followed by those of {@code second}, in a new value.
   *
   * @param first the leading bytes
   * @param second the trailing bytes
   * @return the concatenation
   */
  public static Bytes concat(final Bytes first, final Bytes second) {
    final int firstSize = first.size();
    final MutableBytes result = MutableBytes.create(firstSize + second.size());
    first.copyTo(result, 0);
    second.copyTo(result, firstSize);
    return result;
  }

  /**
   * The bytes of {@code location} followed by one nibble, in a new value.
   *
   * @param location the leading bytes
   * @param nibble the byte to append
   * @return the extended location
   */
  public static Bytes append(final Bytes location, final byte nibble) {
    final int size = location.size();
    final MutableBytes result = MutableBytes.create(size + 1);
    location.copyTo(result, 0);
    result.set(size, nibble);
    return result;
  }
}
