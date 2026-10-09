/*
 * Copyright contributors to Hyperledger Besu.
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class NullNode<V> implements Node<V> {
  @SuppressWarnings("rawtypes")
  private static final NullNode instance = new NullNode();

  private static final byte[] ENCODED = {TrieRlp.NULL};
  private static final byte[] HASH = Keccak256.hash(ENCODED);

  protected NullNode() {}

  @SuppressWarnings("unchecked")
  public static <V> NullNode<V> instance() {
    return instance;
  }

  /** Whether {@code hash} is the hash of an empty trie. */
  public static boolean isEmptyTrieHash(final byte[] hash) {
    return Arrays.equals(hash, HASH);
  }

  @Override
  public Node<V> accept(final PathNodeVisitor<V> visitor, final byte[] path, final int offset) {
    return visitor.visit(this, path, offset);
  }

  @Override
  public void accept(final NodeVisitor<V> visitor) {
    visitor.visit(this);
  }

  @Override
  public void accept(final byte[] location, final LocationNodeVisitor<V> visitor) {
    visitor.visit(location, this);
  }

  @Override
  public byte[] path() {
    return Nibbles.EMPTY;
  }

  @Override
  public Optional<V> getValue() {
    return Optional.empty();
  }

  @Override
  public List<Node<V>> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public byte[] encoded() {
    return ENCODED;
  }

  @Override
  public boolean isReferencedByHash() {
    return false;
  }

  @Override
  public int encodedRefSize() {
    return 1;
  }

  @Override
  public int writeEncodedRef(final byte[] out, final int pos) {
    out[pos] = TrieRlp.NULL;
    return pos + 1;
  }

  @Override
  public byte[] encodedRef() {
    return ENCODED;
  }

  @Override
  public byte[] hash() {
    return HASH;
  }

  @Override
  public Node<V> replacePath(final byte[] path) {
    return this;
  }

  @Override
  public String print() {
    return "[NULL]";
  }

  @Override
  public boolean isDirty() {
    return false;
  }

  @Override
  public void markDirty() {
    // do nothing
  }

  @Override
  public boolean isHealNeeded() {
    return false;
  }

  @Override
  public void markHealNeeded() {
    // do nothing
  }
}
