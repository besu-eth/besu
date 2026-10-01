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
package org.hyperledger.besu.ethereum.trie.immutabletree;

import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;

import org.apache.tuweni.bytes.Bytes;

/** A leaf: the rest of the key path, ending with the leaf terminator, and the value. */
public final class LeafTreeNode extends MaterializedTreeNode {

  private final Bytes path;
  private final Bytes value;

  LeafTreeNode(final int createdBy, final int stamp, final Bytes path, final Bytes value) {
    super(createdBy, stamp);
    this.path = path;
    this.value = value;
  }

  static LeafTreeNode create(final TreeSession session, final Bytes path, final Bytes value) {
    return new LeafTreeNode(session.id(), session.stamp(), path, value);
  }

  /**
   * Returns the remaining key path of this leaf.
   *
   * @return the nibble path, ending with the leaf terminator
   */
  public Bytes path() {
    return path;
  }

  /**
   * Returns the value stored in this leaf.
   *
   * @return the value
   */
  public Bytes value() {
    return value;
  }

  @Override
  Bytes computeEncoding() {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(CompactEncoding.encode(path));
    out.writeBytes(value);
    out.endList();
    return out.encoded();
  }

  @Override
  public String toString() {
    return "Leaf[" + path + " => " + value + "]";
  }
}
