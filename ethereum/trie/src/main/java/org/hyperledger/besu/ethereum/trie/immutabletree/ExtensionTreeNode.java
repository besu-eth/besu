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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import org.apache.tuweni.bytes.Bytes;

/** An extension: a shared path segment (without terminator) leading to a single child. */
public final class ExtensionTreeNode extends MaterializedTreeNode {

  private static final VarHandle CHILD;

  static {
    try {
      CHILD =
          MethodHandles.lookup().findVarHandle(ExtensionTreeNode.class, "child", TreeNode.class);
    } catch (final ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final Bytes path;

  @SuppressWarnings("UnusedVariable") // accessed through CHILD
  private volatile TreeNode child;

  ExtensionTreeNode(final int createdBy, final int stamp, final Bytes path, final TreeNode child) {
    super(createdBy, stamp);
    assert !path.isEmpty() : "Extension path is empty";
    assert !TreePaths.isTerminator(path.get(path.size() - 1))
        : "Extension path ends in a leaf terminator";
    this.path = path;
    this.child = child;
  }

  static ExtensionTreeNode create(
      final TreeSession session, final Bytes path, final TreeNode child) {
    return new ExtensionTreeNode(session.id(), session.stamp(), path, child);
  }

  /**
   * Returns the path segment of this extension.
   *
   * @return the nibble path
   */
  public Bytes path() {
    return path;
  }

  /**
   * Returns the child slot, which holds either the child or a {@link StoredTreeNode} for it.
   *
   * @return the child
   */
  public TreeNode child() {
    return child;
  }

  /** Swaps the child slot between two representations of the same node. */
  boolean swapChild(final TreeNode expected, final TreeNode replacement) {
    return CHILD.compareAndSet(this, expected, replacement);
  }

  @Override
  Bytes computeEncoding() {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(CompactEncoding.encode(path));
    child.writeRef(out);
    out.endList();
    return out.encoded();
  }

  @Override
  public String toString() {
    return "Extension[" + path + "]";
  }
}
