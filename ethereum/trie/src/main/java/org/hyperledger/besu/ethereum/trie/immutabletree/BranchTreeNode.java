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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A branch: sixteen child slots and an optional value, which is the 17th item of its encoding and
 * stands for a key ending at this node.
 *
 * <p>Unloaded children usually make up most of a cached tree, so a branch can hold them compactly:
 * a {@code null} slot is a stored child whose hash is in a shared array, and the {@link
 * StoredTreeNode} for it is only created when asked for. Branches decoded from storage start that
 * way, copies share the array, and pruning unloads a child back into it when the hash matches.
 */
public final class BranchTreeNode extends MaterializedTreeNode {

  private static final VarHandle SLOT = MethodHandles.arrayElementVarHandle(TreeNode[].class);
  private static final int HASH_SIZE = Bytes32.SIZE;

  // Owned by this node; slots only ever swap between representations of the same child
  private final TreeNode[] slots;
  // Hashes of the children stored compactly (null slots), 32 bytes per slot; shared by copies
  private final byte[] storedHashes;
  // This branch's location, the prefix of its compact children's locations
  private final Bytes location;
  private final Bytes value;

  BranchTreeNode(final int createdBy, final int stamp, final Children children, final Bytes value) {
    super(createdBy, stamp);
    this.slots = children.slots;
    this.storedHashes = children.storedHashes;
    this.location = children.location;
    this.value = value;
  }

  /**
   * Creates a branch owned by the session. The children are taken over, not copied.
   *
   * @param session the creating session
   * @param children the child slots
   * @param value the branch value, or null
   * @return the new branch
   */
  static BranchTreeNode create(
      final TreeSession session, final Children children, final Bytes value) {
    return new BranchTreeNode(session.id(), session.stamp(), children, value);
  }

  /** Sixteen empty slots. */
  static Children emptyChildren() {
    final TreeNode[] slots = new TreeNode[TreePaths.RADIX];
    Arrays.fill(slots, EmptyTreeNode.INSTANCE);
    return new Children(slots, null, null);
  }

  /**
   * Returns a child slot: the child, a {@link StoredTreeNode} for it, or the empty node.
   *
   * @param index the nibble
   * @return the child
   */
  public TreeNode child(final int index) {
    final TreeNode slot = (TreeNode) SLOT.getAcquire(slots, index);
    return slot != null ? slot : stored(storedHashes, location, index);
  }

  /** Returns the child if it is loaded, null otherwise. Allocates nothing. */
  MaterializedTreeNode loadedChild(final int index) {
    return SLOT.getAcquire(slots, index) instanceof MaterializedTreeNode loaded ? loaded : null;
  }

  /** Whether this branch holds a shared array of stored children's hashes. */
  boolean hasStoredHashes() {
    return storedHashes != null;
  }

  /** Whether the slot is empty. Allocates nothing. */
  boolean isEmptyChild(final int index) {
    return SLOT.getAcquire(slots, index) == EmptyTreeNode.INSTANCE;
  }

  /** Whether the child is kept compactly, as a hash in the shared array. */
  boolean isCompactChild(final int index) {
    return SLOT.getAcquire(slots, index) == null;
  }

  /** Whether the child is stored and not loaded. Allocates nothing. */
  boolean isStoredChild(final int index) {
    final TreeNode slot = (TreeNode) SLOT.getAcquire(slots, index);
    return slot == null || slot instanceof StoredTreeNode;
  }

  /** Returns a copy of the child slots, for building a modified branch at the same location. */
  Children childrenCopy() {
    final TreeNode[] copy = new TreeNode[TreePaths.RADIX];
    for (int i = 0; i < TreePaths.RADIX; i++) {
      copy[i] = (TreeNode) SLOT.getAcquire(slots, i);
    }
    return new Children(copy, storedHashes, location);
  }

  /**
   * Swaps a child slot between two representations of the same node: a placeholder for its loaded
   * node on load, and back on prune. A placeholder matching the slot's stored hash is kept as a
   * compact (null) slot.
   */
  boolean swapChild(final int index, final TreeNode expected, final TreeNode replacement) {
    final TreeNode witness = isCompact(expected, index) ? null : expected;
    final TreeNode stored = isCompact(replacement, index) ? null : replacement;
    return SLOT.compareAndSet(slots, index, witness, stored);
  }

  private boolean isCompact(final TreeNode node, final int index) {
    return node instanceof StoredTreeNode stored
        && storedHashes != null
        && Bytes.wrap(storedHashes, index * HASH_SIZE, HASH_SIZE).equals(stored.hash());
  }

  private static StoredTreeNode stored(
      final byte[] storedHashes, final Bytes location, final int index) {
    return new StoredTreeNode(
        location, TreePaths.nibble(index), Bytes32.wrap(storedHashes, index * HASH_SIZE));
  }

  /**
   * Returns the value of this branch, if a key ends here.
   *
   * @return the value
   */
  public Optional<Bytes> value() {
    return Optional.ofNullable(value);
  }

  Bytes valueOrNull() {
    return value;
  }

  boolean hasChildren() {
    for (int i = 0; i < TreePaths.RADIX; i++) {
      if (SLOT.getAcquire(slots, i) != EmptyTreeNode.INSTANCE) {
        return true;
      }
    }
    return false;
  }

  @Override
  Bytes computeEncoding() {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    for (int i = 0; i < TreePaths.RADIX; i++) {
      final TreeNode slot = (TreeNode) SLOT.getAcquire(slots, i);
      if (slot != null) {
        slot.writeRef(out);
      } else {
        out.writeBytes(Bytes.wrap(storedHashes, i * HASH_SIZE, HASH_SIZE));
      }
    }
    if (value != null) {
      out.writeBytes(value);
    } else {
      out.writeNull();
    }
    out.endList();
    return out.encoded();
  }

  @Override
  public String toString() {
    return "Branch" + (value != null ? "[value]" : "");
  }

  /**
   * Child slots of a branch under construction. A null slot is a stored child whose hash is in
   * {@code storedHashes}; {@link #get(int)} turns it into a {@link StoredTreeNode}.
   */
  static final class Children {
    private final TreeNode[] slots;
    private final byte[] storedHashes;
    private final Bytes location;

    Children(final TreeNode[] slots, final byte[] storedHashes, final Bytes location) {
      this.slots = slots;
      this.storedHashes = storedHashes;
      this.location = location;
    }

    TreeNode get(final int index) {
      final TreeNode slot = slots[index];
      return slot != null ? slot : stored(storedHashes, location, index);
    }

    void set(final int index, final TreeNode child) {
      slots[index] = child;
    }

    boolean isEmpty(final int index) {
      return slots[index] == EmptyTreeNode.INSTANCE;
    }
  }
}
