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

import static org.hyperledger.besu.ethereum.trie.immutabletree.TreePaths.RADIX;
import static org.hyperledger.besu.ethereum.trie.immutabletree.TreePaths.commonPrefixLength;
import static org.hyperledger.besu.ethereum.trie.immutabletree.TreePaths.isTerminator;

import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.Proof;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Operations on immutable trees. Every update returns a new root and leaves its input untouched.
 *
 * <p>The single-key operations are ports of Besu's {@code PutVisitor}, {@code RemoveVisitor},
 * {@code DeferredPutVisitor} and the {@code replaceChild} / {@code replacePath} / {@code
 * maybeFlatten} rules of its nodes, including their behaviour on non-canonical input, so that any
 * sequence of operations yields the same tree as a {@code StoredMerklePatriciaTrie}. Where Besu
 * builds a node whose content equals the one it replaces, the original is returned instead; the
 * content, and so every hash, is the same, and nothing is written for it.
 */
final class TreeOps {

  private static final TreeNode EMPTY = EmptyTreeNode.INSTANCE;

  private TreeOps() {}

  // ---------------------------------------------------------------------------------------------
  // Traversal

  static void touch(final TreeSession session, final TreeNode node) {
    if (node instanceof MaterializedTreeNode materialized) {
      materialized.touch(session.stamp());
    }
  }

  /** Loads the node if it is a placeholder. */
  static TreeNode resolve(final TreeSession session, final TreeNode node) {
    if (node instanceof StoredTreeNode stored) {
      return session.load(stored);
    }
    touch(session, node);
    return node;
  }

  /**
   * Returns a branch child, loading it if needed. With {@code writeBack} the loaded node replaces
   * the placeholder in the slot, so every tree sharing this branch finds it materialized.
   */
  static TreeNode child(
      final TreeSession session,
      final BranchTreeNode branch,
      final int index,
      final boolean writeBack) {
    final TreeNode child = branch.child(index);
    if (child instanceof StoredTreeNode stored) {
      final TreeNode loaded = session.load(stored);
      if (writeBack) {
        branch.swapChild(index, stored, loaded);
      }
      return loaded;
    }
    touch(session, child);
    return child;
  }

  /** Returns the child of an extension, loading it if needed. */
  static TreeNode child(
      final TreeSession session, final ExtensionTreeNode extension, final boolean writeBack) {
    final TreeNode child = extension.child();
    if (child instanceof StoredTreeNode stored) {
      final TreeNode loaded = session.load(stored);
      if (writeBack) {
        extension.swapChild(stored, loaded);
      }
      return loaded;
    }
    touch(session, child);
    return child;
  }

  // ---------------------------------------------------------------------------------------------
  // Get (GetVisitor)

  static Optional<Bytes> get(final TreeSession session, final TreeNode root, final Bytes path) {
    TreeNode node = resolve(session, root);
    int offset = 0;
    while (true) {
      if (node instanceof LeafTreeNode leaf) {
        return commonPrefixLength(leaf.path(), path, offset) == leaf.path().size()
            ? Optional.of(leaf.value())
            : Optional.empty();
      } else if (node instanceof ExtensionTreeNode extension) {
        final int common = commonPrefixLength(extension.path(), path, offset);
        if (common < extension.path().size()) {
          return Optional.empty();
        }
        offset += common;
        node = child(session, extension, true);
      } else if (node instanceof BranchTreeNode branch) {
        final byte nibble = path.get(offset);
        if (isTerminator(nibble)) {
          return branch.value();
        }
        offset++;
        node = child(session, branch, nibble, true);
      } else {
        return Optional.empty();
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Put (PutVisitor)

  static TreeNode put(
      final TreeSession session,
      final TreeNode node,
      final Bytes path,
      final int offset,
      final Bytes value) {
    return switch (resolve(session, node)) {
      case EmptyTreeNode ignored -> LeafTreeNode.create(session, path.slice(offset), value);
      case LeafTreeNode leaf -> putInLeaf(session, leaf, path, offset, value);
      case ExtensionTreeNode extension -> putInExtension(session, extension, path, offset, value);
      case BranchTreeNode branch -> putInBranch(session, branch, path, offset, value);
      case StoredTreeNode stored -> throw unexpected(stored);
    };
  }

  private static TreeNode putInLeaf(
      final TreeSession session,
      final LeafTreeNode leaf,
      final Bytes path,
      final int offset,
      final Bytes value) {
    final Bytes leafPath = leaf.path();
    final int common = commonPrefixLength(leafPath, path, offset);
    if (common == leafPath.size() && common == path.size() - offset) {
      return leaf.value().equals(value) ? leaf : LeafTreeNode.create(session, leafPath, value);
    }
    return splitLeaf(session, leaf, common, path, offset, value);
  }

  /** Inserts a new key into a leaf whose path diverges from it after {@code common} nibbles. */
  private static TreeNode splitLeaf(
      final TreeSession session,
      final LeafTreeNode leaf,
      final int common,
      final Bytes path,
      final int offset,
      final Bytes value) {
    final Bytes leafPath = leaf.path();
    assert common < leafPath.size() && common < path.size() - offset
        : "Should not have consumed non-matching terminator";
    final TreeNode updatedLeaf =
        LeafTreeNode.create(session, leafPath.slice(common + 1), leaf.value());
    final TreeNode newLeaf = LeafTreeNode.create(session, path.slice(offset + common + 1), value);
    final TreeNode branch =
        createBranch(
            session, leafPath.get(common), updatedLeaf, path.get(offset + common), newLeaf);
    return common > 0
        ? ExtensionTreeNode.create(session, leafPath.slice(0, common), branch)
        : branch;
  }

  private static TreeNode putInExtension(
      final TreeSession session,
      final ExtensionTreeNode extension,
      final Bytes path,
      final int offset,
      final Bytes value) {
    final Bytes extensionPath = extension.path();
    final int common = commonPrefixLength(extensionPath, path, offset);
    assert common < path.size() - offset
        : "Visiting path doesn't end with a non-matching terminator";
    if (common == extensionPath.size()) {
      final TreeNode newChild =
          put(session, child(session, extension, true), path, offset + common, value);
      return extensionReplaceChild(session, extension, newChild);
    }
    return splitExtension(session, extension, common, path, offset, value);
  }

  /** Inserts a new key into an extension whose path diverges from it after {@code common}. */
  private static TreeNode splitExtension(
      final TreeSession session,
      final ExtensionTreeNode extension,
      final int common,
      final Bytes path,
      final int offset,
      final Bytes value) {
    final Bytes extensionPath = extension.path();
    final TreeNode updatedExtension =
        extensionReplacePath(session, extension, extensionPath.slice(common + 1));
    final TreeNode leaf = LeafTreeNode.create(session, path.slice(offset + common + 1), value);
    final TreeNode branch =
        createBranch(
            session, path.get(offset + common), leaf, extensionPath.get(common), updatedExtension);
    return common > 0
        ? ExtensionTreeNode.create(session, extensionPath.slice(0, common), branch)
        : branch;
  }

  private static TreeNode putInBranch(
      final TreeSession session,
      final BranchTreeNode branch,
      final Bytes path,
      final int offset,
      final Bytes value) {
    final byte nibble = path.get(offset);
    if (isTerminator(nibble)) {
      return branchReplaceValue(session, branch, value);
    }
    final TreeNode newChild =
        put(session, child(session, branch, nibble, true), path, offset + 1, value);
    return branchReplaceChild(session, branch, nibble, newChild);
  }

  // ---------------------------------------------------------------------------------------------
  // Remove (RemoveVisitor, flattening allowed)

  static TreeNode remove(
      final TreeSession session, final TreeNode node, final Bytes path, final int offset) {
    return switch (resolve(session, node)) {
      case EmptyTreeNode empty -> empty;
      case LeafTreeNode leaf ->
          commonPrefixLength(leaf.path(), path, offset) == leaf.path().size() ? EMPTY : leaf;
      case ExtensionTreeNode extension -> {
        final int common = commonPrefixLength(extension.path(), path, offset);
        if (common == extension.path().size()) {
          final TreeNode newChild =
              remove(session, child(session, extension, true), path, offset + common);
          yield extensionReplaceChild(session, extension, newChild);
        }
        // path diverges before the end of the extension, so it cannot match
        yield extension;
      }
      case BranchTreeNode branch -> {
        final byte nibble = path.get(offset);
        if (isTerminator(nibble)) {
          yield branchRemoveValue(session, branch);
        }
        final TreeNode newChild =
            remove(session, child(session, branch, nibble, true), path, offset + 1);
        yield branchReplaceChild(session, branch, nibble, newChild);
      }
      case StoredTreeNode stored -> throw unexpected(stored);
    };
  }

  // ---------------------------------------------------------------------------------------------
  // Merge (DeferredPutVisitor): read the existing value, then update or remove it in one descent

  static TreeNode merge(
      final TreeSession session,
      final TreeNode node,
      final Bytes path,
      final int offset,
      final Function<Optional<Bytes>, Optional<Bytes>> merger) {
    return switch (resolve(session, node)) {
      case EmptyTreeNode empty -> {
        final Optional<Bytes> merged = merger.apply(Optional.empty());
        yield merged.isPresent()
            ? LeafTreeNode.create(session, path.slice(offset), merged.get())
            : empty;
      }
      case LeafTreeNode leaf -> {
        final Bytes leafPath = leaf.path();
        final int common = commonPrefixLength(leafPath, path, offset);
        if (common == leafPath.size() && common == path.size() - offset) {
          final Optional<Bytes> merged = merger.apply(Optional.of(leaf.value()));
          if (merged.isEmpty()) {
            yield EMPTY;
          }
          yield leaf.value().equals(merged.get())
              ? leaf
              : LeafTreeNode.create(session, leafPath, merged.get());
        }
        assert common < leafPath.size() && common < path.size() - offset
            : "Should not have consumed non-matching terminator";
        // Paths diverge: a new key with no prior value
        final Optional<Bytes> newValue = merger.apply(Optional.empty());
        yield newValue.isEmpty()
            ? leaf
            : splitLeaf(session, leaf, common, path, offset, newValue.get());
      }
      case ExtensionTreeNode extension -> {
        final int common = commonPrefixLength(extension.path(), path, offset);
        assert common < path.size() - offset
            : "Visiting path doesn't end with a non-matching terminator";
        if (common == extension.path().size()) {
          final TreeNode newChild =
              merge(session, child(session, extension, true), path, offset + common, merger);
          yield extensionReplaceChild(session, extension, newChild);
        }
        // Path diverges before the end of the extension: a new key with no prior value
        final Optional<Bytes> newValue = merger.apply(Optional.empty());
        yield newValue.isEmpty()
            ? extension
            : splitExtension(session, extension, common, path, offset, newValue.get());
      }
      case BranchTreeNode branch -> {
        final byte nibble = path.get(offset);
        if (isTerminator(nibble)) {
          final Optional<Bytes> merged = merger.apply(branch.value());
          yield merged.isPresent()
              ? branchReplaceValue(session, branch, merged.get())
              : branchRemoveValue(session, branch);
        }
        final TreeNode newChild =
            merge(session, child(session, branch, nibble, true), path, offset + 1, merger);
        yield branchReplaceChild(session, branch, nibble, newChild);
      }
      case StoredTreeNode stored -> throw unexpected(stored);
    };
  }

  // ---------------------------------------------------------------------------------------------
  // Node rules shared with Besu's BranchNode, ExtensionNode and node factories

  /** {@code NodeFactory.createBranch(leftIndex, left, rightIndex, right)}. */
  static TreeNode createBranch(
      final TreeSession session,
      final int leftIndex,
      final TreeNode left,
      final int rightIndex,
      final TreeNode right) {
    assert leftIndex <= RADIX && rightIndex <= RADIX && leftIndex != rightIndex;
    final BranchTreeNode.Children children = BranchTreeNode.emptyChildren();
    if (leftIndex == RADIX) {
      children.set(rightIndex, right);
      return BranchTreeNode.create(session, children, valueOf(session, left));
    } else if (rightIndex == RADIX) {
      children.set(leftIndex, left);
      return BranchTreeNode.create(session, children, valueOf(session, right));
    } else {
      children.set(leftIndex, left);
      children.set(rightIndex, right);
      return BranchTreeNode.create(session, children, null);
    }
  }

  /** {@code Node.getValue()}, or null when absent. */
  private static Bytes valueOf(final TreeSession session, final TreeNode node) {
    return switch (resolve(session, node)) {
      case LeafTreeNode leaf -> leaf.value();
      case BranchTreeNode branch -> branch.valueOrNull();
      default -> null;
    };
  }

  /**
   * {@code node.replacePath(prefix ++ node.getPath())}: re-roots a node one or more nibbles higher,
   * which is how extensions collapse and single-child branches flatten.
   */
  static TreeNode prependPath(final TreeSession session, final Bytes prefix, final TreeNode node) {
    return switch (resolve(session, node)) {
      case EmptyTreeNode empty -> empty;
      case LeafTreeNode leaf ->
          LeafTreeNode.create(session, TreePaths.concat(prefix, leaf.path()), leaf.value());
      case ExtensionTreeNode extension ->
          ExtensionTreeNode.create(
              session, TreePaths.concat(prefix, extension.path()), extension.child());
      case BranchTreeNode branch -> ExtensionTreeNode.create(session, prefix, branch);
      case StoredTreeNode stored -> throw unexpected(stored);
    };
  }

  /** {@code ExtensionNode.replaceChild(updatedChild)}: collapses into the child if needed. */
  static TreeNode extensionReplaceChild(
      final TreeSession session, final ExtensionTreeNode extension, final TreeNode updated) {
    if (updated == extension.child() && updated instanceof BranchTreeNode) {
      return extension;
    }
    return prependPath(session, extension.path(), updated);
  }

  /** {@code ExtensionNode.replacePath(path)}. */
  private static TreeNode extensionReplacePath(
      final TreeSession session, final ExtensionTreeNode extension, final Bytes path) {
    return path.isEmpty()
        ? extension.child()
        : ExtensionTreeNode.create(session, path, extension.child());
  }

  /** {@code BranchNode.replaceChild(index, updatedChild, true)}. */
  static TreeNode branchReplaceChild(
      final TreeSession session,
      final BranchTreeNode branch,
      final int index,
      final TreeNode updated) {
    if (updated != EMPTY && updated == branch.child(index)) {
      return branch;
    }
    final BranchTreeNode.Children newChildren = branch.childrenCopy();
    newChildren.set(index, updated);
    if (updated == EMPTY) {
      final Bytes value = branch.valueOrNull();
      // hasChildren() looks at this node's children before the update, as in Besu
      if (value != null && !branch.hasChildren()) {
        return LeafTreeNode.create(session, TreePaths.nibble(index), value);
      } else if (value == null) {
        final TreeNode flattened = maybeFlatten(session, branch, newChildren);
        if (flattened != null) {
          return flattened;
        }
      }
    }
    return BranchTreeNode.create(session, newChildren, branch.valueOrNull());
  }

  /** {@code BranchNode.replaceValue(value)}. */
  private static TreeNode branchReplaceValue(
      final TreeSession session, final BranchTreeNode branch, final Bytes value) {
    if (value.equals(branch.valueOrNull())) {
      return branch;
    }
    return BranchTreeNode.create(session, branch.childrenCopy(), value);
  }

  /** {@code BranchNode.removeValue()}. */
  private static TreeNode branchRemoveValue(
      final TreeSession session, final BranchTreeNode branch) {
    final BranchTreeNode.Children children = branch.childrenCopy();
    final TreeNode flattened = maybeFlatten(session, branch, children);
    if (flattened != null) {
      return flattened;
    }
    return branch.valueOrNull() == null ? branch : BranchTreeNode.create(session, children, null);
  }

  /**
   * {@code BranchNode.maybeFlatten(children)}: a branch left with a single child and no value is
   * replaced by that child, re-rooted one nibble higher. Returns null if it does not apply.
   *
   * @param original the branch whose slots the children came from, for loading into place
   */
  static TreeNode maybeFlatten(
      final TreeSession session,
      final BranchTreeNode original,
      final BranchTreeNode.Children children) {
    int onlyChild = -1;
    for (int i = 0; i < RADIX; i++) {
      if (!children.isEmpty(i)) {
        if (onlyChild >= 0) {
          return null;
        }
        onlyChild = i;
      }
    }
    if (onlyChild < 0) {
      return null;
    }
    TreeNode child = children.get(onlyChild);
    if (child instanceof StoredTreeNode stored) {
      child = session.load(stored);
      if (original != null) {
        original.swapChild(onlyChild, stored, child);
      }
    }
    return prependPath(session, TreePaths.nibble(onlyChild), child);
  }

  // ---------------------------------------------------------------------------------------------
  // Commit: write the nodes this session created, and nothing else

  /** A node write produced by a commit. */
  record NodeWrite(Bytes location, Bytes32 hash, Bytes encoded) {}

  /**
   * Writes every node the session created that is reachable from {@code root}. Children the session
   * did not create are neither visited nor loaded. Like Besu, nodes shorter than 32 bytes are
   * written only as the root, since elsewhere they are embedded in their parent.
   *
   * @return the number of nodes written
   */
  static int commit(
      final TreeSession session,
      final TreeNode root,
      final NodeUpdater updater,
      final ForkJoinPool pool) {
    if (!(root instanceof MaterializedTreeNode materialized)
        || !materialized.isCreatedBy(session.id())) {
      return 0;
    }
    final ConcurrentLinkedQueue<NodeWrite> writes = new ConcurrentLinkedQueue<>();
    if (pool == null) {
      collectWrites(session.id(), materialized, Bytes.EMPTY, true, writes, 0);
    } else {
      pool.invoke(
          ForkJoinTask.adapt(
              () -> collectWrites(session.id(), materialized, Bytes.EMPTY, true, writes, 2)));
    }
    for (final NodeWrite write : writes) {
      updater.store(write.location(), write.hash(), write.encoded());
    }
    return writes.size();
  }

  private static void collectWrites(
      final int sessionId,
      final MaterializedTreeNode node,
      final Bytes location,
      final boolean isRoot,
      final ConcurrentLinkedQueue<NodeWrite> writes,
      final int forkLevels) {
    if (node instanceof BranchTreeNode branch) {
      final List<ForkJoinTask<?>> forks = forkLevels > 0 ? new ArrayList<>() : null;
      for (int i = 0; i < RADIX; i++) {
        final MaterializedTreeNode child = branch.loadedChild(i);
        if (child != null && child.isCreatedBy(sessionId)) {
          final Bytes childLocation = TreePaths.concat(location, TreePaths.nibble(i));
          if (forks != null && child instanceof BranchTreeNode) {
            forks.add(
                ForkJoinTask.adapt(
                        () ->
                            collectWrites(
                                sessionId, child, childLocation, false, writes, forkLevels - 1))
                    .fork());
          } else {
            collectWrites(sessionId, child, childLocation, false, writes, forkLevels - 1);
          }
        }
      }
      if (forks != null) {
        forks.forEach(ForkJoinTask::join);
      }
    } else if (node instanceof ExtensionTreeNode extension
        && extension.child() instanceof MaterializedTreeNode child
        && child.isCreatedBy(sessionId)) {
      collectWrites(
          sessionId,
          child,
          TreePaths.concat(location, extension.path()),
          false,
          writes,
          forkLevels);
    }
    final Bytes encoded = node.encoded();
    if (isRoot || encoded.size() >= 32) {
      writes.add(new NodeWrite(location, node.hash(), encoded));
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Reads over ranges and proofs

  /**
   * Returns up to {@code limit} leaves whose key is at least {@code startKeyHash}, in key order,
   * like Besu's {@code StorageEntriesCollector}. Subtrees entirely before the start key are skipped
   * without being loaded.
   */
  static Map<Bytes32, Bytes> entriesFrom(
      final TreeSession session,
      final TreeNode root,
      final Bytes32 startKeyHash,
      final int limit,
      final boolean writeBack) {
    final Map<Bytes32, Bytes> entries = new TreeMap<>();
    if (limit <= 0) {
      return entries;
    }
    final EntryCollector collector =
        new EntryCollector(
            session, CompactEncoding.bytesToPath(startKeyHash), startKeyHash, limit, writeBack);
    collector.visit(resolve(session, root), 0, true, entries);
    return entries;
  }

  private static final class EntryCollector {
    private final TreeSession session;
    private final Bytes startPath;
    private final Bytes32 startKeyHash;
    private final int limit;
    private final boolean writeBack;
    private final byte[] path = new byte[256];

    EntryCollector(
        final TreeSession session,
        final Bytes startPath,
        final Bytes32 startKeyHash,
        final int limit,
        final boolean writeBack) {
      this.session = session;
      this.startPath = startPath;
      this.startKeyHash = startKeyHash;
      this.limit = limit;
      this.writeBack = writeBack;
    }

    /**
     * Returns false once the limit is reached. {@code bounded}: the path so far equals the start
     * key's.
     */
    boolean visit(
        final TreeNode node,
        final int depth,
        final boolean bounded,
        final Map<Bytes32, Bytes> entries) {
      if (node instanceof LeafTreeNode leaf) {
        final Bytes leafPath = leaf.path();
        leafPath.copyTo(org.apache.tuweni.bytes.MutableBytes.wrap(path), depth);
        final Bytes fullPath = Bytes.wrap(path, 0, depth + leafPath.size());
        final Bytes32 keyHash = Bytes32.wrap(CompactEncoding.pathToBytes(fullPath), 0);
        if (keyHash.compareTo(startKeyHash) >= 0) {
          entries.put(keyHash, leaf.value());
        }
        return entries.size() < limit;
      } else if (node instanceof ExtensionTreeNode extension) {
        final Bytes extensionPath = extension.path();
        boolean childBounded = false;
        if (bounded) {
          final int compared = compareWithStart(extensionPath, depth);
          if (compared < 0) {
            return true; // every key below is smaller than the start key
          }
          childBounded = compared == 0;
        }
        extensionPath.copyTo(org.apache.tuweni.bytes.MutableBytes.wrap(path), depth);
        return visit(
            child(session, extension, writeBack),
            depth + extensionPath.size(),
            childBounded,
            entries);
      } else if (node instanceof BranchTreeNode branch) {
        int from = 0;
        if (bounded) {
          final byte startNibble = startPath.get(depth);
          if (isTerminator(startNibble)) {
            return true;
          }
          from = startNibble;
        }
        for (int i = from; i < RADIX; i++) {
          if (branch.isEmptyChild(i)) {
            continue;
          }
          path[depth] = (byte) i;
          if (!visit(
              child(session, branch, i, writeBack), depth + 1, bounded && i == from, entries)) {
            return false;
          }
        }
        return true;
      }
      return true;
    }

    private int compareWithStart(final Bytes segment, final int depth) {
      for (int i = 0; i < segment.size(); i++) {
        final int startIndex = depth + i;
        if (startIndex >= startPath.size()) {
          return 1;
        }
        final int diff = Integer.compare(segment.get(i), startPath.get(startIndex));
        if (diff != 0) {
          return diff;
        }
      }
      return 0;
    }
  }

  /**
   * Returns the value at {@code path} with the encodings of the nodes on the way, like Besu's
   * {@code ProofVisitor}: the root and every node referenced by hash.
   */
  static Proof<Bytes> proof(final TreeSession session, final TreeNode root, final Bytes path) {
    final List<Bytes> proof = new ArrayList<>();
    final TreeNode resolvedRoot = resolve(session, root);
    TreeNode node = resolvedRoot;
    int offset = 0;
    Optional<Bytes> value = Optional.empty();
    while (node instanceof MaterializedTreeNode materialized) {
      if (node == resolvedRoot || materialized.isReferencedByHash()) {
        proof.add(materialized.encoded());
      }
      if (node instanceof LeafTreeNode leaf) {
        if (commonPrefixLength(leaf.path(), path, offset) == leaf.path().size()) {
          value = Optional.of(leaf.value());
        }
        break;
      } else if (node instanceof ExtensionTreeNode extension) {
        final int common = commonPrefixLength(extension.path(), path, offset);
        if (common < extension.path().size()) {
          break;
        }
        offset += common;
        node = child(session, extension, true);
      } else {
        final BranchTreeNode branch = (BranchTreeNode) node;
        final byte nibble = path.get(offset);
        if (isTerminator(nibble)) {
          value = branch.value();
          break;
        }
        offset++;
        node = child(session, branch, nibble, true);
      }
    }
    return new Proof<>(value, proof);
  }

  private static IllegalStateException unexpected(final TreeNode node) {
    return new IllegalStateException("Unexpected unresolved node " + node);
  }
}
