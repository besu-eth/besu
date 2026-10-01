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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

import org.apache.tuweni.bytes.Bytes;

/**
 * Applies a set of updates to a tree in one pass, working on disjoint subtrees in parallel.
 *
 * <p>Updates are sorted by path, so the updates below any node are a contiguous range. Each node is
 * visited once for all of them: a branch hands each child the range under it, and a leaf or an
 * extension in the way is first spread over a branch at the depth where the paths diverge. The
 * result is then normalised the way Besu's nodes normalise themselves (empty branches vanish,
 * single-child branches flatten, extensions merge into their child).
 *
 * <p>A Merkle Patricia trie is canonical: its shape, and so its root, depends only on the keys and
 * values it holds, not on the order of the updates that built it. Besu's single-key operations keep
 * it canonical as long as all keys have the same length and end with the leaf terminator, which
 * holds for account and storage tries. The batch path requires exactly that and falls back to
 * applying the updates one by one otherwise, including below a branch that carries a value.
 */
final class BatchUpdater {

  /** A group of at least this many updates goes to its own task when a branch has several. */
  private static final int FORK_THRESHOLD = 4;

  private static final TreeNode EMPTY = EmptyTreeNode.INSTANCE;

  private final TreeSession session;
  private final TreeUpdate[] updates;
  private final ForkJoinPool pool;
  private final int keyLength;

  private BatchUpdater(
      final TreeSession session,
      final TreeUpdate[] updates,
      final ForkJoinPool pool,
      final int keyLength) {
    this.session = session;
    this.updates = updates;
    this.pool = pool;
    this.keyLength = keyLength;
  }

  /**
   * Applies updates to a tree.
   *
   * @param session the session creating the new nodes
   * @param root the root to update
   * @param updates the updates, sorted by path, one per key
   * @param pool the pool to work in, or null to apply them in the calling thread
   * @return the new root
   */
  static TreeNode apply(
      final TreeSession session,
      final TreeNode root,
      final TreeUpdate[] updates,
      final ForkJoinPool pool) {
    if (updates.length == 0) {
      return root;
    }
    final int keyLength = updates[0].path().size();
    for (final TreeUpdate update : updates) {
      final Bytes path = update.path();
      if (path.size() != keyLength || !TreePaths.isTerminator(path.get(keyLength - 1))) {
        return applySequentially(session, root, updates, 0, updates.length, 0);
      }
    }
    final BatchUpdater updater = new BatchUpdater(session, updates, pool, keyLength);
    if (pool == null || updates.length < FORK_THRESHOLD) {
      return updater.apply(root, 0, updates.length, 0);
    }
    return pool.invoke(ForkJoinTask.adapt(() -> updater.apply(root, 0, updates.length, 0)));
  }

  private static TreeNode applySequentially(
      final TreeSession session,
      final TreeNode node,
      final TreeUpdate[] updates,
      final int from,
      final int to,
      final int depth) {
    TreeNode result = node;
    for (int i = from; i < to; i++) {
      result = updates[i].applyTo(session, result, depth);
    }
    return result;
  }

  /**
   * Applies {@code updates[from, to)} to the subtree at {@code node}, which sits at {@code depth}.
   */
  private TreeNode apply(final TreeNode node, final int from, final int to, final int depth) {
    if (to - from == 1) {
      return updates[from].applyTo(session, node, depth);
    }
    return switch (TreeOps.resolve(session, node)) {
      case EmptyTreeNode ignored -> applyToEmpty(from, to, depth);
      case LeafTreeNode leaf -> applyToLeaf(leaf, from, to, depth);
      case ExtensionTreeNode extension -> applyToExtension(extension, from, to, depth);
      case BranchTreeNode branch ->
          branch.valueOrNull() != null
              // a value means keys of another length: keep the exact single-key behaviour
              ? applySequentially(session, branch, updates, from, to, depth)
              : applyToBranch(branch, branch.childrenCopy(), from, to, depth);
      case StoredTreeNode stored ->
          throw new IllegalStateException("Unexpected unresolved node " + stored);
    };
  }

  private TreeNode applyToEmpty(final int from, final int to, final int depth) {
    final int shared = sharedPrefix(from, to, depth);
    final TreeNode sub =
        applyToBranch(null, BranchTreeNode.emptyChildren(), from, to, depth + shared);
    return withPrefix(updates[from].path().slice(depth, shared), sub);
  }

  private TreeNode applyToLeaf(
      final LeafTreeNode leaf, final int from, final int to, final int depth) {
    final Bytes leafPath = leaf.path();
    if (leafPath.size() != keyLength - depth) {
      return applySequentially(session, leaf, updates, from, to, depth);
    }
    // Distinct keys of equal length differ before the terminator, so leafPath[shared] is a nibble
    final int shared =
        Math.min(
            sharedPrefix(from, to, depth),
            TreePaths.commonPrefixLength(leafPath, updates[from].path(), depth));
    final BranchTreeNode.Children children = BranchTreeNode.emptyChildren();
    children.set(
        leafPath.get(shared),
        LeafTreeNode.create(session, leafPath.slice(shared + 1), leaf.value()));
    final TreeNode sub = applyToBranch(null, children, from, to, depth + shared);
    return withPrefix(leafPath.slice(0, shared), sub);
  }

  private TreeNode applyToExtension(
      final ExtensionTreeNode extension, final int from, final int to, final int depth) {
    final Bytes extensionPath = extension.path();
    final int shared =
        Math.min(
            sharedPrefix(from, to, depth),
            TreePaths.commonPrefixLength(extensionPath, updates[from].path(), depth));
    if (shared == extensionPath.size()) {
      final TreeNode child = TreeOps.child(session, extension, true);
      final TreeNode newChild = apply(child, from, to, depth + shared);
      return TreeOps.extensionReplaceChild(session, extension, newChild);
    }
    // The updates leave the extension at `shared`: split it with a branch there
    final BranchTreeNode.Children children = BranchTreeNode.emptyChildren();
    final int nibble = extensionPath.get(shared);
    final Bytes rest = extensionPath.slice(shared + 1);
    if (!rest.isEmpty()) {
      children.set(nibble, ExtensionTreeNode.create(session, rest, extension.child()));
    } else if (hasGroup(from, to, depth + shared, nibble)) {
      children.set(nibble, TreeOps.child(session, extension, true));
    } else {
      children.set(nibble, extension.child());
    }
    final TreeNode sub = applyToBranch(null, children, from, to, depth + shared);
    return withPrefix(extensionPath.slice(0, shared), sub);
  }

  /**
   * Applies the updates to the children of a branch and normalises the result.
   *
   * @param original the branch the children come from, or null for a branch built by this update
   * @param children the child slots to update, owned by the result
   */
  private TreeNode applyToBranch(
      final BranchTreeNode original,
      final BranchTreeNode.Children children,
      final int from,
      final int to,
      final int depth) {
    final int[] groupNibble = new int[RADIX];
    final int[] groupEnd = new int[RADIX + 1];
    int groups = 0;
    groupEnd[0] = from;
    for (int i = from; i < to; ) {
      final byte nibble = updates[i].path().get(depth);
      assert !TreePaths.isTerminator(nibble) : "Distinct keys of equal length share a terminator";
      int j = i + 1;
      while (j < to && updates[j].path().get(depth) == nibble) {
        j++;
      }
      groupNibble[groups] = nibble;
      groupEnd[++groups] = j;
      i = j;
    }

    boolean changed = original == null;
    final boolean fork = pool != null && groups > 1;
    ForkJoinTask<?>[] tasks = null;
    for (int g = 0; g < groups; g++) {
      final int nibble = groupNibble[g];
      final int start = groupEnd[g];
      final int end = groupEnd[g + 1];
      final TreeNode child = resolveChild(original, children, nibble);
      children.set(nibble, child);
      if (fork && end - start >= FORK_THRESHOLD) {
        if (tasks == null) {
          tasks = new ForkJoinTask<?>[RADIX];
        }
        tasks[g] = ForkJoinTask.adapt(() -> hashed(apply(child, start, end, depth + 1))).fork();
      } else {
        final TreeNode updated = apply(child, start, end, depth + 1);
        changed |= updated != child;
        children.set(nibble, updated);
      }
    }
    if (tasks != null) {
      for (int g = 0; g < groups; g++) {
        if (tasks[g] != null) {
          final int nibble = groupNibble[g];
          final TreeNode updated = (TreeNode) tasks[g].join();
          changed |= updated != children.get(nibble);
          children.set(nibble, updated);
        }
      }
    }
    if (!changed) {
      return original;
    }
    return normalize(original, children);
  }

  /** A branch without value and with the given children, in canonical form. */
  private TreeNode normalize(
      final BranchTreeNode original, final BranchTreeNode.Children children) {
    int count = 0;
    int last = -1;
    for (int i = 0; i < RADIX; i++) {
      if (!children.isEmpty(i)) {
        count++;
        last = i;
      }
    }
    if (count == 0) {
      return EMPTY;
    }
    if (count == 1) {
      return TreeOps.prependPath(
          session, TreePaths.nibble(last), resolveChild(original, children, last));
    }
    return BranchTreeNode.create(session, children, null);
  }

  private TreeNode resolveChild(
      final BranchTreeNode original, final BranchTreeNode.Children children, final int nibble) {
    final TreeNode child = children.get(nibble);
    if (child instanceof StoredTreeNode stored) {
      final TreeNode loaded = session.load(stored);
      if (original != null) {
        original.swapChild(nibble, stored, loaded);
      }
      return loaded;
    }
    TreeOps.touch(session, child);
    return child;
  }

  private TreeNode withPrefix(final Bytes prefix, final TreeNode node) {
    return prefix.isEmpty() ? node : TreeOps.prependPath(session, prefix, node);
  }

  private int sharedPrefix(final int from, final int to, final int depth) {
    // Sorted paths: the first and last share exactly the prefix common to all
    return TreePaths.commonPrefixLength(
        updates[from].path(), updates[to - 1].path(), depth, Integer.MAX_VALUE);
  }

  private boolean hasGroup(final int from, final int to, final int depth, final int nibble) {
    for (int i = from; i < to; i++) {
      if (updates[i].path().get(depth) == nibble) {
        return true;
      }
    }
    return false;
  }

  private static TreeNode hashed(final TreeNode node) {
    if (node instanceof MaterializedTreeNode materialized) {
      materialized.ensureRef();
    }
    return node;
  }
}
