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
package org.hyperledger.besu.ethereum.trie.patricia;

import static org.hyperledger.besu.ethereum.trie.CompactEncoding.bytesToPath;

import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.NullNode;
import org.hyperledger.besu.ethereum.trie.StoredNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;

/**
 * Streaming Merkle Patricia trie for strictly ascending bulk inserts.
 *
 * <p>Each insert runs {@link AscendingCollapsePutVisitor} on the root: the subtree the key finishes
 * is handed to the sink and replaced by a {@link StoredNode} stub, so live memory is the rightmost
 * path. Keys MUST arrive in strictly ascending order (typically {@code keccak256(address)} / {@code
 * keccak256(slotKey)}).
 *
 * <p>{@link #prepare} builds and hashes a complete subtree (every key under a nibble prefix) from
 * any thread; {@link #insert(Subtree)} attaches it, hashing only its top node. {@link
 * #insertSealed} attaches a subtree known only by its reference, as range proofs give them.
 */
public final class AscendingCollapsePatriciaTrie {

  private static final NodeUpdater NO_SINK = (location, hash, node) -> {};

  /**
   * Keys prepared off the inserting thread: their subtree is built and hashed below its top node.
   */
  public static final class Subtree {
    private final Bytes firstKey;
    private final Bytes firstValue;
    private final Bytes lastKey;
    private final int size;
    private final Node<Bytes> top;

    private Subtree(
        final Bytes firstKey,
        final Bytes firstValue,
        final Bytes lastKey,
        final int size,
        final Node<Bytes> top) {
      this.firstKey = firstKey;
      this.firstValue = firstValue;
      this.lastKey = lastKey;
      this.size = size;
      this.top = top;
    }

    /**
     * The top node attached {@code depth} nibbles down: built from the root, its path is cut by the
     * nibbles above it.
     */
    private Node<Bytes> at(final int depth) {
      if (depth == 0) {
        return top;
      }
      if (depth > top.getPath().size()
          || top instanceof BranchNode
          || (top instanceof LeafNode && depth == top.getPath().size())) {
        throw new IllegalArgumentException("prepared keys do not form a complete subtree");
      }
      return top.replacePath(top.getPath().slice(depth));
    }
  }

  private final DefaultNodeFactory<Bytes> nodeFactory =
      new DefaultNodeFactory<>(Function.identity());
  private final NodeUpdater sink;
  private Node<Bytes> root = NullNode.instance();
  private Bytes lastKey;
  private Bytes lastPath;
  private long insertCount;
  private boolean sealed;

  /** Creates a trie that only computes its root hash. */
  public AscendingCollapsePatriciaTrie() {
    this(NO_SINK);
  }

  /**
   * Creates a trie that hands its nodes to a sink.
   *
   * @param sink receives each hash-referenced node at its location, children before parents, then
   *     the root; it must accept calls from the threads that {@link #prepare}
   */
  public AscendingCollapsePatriciaTrie(final NodeUpdater sink) {
    this.sink = sink;
  }

  /**
   * Inserts {@code (key, value)} in strictly ascending key order.
   *
   * @throws IllegalArgumentException if order is violated or value is empty
   * @throws IllegalStateException if the trie has already been sealed
   */
  public void insert(final Bytes key, final Bytes value) {
    if (sealed) {
      throw new IllegalStateException("ascending Patricia trie already sealed");
    }
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException("Patricia trie rejects empty values");
    }
    if (lastKey != null && key.compareTo(lastKey) <= 0) {
      throw new IllegalArgumentException("keys must be inserted in strictly ascending order");
    }
    final Bytes path = bytesToPath(key);
    final Bytes finished = finished(lastPath, path);
    lastKey = key;
    lastPath = path;
    insertCount++;
    root = root.accept(new AscendingCollapsePutVisitor(nodeFactory, value, finished, sink), path);
  }

  /**
   * Builds and hashes {@code keys} (strictly ascending, with their {@code values}) as a subtree to
   * {@link #insert(Subtree)} later. Thread-safe: it reads no state of this trie. The keys must be
   * every key the trie will hold under their common nibble prefix.
   *
   * @param keys the keys, strictly ascending
   * @param values their non-empty values
   * @return the subtree to insert
   * @throws IllegalArgumentException if the keys are empty or out of order, or a value is empty
   */
  public Subtree prepare(final List<Bytes> keys, final List<Bytes> values) {
    if (keys.isEmpty() || keys.size() != values.size()) {
      throw new IllegalArgumentException("prepare needs as many values as keys, at least one");
    }
    Node<Bytes> top = NullNode.instance();
    Bytes previous = null;
    Bytes previousPath = null;
    for (int i = 0; i < keys.size(); i++) {
      final Bytes key = keys.get(i);
      final Bytes value = values.get(i);
      if (value == null || value.isEmpty()) {
        throw new IllegalArgumentException("Patricia trie rejects empty values");
      }
      if (previous != null && key.compareTo(previous) <= 0) {
        throw new IllegalArgumentException("keys must be inserted in strictly ascending order");
      }
      final Bytes path = bytesToPath(key);
      top =
          top.accept(
              new AscendingCollapsePutVisitor(
                  nodeFactory, value, finished(previousPath, path), sink),
              path);
      previous = key;
      previousPath = path;
    }
    return new Subtree(
        keys.getFirst(), values.getFirst(), previous, keys.size(), collapsed(top, Bytes.EMPTY));
  }

  /** {@code top} with the children of its branch handed to the sink and replaced by stubs. */
  private Node<Bytes> collapsed(final Node<Bytes> top, final Bytes location) {
    if (top instanceof ExtensionNode<Bytes> extension) {
      return nodeFactory.createExtension(
          extension.getPath(),
          collapsed(extension.getChild(), Bytes.concatenate(location, extension.getPath())));
    }
    if (top instanceof BranchNode<Bytes> branch) {
      final List<Node<Bytes>> children = new ArrayList<>(branch.maxChild());
      for (int i = 0; i < branch.maxChild(); i++) {
        children.add(
            AscendingCollapsePutVisitor.collapse(
                nodeFactory,
                sink,
                branch.child((byte) i),
                Bytes.concatenate(location, Bytes.of(i))));
      }
      return nodeFactory.createBranch(children, branch.getValue());
    }
    return top;
  }

  /**
   * Attaches a {@link #prepare prepared} subtree; its keys count as inserted.
   *
   * @param subtree prepared by this trie
   * @throws IllegalArgumentException if its keys are not after every key inserted so far, or do not
   *     form a complete subtree
   * @throws IllegalStateException if the trie has already been sealed
   */
  public void insert(final Subtree subtree) {
    if (sealed) {
      throw new IllegalStateException("ascending Patricia trie already sealed");
    }
    if (lastKey != null && subtree.firstKey.compareTo(lastKey) <= 0) {
      throw new IllegalArgumentException("keys must be inserted in strictly ascending order");
    }
    final Bytes path = bytesToPath(subtree.firstKey);
    final Bytes finished = finished(lastPath, path);
    root =
        root.accept(
            new AscendingCollapsePutVisitor(
                nodeFactory,
                subtree.firstValue,
                Optional.of(below -> subtree.at(path.size() - below.size())),
                finished,
                sink),
            path);
    lastKey = subtree.lastKey;
    lastPath = bytesToPath(subtree.lastKey);
    insertCount += subtree.size;
  }

  /**
   * Attaches a subtree known only by its reference, after every key inserted so far. Its keys do
   * not count as inserted.
   *
   * @param location the nibble path of the subtree root
   * @param ref the reference its parent branch holds: a hash reference or an inlined node
   * @throws IllegalArgumentException if it is not after every key inserted so far
   * @throws IllegalStateException if the trie has already been sealed
   */
  public void insertSealed(final Bytes location, final Bytes ref) {
    final Node<Bytes> node =
        ref.size() == 33 && ref.get(0) == (byte) 0xa0
            ? new StoredNode<>(nodeFactory, location, Bytes32.wrap(ref.slice(1)))
            : TrieNodeDecoder.decode(location, ref);
    insert(
        new Subtree(
            keyUnder(location, (byte) 0),
            Bytes.EMPTY,
            keyUnder(location, (byte) 0x0f),
            0,
            // as built from the root, like a prepared top: an extension the nibbles above cut
            location.isEmpty() ? node : nodeFactory.createExtension(location, node)));
  }

  /** Returns the number of keys inserted since construction. */
  public long insertCount() {
    return insertCount;
  }

  /**
   * Seals the trie and returns its root hash. Further {@link #insert} calls are rejected.
   *
   * <p>Only the rightmost path is still unhashed, so this is O(depth). The sink gets that path,
   * then the root even when it is small enough to be inlined.
   *
   * @return merkle root of the sealed trie (the empty trie hash when empty)
   */
  public Bytes32 rootHash() {
    if (!sealed && !(root instanceof NullNode || root instanceof StoredNode)) {
      AscendingCollapsePutVisitor.commit(sink, root, Bytes.EMPTY);
    }
    sealed = true;
    return root instanceof NullNode ? MerkleTrie.EMPTY_TRIE_NODE_HASH : root.getHash();
  }

  // where the subtree holding the previous key starts, one nibble below where the paths split
  private static Bytes finished(final Bytes previousPath, final Bytes path) {
    return previousPath == null
        ? null
        : previousPath.slice(0, previousPath.commonPrefixLength(path) + 1);
  }

  // the lowest or the highest key under a location
  private static Bytes keyUnder(final Bytes location, final byte fill) {
    if (location.size() > 64) {
      throw new IllegalArgumentException("a sealed subtree cannot sit below a key");
    }
    final MutableBytes path = MutableBytes.create(65);
    path.fill(fill);
    path.set(0, location);
    path.set(64, CompactEncoding.LEAF_TERMINATOR);
    return CompactEncoding.pathToBytes(path);
  }
}
