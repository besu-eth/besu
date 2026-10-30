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
package org.hyperledger.besu.ethereum.eth.sync.snap2.trie;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.patricia.AscendingCollapsePatriciaTrie;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Verifies a snap range response: the leaves must be exactly those of the trie from the origin to
 * the last returned key, which holds when the root recomputed from them and the proof matches.
 */
public final class RangeProof {

  private RangeProof() {}

  /**
   * Outcome of a verification.
   *
   * @param valid whether the response is a correct slice of the trie
   * @param hasMore whether the trie holds keys after the last returned key
   */
  public record Result(boolean valid, boolean hasMore) {
    static final Result INVALID = new Result(false, false);
  }

  /**
   * Verifies a range response.
   *
   * @param rootHash the trie root
   * @param origin the requested starting key
   * @param keys the returned keys, strictly increasing and not below origin
   * @param values the returned leaf values, RLP encoded as stored in the trie
   * @param proof the boundary proof nodes, empty when the response claims the whole trie
   * @return the verification result
   */
  public static Result verify(
      final Bytes32 rootHash,
      final Bytes32 origin,
      final List<Bytes32> keys,
      final List<Bytes> values,
      final List<Bytes> proof) {
    return check(rootHash, origin, keys, values, proof, null);
  }

  /**
   * Verifies a range response and, once the root matches, hands the nodes it rebuilds to the sink:
   * those of the trie with a key in the range, then the root. The ranges of one root together give
   * every node of its trie.
   */
  public static Result verify(
      final Bytes32 rootHash,
      final Bytes32 origin,
      final List<Bytes32> keys,
      final List<Bytes> values,
      final List<Bytes> proof,
      final NodeUpdater sink) {
    final List<Handed> handed = new ArrayList<>();
    final Result result =
        check(
            rootHash,
            origin,
            keys,
            values,
            proof,
            (location, hash, node) -> handed.add(new Handed(location, hash, node)));
    if (result.valid()) {
      handed.forEach(node -> sink.store(node.location(), node.hash(), node.node()));
    }
    return result;
  }

  private static Result check(
      final Bytes32 rootHash,
      final Bytes32 origin,
      final List<Bytes32> keys,
      final List<Bytes> values,
      final List<Bytes> proof,
      final NodeUpdater sink) {
    if (keys.size() != values.size() || !isSortedFrom(origin, keys)) {
      return Result.INVALID;
    }
    try {
      if (proof.isEmpty()) {
        if (!origin.isZero()) {
          return Result.INVALID;
        }
        return computeRoot(keys, values, sink).equals(rootHash)
            ? new Result(true, false)
            : Result.INVALID;
      }
      final Map<Bytes32, Bytes> nodes = new HashMap<>(proof.size() * 2);
      for (final Bytes node : proof) {
        nodes.put(keccak(node), node);
      }
      final Walker walker =
          new Walker(
              nodes,
              origin.toArrayUnsafe(),
              (keys.isEmpty() ? RangeManager.MAX_RANGE : keys.getLast()).toArrayUnsafe());
      walker.visit(hashRef(rootHash), Bytes.EMPTY);

      final AscendingCollapsePatriciaTrie trie = trie(sink);
      for (final Sealed sealed : walker.left) {
        trie.insertSealed(sealed.location(), sealed.ref());
      }
      for (int i = 0; i < keys.size(); i++) {
        trie.insert(keys.get(i), values.get(i));
      }
      for (final Sealed sealed : walker.right) {
        trie.insertSealed(sealed.location(), sealed.ref());
      }
      return trie.rootHash().equals(rootHash)
          ? new Result(true, !walker.right.isEmpty())
          : Result.INVALID;
    } catch (final RLPException
        | MerkleTrieException
        | IllegalArgumentException
        | IllegalStateException e) {
      return Result.INVALID;
    }
  }

  // the root of a trie holding exactly the given sorted leaves
  private static Bytes32 computeRoot(
      final List<Bytes32> keys, final List<Bytes> values, final NodeUpdater sink) {
    final AscendingCollapsePatriciaTrie trie = trie(sink);
    for (int i = 0; i < keys.size(); i++) {
      trie.insert(keys.get(i), values.get(i));
    }
    return trie.rootHash();
  }

  private static AscendingCollapsePatriciaTrie trie(final NodeUpdater sink) {
    return sink == null
        ? new AscendingCollapsePatriciaTrie()
        : new AscendingCollapsePatriciaTrie(sink);
  }

  private static Bytes32 keccak(final Bytes node) {
    return Bytes32.wrap(Hash.hash(node).getBytes());
  }

  private static Bytes hashRef(final Bytes32 hash) {
    return RLP.encodeOne(hash);
  }

  private static boolean isHashRef(final Bytes ref) {
    return ref.size() == 33 && ref.get(0) == (byte) 0xa0;
  }

  private static int nibble(final byte[] key, final int index) {
    return (key[index >>> 1] >>> ((index & 1) == 0 ? 4 : 0)) & 0x0f;
  }

  private static boolean isSortedFrom(final Bytes32 origin, final List<Bytes32> keys) {
    Bytes32 previous = null;
    for (final Bytes32 key : keys) {
      if (previous == null ? key.compareTo(origin) < 0 : key.compareTo(previous) <= 0) {
        return false;
      }
      previous = key;
    }
    return true;
  }

  private record Sealed(Bytes location, Bytes ref) {}

  private record Handed(Bytes location, Bytes32 hash, Bytes node) {}

  private enum Relation {
    LEFT,
    RIGHT,
    INSIDE,
    OVERLAP
  }

  private static final class Walker {
    private final Map<Bytes32, Bytes> nodes;
    private final byte[] origin;
    private final byte[] rightBound;
    private final List<Sealed> left = new ArrayList<>();
    private final List<Sealed> right = new ArrayList<>();

    private Walker(final Map<Bytes32, Bytes> nodes, final byte[] origin, final byte[] rightBound) {
      this.nodes = nodes;
      this.origin = origin;
      this.rightBound = rightBound;
    }

    // visits a node that overlaps a boundary of the range
    private void visit(final Bytes ref, final Bytes location) {
      final Bytes rlp = resolve(ref);
      final RLPInput in = RLP.input(rlp);
      final int items = in.enterList();
      if (items == 17) {
        if (location.size() >= 64) {
          throw new IllegalArgumentException("branch at the wrong depth");
        }
        for (int i = 0; i < 16; i++) {
          final Bytes child = readRef(in);
          if (child != null) {
            final Bytes childLocation = Bytes.concatenate(location, Bytes.of(i));
            dispatch(child, childLocation, relation(childLocation), true);
          }
        }
        if (!in.readBytes().isEmpty()) {
          throw new IllegalArgumentException("branch value in a 32-byte key trie");
        }
      } else if (items == 2) {
        final Bytes path = CompactEncoding.decode(in.readBytes());
        final boolean leaf =
            path.size() > 0 && path.get(path.size() - 1) == CompactEncoding.LEAF_TERMINATOR;
        final Bytes full =
            Bytes.concatenate(location, leaf ? path.slice(0, path.size() - 1) : path);
        if (leaf) {
          if (full.size() != 64) {
            throw new IllegalArgumentException("leaf at the wrong depth");
          }
          in.readBytes();
          final Relation relation = relation(full);
          dispatch(ref, location, relation == Relation.OVERLAP ? Relation.INSIDE : relation, false);
        } else {
          if (path.isEmpty() || full.size() >= 64) {
            throw new IllegalArgumentException("invalid extension");
          }
          final Relation relation = relation(full);
          if (relation == Relation.OVERLAP) {
            final Bytes child = readRef(in);
            if (child == null) {
              throw new IllegalArgumentException("extension without child");
            }
            visit(child, full);
          } else {
            dispatch(ref, location, relation, false);
          }
        }
      } else {
        throw new IllegalArgumentException("invalid node");
      }
    }

    private void dispatch(
        final Bytes ref, final Bytes location, final Relation relation, final boolean descend) {
      switch (relation) {
        case LEFT -> left.add(new Sealed(location, ref));
        case RIGHT -> right.add(new Sealed(location, ref));
        case INSIDE -> {
          // rebuilt from the returned leaves
        }
        case OVERLAP -> {
          if (!descend) {
            throw new IllegalStateException("unexpected overlap");
          }
          visit(ref, location);
        }
      }
    }

    private Bytes resolve(final Bytes ref) {
      if (!isHashRef(ref)) {
        return ref;
      }
      final Bytes node = nodes.get(Bytes32.wrap(ref.slice(1)));
      if (node == null) {
        throw new IllegalArgumentException("missing proof node");
      }
      return node;
    }

    private Relation relation(final Bytes location) {
      final int toOrigin = compare(location, origin);
      if (toOrigin < 0) {
        return Relation.LEFT;
      }
      final int toRight = compare(location, rightBound);
      if (toRight > 0) {
        return Relation.RIGHT;
      }
      return toOrigin > 0 && toRight < 0 ? Relation.INSIDE : Relation.OVERLAP;
    }

    private static int compare(final Bytes location, final byte[] key) {
      for (int i = 0; i < location.size(); i++) {
        final int diff = (location.get(i) & 0x0f) - nibble(key, i);
        if (diff != 0) {
          return diff;
        }
      }
      return 0;
    }

    private static Bytes readRef(final RLPInput in) {
      if (in.nextIsList()) {
        return in.readAsRlp().raw();
      }
      final Bytes value = in.readBytes();
      if (value.isEmpty()) {
        return null;
      }
      if (value.size() != 32) {
        throw new IllegalArgumentException("invalid child reference");
      }
      return hashRef(Bytes32.wrap(value));
    }
  }
}
