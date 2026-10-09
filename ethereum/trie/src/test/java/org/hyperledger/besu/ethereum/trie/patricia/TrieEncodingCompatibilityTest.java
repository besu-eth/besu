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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.Proof;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

/** Pins encodings, hashes and stored nodes of the trie, computed before it moved off tuweni. */
public class TrieEncodingCompatibilityTest {

  @Test
  public void storedTrieEncodingIsStable() {
    final Random random = new Random(1);
    final Map<Bytes32, Bytes> storage = new HashMap<>();
    final NodeLoader nodeLoader = (location, hash) -> Optional.ofNullable(storage.get(hash));
    final MerkleTrie<Bytes, Bytes> trie =
        new StoredMerklePatriciaTrie<>(nodeLoader, Function.identity(), Function.identity());

    final List<Bytes32> keys = new ArrayList<>();
    for (int i = 0; i < 3000; i++) {
      final Bytes32 key = randomKey(random, keys);
      keys.add(key);
      trie.put(key, randomValue(random));
    }
    for (int i = 0; i < 300; i++) {
      trie.remove(keys.get(random.nextInt(keys.size())));
    }

    final List<Bytes> committed = new ArrayList<>();
    trie.commit(
        (location, hash, value) -> {
          storage.put(hash, value);
          committed.add(location);
          committed.add(hash);
          committed.add(value);
        });
    final Bytes32 rootHash = trie.getRootHash();

    // reload the trie from storage, update it and commit again
    final MerkleTrie<Bytes, Bytes> reloaded =
        new StoredMerklePatriciaTrie<>(
            nodeLoader, rootHash, Function.identity(), Function.identity());
    final List<Bytes> values = new ArrayList<>();
    for (final Bytes32 key : keys) {
      values.add(reloaded.get(key).orElse(Bytes.EMPTY));
    }
    for (int i = 0; i < 500; i++) {
      final Bytes32 key =
          random.nextBoolean() ? keys.get(random.nextInt(keys.size())) : randomKey(random, keys);
      if (random.nextInt(4) == 0) {
        reloaded.remove(key);
      } else {
        reloaded.put(key, randomValue(random));
      }
    }
    reloaded.commit(
        (location, hash, value) -> {
          storage.put(hash, value);
          committed.add(location);
          committed.add(hash);
          committed.add(value);
        });

    final List<Bytes> proofs = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      final Proof<Bytes> proof = reloaded.getValueWithProof(keys.get(random.nextInt(keys.size())));
      proofs.addAll(proof.getProofRelatedNodes());
      proofs.add(proof.getValue().orElse(Bytes.EMPTY));
    }

    final Map<Bytes32, Bytes> entries = reloaded.entriesFrom(keys.get(7), 200);
    final List<Bytes> entriesList = new ArrayList<>();
    entries.forEach(
        (key, value) -> {
          entriesList.add(key);
          entriesList.add(value);
        });

    final List<Bytes> nodes = new ArrayList<>();
    reloaded.visitAll(
        node -> {
          nodes.add(node.getHash());
          nodes.add(node.getPath());
          nodes.add(node.getLocation().orElse(Bytes.EMPTY));
          nodes.add(node.getEncodedBytes());
          nodes.add(node.getEncodedBytesRef());
        });

    assertThat(rootHash)
        .isEqualTo(
            Bytes32.fromHexString(
                "0x33402db02b76c8161c762a82587efa0af392b5364af7492bcb8841bb13d98b71"));
    assertThat(reloaded.getRootHash())
        .isEqualTo(
            Bytes32.fromHexString(
                "0xd8e267d74a2dd689fb10fa68b2b3d82920f0f7d4af3275ea2c045d59beb48ff2"));
    assertThat(fingerprint(committed))
        .isEqualTo(
            Bytes32.fromHexString(
                "0xefba4f096fa7b025ba7fc3a6b0e1c74920bceb75eda77f047b0da2c2810d8d42"));
    assertThat(fingerprint(values))
        .isEqualTo(
            Bytes32.fromHexString(
                "0xb92ab00073c897b07a9217849631b5dae23499dd773a21eb9a5767d82a8fe0fa"));
    assertThat(fingerprint(proofs))
        .isEqualTo(
            Bytes32.fromHexString(
                "0xf7388cf94630b918eb24d06965af99b469c1124eac99db2664363316eb912fab"));
    assertThat(fingerprint(entriesList))
        .isEqualTo(
            Bytes32.fromHexString(
                "0x9538ff4fc0a530382c820c5c3aed9ede7a05ea6c58d6101e3c81d431fb8e6cef"));
    assertThat(fingerprint(nodes))
        .isEqualTo(
            Bytes32.fromHexString(
                "0xfd504b22b2decff89ffc0176a717d068c5d40fb45edf2113a7d5fddc0c393fd7"));
  }

  @Test
  public void variableLengthKeysEncodingIsStable() {
    final Random random = new Random(2);
    final MerkleTrie<Bytes, Bytes> trie = new SimpleMerklePatriciaTrie<>(Function.identity());
    final List<Bytes> roots = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      // short keys of different sizes, some of them prefixes of others, like transaction indices
      final byte[] key = new byte[1 + random.nextInt(3)];
      random.nextBytes(key);
      trie.put(Bytes.wrap(key), randomValue(random));
      if (i % 100 == 0) {
        roots.add(trie.getRootHash());
      }
    }
    roots.add(trie.getRootHash());

    final List<Bytes> proofs = new ArrayList<>();
    final List<Bytes> decodedNodes = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      final byte[] key = new byte[1 + random.nextInt(3)];
      random.nextBytes(key);
      final Proof<Bytes> proof = trie.getValueWithProof(Bytes.wrap(key));
      proofs.addAll(proof.getProofRelatedNodes());
      for (final Bytes rlp : proof.getProofRelatedNodes()) {
        final List<Node<Bytes>> nodes = TrieNodeDecoder.decodeNodes(Bytes.of(1, 2), rlp);
        for (int n = 0; n < nodes.size(); n++) {
          final Node<Bytes> node = nodes.get(n);
          decodedNodes.add(node.getHash());
          decodedNodes.add(node.getLocation().orElse(Bytes.EMPTY));
          // descendants referenced by hash are not part of the decoded rlp
          if (n == 0 || !node.isReferencedByHash()) {
            decodedNodes.add(node.getPath());
            decodedNodes.add(node.getValue().orElse(Bytes.EMPTY));
          }
        }
      }
    }

    assertThat(fingerprint(roots))
        .isEqualTo(
            Bytes32.fromHexString(
                "0xcfe654c40cd2e4a6a1772c7bce88728df4ecda0229a84f0599549af95f79a95d"));
    assertThat(fingerprint(proofs))
        .isEqualTo(
            Bytes32.fromHexString(
                "0x95a3fbfda207248f5fee1e723ac8e636268b67cd6870fa8855cbe5e815f4f9cd"));
    assertThat(fingerprint(decodedNodes))
        .isEqualTo(
            Bytes32.fromHexString(
                "0x5c2663ca9722b8d833f21cdbd053e0fb0222da14aefd88a14d068d3b57fc01ac"));
  }

  private static Bytes32 randomKey(final Random random, final List<Bytes32> keys) {
    final byte[] key = new byte[32];
    random.nextBytes(key);
    if (!keys.isEmpty() && random.nextInt(5) == 0) {
      // share a prefix with an existing key, to create extension nodes
      final Bytes32 other = keys.get(random.nextInt(keys.size()));
      System.arraycopy(other.toArrayUnsafe(), 0, key, 0, 1 + random.nextInt(30));
    }
    return Bytes32.wrap(key);
  }

  private static Bytes randomValue(final Random random) {
    final byte[] value;
    switch (random.nextInt(5)) {
      case 0 -> value = new byte[] {(byte) random.nextInt(0x80)};
      case 1 -> value = new byte[] {(byte) (0x80 + random.nextInt(0x80))};
      case 2 -> value = new byte[2 + random.nextInt(54)];
      case 3 -> value = new byte[56 + random.nextInt(300)];
      default -> value = new byte[70 + random.nextInt(10)];
    }
    if (value.length > 1) {
      random.nextBytes(value);
    }
    return Bytes.wrap(value);
  }

  private static Bytes32 fingerprint(final List<? extends Bytes> items) {
    final List<Bytes> framed = new ArrayList<>();
    for (final Bytes item : items) {
      framed.add(Bytes.ofUnsignedInt(item.size()));
      framed.add(item);
    }
    return Hash.keccak256(Bytes.concatenate(framed));
  }
}
