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
package org.hyperledger.besu.ethereum.worldstate;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletionException;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;
import org.junit.jupiter.api.Test;

class WorldStateRepairRequestTest {
  private static final Hash BLOCK_HASH = Hash.fromHexString("0x" + "01".repeat(32));
  private static final Hash STATE_ROOT = Hash.fromHexString("0x" + "02".repeat(32));
  private static final Hash NODE_HASH = Hash.fromHexString("0x" + "03".repeat(32));

  @Test
  void extractsCompleteMissingNodeThroughNestedCompletionExceptions() {
    final Address address = Address.fromHexString("0x0000000000000000000000000000000000000001");
    final Bytes location = Bytes.fromHexString("0x1234");
    final MerkleTrieException missing =
        new MerkleTrieException(
            "missing", Optional.of(address), Bytes32.wrap(NODE_HASH.getBytes()), location);

    final WorldStateRepairRequest request =
        WorldStateRepairRequest.fromFailure(
            BLOCK_HASH, STATE_ROOT, new CompletionException(new CompletionException(missing)));

    assertThat(request.blockHash()).isEqualTo(BLOCK_HASH);
    assertThat(request.stateRoot()).isEqualTo(STATE_ROOT);
    assertThat(request.missingNode())
        .contains(
            new WorldStateRepairRequest.MissingTrieNode(NODE_HASH, location, Optional.of(address)));
  }

  @Test
  void storageAndIoFailuresSuppressMissingNodeHint() {
    final MerkleTrieException missing =
        new MerkleTrieException("missing", Bytes32.wrap(NODE_HASH.getBytes()), Bytes.EMPTY);
    final Throwable[] failures = {
      new CompletionException(new StorageException("database failure", missing)),
      new CompletionException(new IOException("disk failure", missing))
    };

    for (final Throwable failure : failures) {
      assertThat(WorldStateRepairRequest.fromFailure(BLOCK_HASH, STATE_ROOT, failure).missingNode())
          .isEmpty();
    }
  }

  @Test
  void incompleteMissingNodeDoesNotProduceHint() {
    final WorldStateRepairRequest request =
        WorldStateRepairRequest.fromFailure(
            BLOCK_HASH, STATE_ROOT, new CompletionException(new MerkleTrieException("missing")));

    assertThat(request.missingNode()).isEmpty();
  }

  @Test
  void missingNodeCopiesMutableLocationOnConstructionAndAccess() {
    final MutableBytes source = MutableBytes.create(2);
    source.set(0, (byte) 0x12);
    source.set(1, (byte) 0x34);
    final WorldStateRepairRequest.MissingTrieNode node =
        new WorldStateRepairRequest.MissingTrieNode(NODE_HASH, source, Optional.empty());
    source.set(0, (byte) 0x56);

    assertThat(node.location()).isEqualTo(Bytes.fromHexString("0x1234"));
    final MutableBytes callerCopy = node.location().mutableCopy();
    callerCopy.set(0, (byte) 0x78);
    assertThat(node.location()).isEqualTo(Bytes.fromHexString("0x1234"));
    assertThat(node)
        .isEqualTo(
            new WorldStateRepairRequest.MissingTrieNode(
                NODE_HASH, Bytes.fromHexString("0x1234"), Optional.empty()));
  }
}
