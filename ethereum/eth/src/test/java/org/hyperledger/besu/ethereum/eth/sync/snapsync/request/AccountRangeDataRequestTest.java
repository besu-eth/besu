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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.request;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;
import java.util.TreeMap;
import java.util.function.Function;

import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AccountRangeDataRequestTest {

  private static final Hash FIRST_ACCOUNT = Hash.wrap(Bytes32.leftPad(Bytes.of(1)));
  private static final Hash SECOND_ACCOUNT = Hash.wrap(Bytes32.leftPad(Bytes.of(2)));
  private static final Hash THIRD_ACCOUNT = Hash.wrap(Bytes32.leftPad(Bytes.of(3)));
  private static final Hash FOURTH_ACCOUNT = Hash.wrap(Bytes32.leftPad(Bytes.of(4)));

  private final SnapRequestContext downloadState = mock(SnapRequestContext.class);

  @BeforeEach
  public void setUp() {
    when(downloadState.getMetricsManager()).thenReturn(mock(SnapSyncMetricsManager.class));
  }

  @Test
  public void shouldNotRequestTheStorageOfAnAccountPastTheEndOfTheRange() {
    final WorldStateStorageCoordinator coordinator =
        new WorldStateStorageCoordinator(
            new BonsaiWorldStateKeyValueStorage(
                new InMemoryKeyValueStorageProvider(),
                new NoOpMetricsSystem(),
                DataStorageConfiguration.DEFAULT_BONSAI_CONFIG));
    final MerkleTrie<Bytes, Bytes> accountTrie =
        new SimpleMerklePatriciaTrie<>(Function.identity());
    for (final Hash account :
        List.of(FIRST_ACCOUNT, SECOND_ACCOUNT, THIRD_ACCOUNT, FOURTH_ACCOUNT)) {
      accountTrie.put(account.getBytes(), accountWithStorage());
    }
    // the range ends at the second account, the response goes on to the first one after it
    final TreeMap<Bytes32, Bytes> accounts = new TreeMap<>();
    for (final Hash account : List.of(FIRST_ACCOUNT, SECOND_ACCOUNT, THIRD_ACCOUNT)) {
      accounts.put(Bytes32.wrap(account.getBytes()), accountWithStorage());
    }
    final ArrayDeque<Bytes> proofs = new ArrayDeque<>();
    proofs.addAll(accountTrie.getValueWithProof(RangeManager.MIN_RANGE).getProofRelatedNodes());
    proofs.addAll(accountTrie.getValueWithProof(THIRD_ACCOUNT.getBytes()).getProofRelatedNodes());

    final AccountRangeDataRequest request =
        SnapDataRequest.createAccountRangeDataRequest(
            Hash.wrap(accountTrie.getRootHash()),
            RangeManager.MIN_RANGE,
            Bytes32.wrap(SECOND_ACCOUNT.getBytes()));
    request.addResponse(new WorldStateProofProvider(coordinator), accounts, proofs);
    assertThat(request.isResponseReceived()).isTrue();

    assertThat(request.getChildRequests(downloadState, coordinator, null))
        .filteredOn(StorageRangeDataRequest.class::isInstance)
        .extracting(child -> ((StorageRangeDataRequest) child).getAccountHash())
        .containsExactly(FIRST_ACCOUNT, SECOND_ACCOUNT);
  }

  private static Bytes accountWithStorage() {
    final PmtStateTrieAccountValue account =
        new PmtStateTrieAccountValue(1L, Wei.ONE, Hash.hash(Bytes.of(7)), Hash.EMPTY);
    return RLP.encode(account::writeTo);
  }
}
