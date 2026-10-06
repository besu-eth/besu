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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.worldstate.ImmutableExtraStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class UnreadableContractCodeTest extends AbstractIsolationTests {

  // SSTORE(0, 1), deployed by INIT_CODE, so running it as empty code changes the post state
  private static final Bytes RUNTIME_CODE = Bytes.fromHexString("0x600160005500");
  private static final Bytes INIT_CODE = Bytes.fromHexString("0x656001600055006000526006601af3");
  private static final Hash CODE_HASH = Hash.hash(RUNTIME_CODE);

  private final BonsaiCodeCache emptyCodeCache = new BonsaiCodeCache();

  @Test
  void blockImportIsALocalFailureAndRecordsNoBadBlock() {
    final Address contract = deployContract();
    final Block block = forTransactions(List.of(callTransaction(contract)));
    loseCode(contract);

    final ProtocolContext context = contextWithEmptyCodeCache();
    final BlockProcessingResult result =
        protocolSchedule
            .getByBlockHeader(block.getHeader())
            .getBlockValidator()
            .validateAndProcessBlock(
                context, block, HeaderValidationMode.NONE, HeaderValidationMode.NONE);

    assertThat(result.causedBy()).containsInstanceOf(MerkleTrieException.class);
    assertThat(result.isLocalFailure()).isTrue();
    assertThat(context.getBadBlockManager().getBadBlocks()).isEmpty();
    assertThat(emptyCodeCache.getIfPresent(CODE_HASH)).isNull();
  }

  @Test
  void blockCreationDoesNotSelectTheTransaction() {
    final Address contract = deployContract();
    loseCode(contract);

    final Block block =
        TestBlockCreator.forHeader(
                contextWithEmptyCodeCache(), protocolSchedule, transactionPool, ethScheduler)
            .createBlock(
                List.of(callTransaction(contract)),
                Collections.emptyList(),
                System.currentTimeMillis(),
                blockchain.getChainHeadHeader())
            .getBlock();

    assertThat(block.getBody().getTransactions()).isEmpty();
    assertThat(emptyCodeCache.getIfPresent(CODE_HASH)).isNull();
  }

  private Address deployContract() {
    final Transaction deployment =
        new TransactionTestFixture()
            .sender(accounts.get(0).address())
            .payload(INIT_CODE)
            .gasLimit(100_000L)
            .nonce(0L)
            .createTransaction(sender1);
    assertThat(
            executeBlock(archive.getWorldState(), forTransactions(List.of(deployment)))
                .isSuccessful())
        .isTrue();
    final Address contract = Address.contractAddress(deployment.getSender(), 0L);
    assertThat(archive.getWorldState().get(contract).getCode()).isEqualTo(RUNTIME_CODE);
    return contract;
  }

  private Transaction callTransaction(final Address contract) {
    return new TransactionTestFixture()
        .sender(accounts.get(0).address())
        .to(Optional.of(contract))
        .gasLimit(100_000L)
        .nonce(1L)
        .createTransaction(sender1);
  }

  // the account still references code that storage no longer returns, whatever the code strategy
  private void loseCode(final Address contract) {
    final SegmentedKeyValueStorageTransaction transaction =
        ((BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage)
            .getComposedWorldStateStorage()
            .startTransaction();
    transaction.remove(CODE_STORAGE, CODE_HASH.getBytes().toArrayUnsafe());
    transaction.remove(CODE_STORAGE, contract.addressHash().getBytes().toArrayUnsafe());
    transaction.commit();
  }

  // the base archive's code cache holds the deployed code and would hide the unreadable storage
  private ProtocolContext contextWithEmptyCodeCache() {
    final BonsaiWorldStateProvider archiveWithEmptyCodeCache =
        new BonsaiWorldStateProvider(
            (BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage,
            blockchain,
            ImmutableExtraStorageConfiguration.builder().maxLayersToLoad(16L).build(),
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            null,
            EvmConfiguration.DEFAULT,
            emptyCodeCache);
    return new ProtocolContext.Builder()
        .withBlockchain(blockchain)
        .withWorldStateArchive(archiveWithEmptyCodeCache)
        .build();
  }
}
