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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.crypto.Hash.keccak256;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig.createStatefulConfigWithTrie;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecAdapters;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.NoOpBonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Replays a block on a Bonsai world state and checks that the state before and after each of its
 * transactions is the one block import builds.
 */
class BlockReplayTest {

  // Shanghai, where SELFDESTRUCT still deletes a contract created before the transaction
  private static final String GENESIS =
      """
      {"config": {"chainId": 42, "homesteadBlock": 0, "eip150Block": 0, "eip155Block": 0,
        "eip158Block": 0, "byzantiumBlock": 0, "constantinopleBlock": 0, "petersburgBlock": 0,
        "istanbulBlock": 0, "berlinBlock": 0, "londonBlock": 0, "terminalTotalDifficulty": 0,
        "shanghaiTime": 0}}
      """;
  private static final KeyPair KEY_PAIR =
      SignatureAlgorithmFactory.getInstance()
          .createKeyPair(
              SECPPrivateKey.create(
                  Bytes32.fromHexString(
                      "0x8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63"),
                  SignatureAlgorithm.ALGORITHM));
  private static final Address SENDER = Address.extract(KEY_PAIR.getPublicKey());
  private static final Address COINBASE = Address.fromHexString("0x3000");
  // an existing empty account, which the first transaction clears and the second funds
  private static final Address EMPTY = Address.fromHexString("0x1000");
  private static final Address FACTORY = Address.fromHexString("0x2000");
  // self-destructs to its caller
  private static final Bytes CHILD_CODE = Bytes.fromHexString("0x33ff");
  // stores 1 in slot 0 and deploys the child's code
  private static final Bytes CHILD_INIT_CODE =
      Bytes.fromHexString("0x60016000556133ff6000526002601ef3");
  // CREATE2 of the child with salt 0
  private static final Bytes FACTORY_CODE =
      Bytes.concatenate(
          Bytes.fromHexString("0x6f"),
          CHILD_INIT_CODE,
          Bytes.fromHexString("0x6000526000601060106000f55000"));
  private static final Address CHILD =
      Address.extract(
          keccak256(
              Bytes.concatenate(
                  Bytes.of(0xff), FACTORY.getBytes(), Bytes32.ZERO, keccak256(CHILD_INIT_CODE))));

  // clears the empty account, funds it, self-destructs the child and creates it again
  private static final List<Transaction> TRANSACTIONS =
      List.of(
          transaction(0, EMPTY, Wei.ZERO),
          transaction(1, EMPTY, Wei.ONE),
          transaction(2, CHILD, Wei.ZERO),
          transaction(3, FACTORY, Wei.ZERO));

  private static final BlockHeader HEADER =
      new BlockHeaderTestFixture()
          .number(1)
          .timestamp(1)
          .coinbase(COINBASE)
          .baseFeePerGas(Wei.ZERO)
          .gasLimit(30_000_000)
          .buildHeader();

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void replayBuildsTheStateBlockImportBuilds(final boolean evmV2) {
    final ProtocolSchedule protocolSchedule = protocolSchedule(evmV2);
    final List<Hash> importedRoots = importedRoots(protocolSchedule);

    for (int i = 0; i < TRANSACTIONS.size(); i++) {
      final Hash transactionHash = TRANSACTIONS.get(i).getHash();
      assertThat(
              replay(
                  protocolSchedule,
                  (replay, state) ->
                      replay.beforeTransactionInBlock(
                          state, HEADER.getHash(), transactionHash, rootAfterReplay(state))))
          .as("before transaction %s", i)
          .contains(importedRoots.get(i));
      assertThat(
              replay(
                  protocolSchedule,
                  (replay, state) ->
                      replay.afterTransactionInBlock(
                          state, HEADER.getHash(), transactionHash, rootAfterReplay(state))))
          .as("after transaction %s", i)
          .contains(importedRoots.get(i + 1));
    }
  }

  @FunctionalInterface
  private interface Replay {
    Optional<Hash> run(BlockReplay replay, Tracer.TraceableState state);
  }

  private static Optional<Hash> replay(
      final ProtocolSchedule protocolSchedule, final Replay replay) {
    final BonsaiWorldState worldState = genesisWorldState();
    final Tracer.TraceableState state = mock(Tracer.TraceableState.class);
    when(state.updater()).thenAnswer(invocation -> worldState.updater());
    when(state.rootHash()).thenAnswer(invocation -> rootHash(worldState));
    final Blockchain blockchain = mock(Blockchain.class);
    when(blockchain.getBlockHeader(HEADER.getHash())).thenReturn(Optional.of(HEADER));
    when(blockchain.getBlockBody(HEADER.getHash()))
        .thenReturn(Optional.of(new BlockBody(TRANSACTIONS, List.of())));
    return replay.run(
        new BlockReplay(protocolSchedule, mock(ProtocolContext.class), blockchain), state);
  }

  private static BlockReplay.TransactionAction<Hash> rootAfterReplay(
      final Tracer.TraceableState state) {
    return (transaction, index, block, blockchain, transactionProcessor, blobGasPrice) ->
        state.rootHash();
  }

  /** The state roots before the first transaction and after each one, as block import runs them. */
  private static List<Hash> importedRoots(final ProtocolSchedule protocolSchedule) {
    final MainnetTransactionProcessor transactionProcessor =
        protocolSchedule.getByBlockHeader(HEADER).getTransactionProcessor();
    final BonsaiWorldState worldState = genesisWorldState();
    final List<Hash> roots = new ArrayList<>();
    roots.add(worldState.rootHash());
    for (final Transaction transaction : TRANSACTIONS) {
      final WorldUpdater blockUpdater = worldState.updater();
      final WorldUpdater transactionUpdater = blockUpdater.updater();
      assertThat(
              transactionProcessor
                  .processTransaction(
                      transactionUpdater,
                      HEADER,
                      transaction,
                      COINBASE,
                      (frame, number) -> Hash.ZERO,
                      TransactionValidationParams.processingBlock(),
                      Wei.ZERO)
                  .isSuccessful())
          .isTrue();
      transactionUpdater.commit();
      blockUpdater.commit();
      blockUpdater.markTransactionBoundary();
      roots.add(rootHash(worldState));
    }
    return roots;
  }

  private static Hash rootHash(final BonsaiWorldState worldState) {
    worldState.updater().commit();
    worldState.persist(null);
    return worldState.rootHash();
  }

  private static Transaction transaction(final long nonce, final Address to, final Wei value) {
    return Transaction.builder()
        .type(TransactionType.EIP1559)
        .chainId(BigInteger.valueOf(42))
        .nonce(nonce)
        .maxPriorityFeePerGas(Wei.ZERO)
        .maxFeePerGas(Wei.ZERO)
        .gasLimit(300_000)
        .to(to)
        .value(value)
        .payload(Bytes.EMPTY)
        .signAndBuild(KEY_PAIR);
  }

  private static ProtocolSchedule protocolSchedule(final boolean evmV2) {
    return new ProtocolScheduleBuilder(
            GenesisConfig.fromConfig(GENESIS).getConfigOptions(),
            Optional.of(BigInteger.valueOf(42)),
            ProtocolSpecAdapters.create(0, Function.identity()),
            false,
            new EvmConfiguration(32_000L, EvmConfiguration.WorldUpdaterMode.STACKED, true, evmV2),
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            ImmutableBalConfiguration.builder().build(),
            new NoOpMetricsSystem())
        .createProtocolSchedule();
  }

  private static BonsaiWorldState genesisWorldState() {
    final BonsaiWorldStateKeyValueStorage storage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
    final BonsaiWorldState worldState =
        new BonsaiWorldState(
            storage,
            new NoOpBonsaiCachedMerkleTrieLoader(),
            new NoOpBonsaiWorldStateCacheManager(
                storage, EvmConfiguration.DEFAULT, new BonsaiCodeCache()),
            new NoOpTrieLogManager(),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new BonsaiCodeCache());
    final WorldUpdater genesis = worldState.updater();
    genesis.createAccount(SENDER, 0, Wei.of(10));
    genesis.createAccount(EMPTY, 0, Wei.ZERO);
    genesis.createAccount(FACTORY, 1, Wei.ZERO).setCode(FACTORY_CODE);
    final MutableAccount child = genesis.createAccount(CHILD, 1, Wei.ZERO);
    child.setCode(CHILD_CODE);
    child.setStorageValue(UInt256.ZERO, UInt256.ONE);
    genesis.commit();
    worldState.persist(null);
    return worldState;
  }
}
