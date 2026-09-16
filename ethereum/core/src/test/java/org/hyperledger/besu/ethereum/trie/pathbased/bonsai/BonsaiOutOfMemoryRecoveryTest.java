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
import static org.assertj.core.api.Assertions.fail;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import org.hyperledger.besu.config.GenesisAccount;
import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BlockchainStorage;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCache;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Replays a chain into a node that hits an {@link OutOfMemoryError} while importing one block and
 * keeps running, then checks the node and a restart of it against the chain it should have.
 */
public class BonsaiOutOfMemoryRecoveryTest extends AbstractIsolationTests {

  private static final int REPLAYED_BLOCKS = 6;
  private static final long FAULTY_BLOCK = 3;
  private static final int MAX_ATTEMPTS = 3;
  private static final Address RECIPIENT = Address.fromHexString("0xdeadbeef");
  private static final Wei ONE_ETHER = Wei.of(1_000_000_000_000_000_000L);

  enum Phase {
    IMPORT,
    NEW_PAYLOAD,
    FORKCHOICE_UPDATED
  }

  enum Location {
    ACCOUNT_READ,
    STATE_UPDATER,
    TRIE_LOG_SAVE,
    WORLD_STATE_COMMIT,
    BLOCKCHAIN_COMMIT
  }

  /** Faults while a block is executed on the head world state and appended to the chain. */
  enum ImportFault {
    /** Mid-block, after the accumulator already holds changes of the block. */
    TRANSACTION_EXECUTION(Location.ACCOUNT_READ, 1),
    /** All changes of the block are in the accumulator, nothing is written yet. */
    BEFORE_PERSIST(Location.STATE_UPDATER, 0),
    /** While the trie log is being written, before it is committed. */
    TRIE_LOG_SAVE(Location.TRIE_LOG_SAVE, 0),
    /** Trie log committed, world state not yet committed. */
    WORLD_STATE_COMMIT(Location.WORLD_STATE_COMMIT, 0),
    /** World state committed, block and chain head not yet committed. */
    CHAIN_APPEND(Location.BLOCKCHAIN_COMMIT, 0);

    private final Location location;
    private final int skippedCalls;

    ImportFault(final Location location, final int skippedCalls) {
      this.location = location;
      this.skippedCalls = skippedCalls;
    }
  }

  /** Faults while a block arrives through engine_newPayload and engine_forkchoiceUpdated. */
  enum EngineFault {
    /** Trie log of the executed block is being written, before it is committed. */
    NEW_PAYLOAD_TRIE_LOG_SAVE(Phase.NEW_PAYLOAD, Location.TRIE_LOG_SAVE, 0),
    /** Trie log committed, block not yet stored. */
    NEW_PAYLOAD_BLOCK_COMMIT(Phase.NEW_PAYLOAD, Location.BLOCKCHAIN_COMMIT, 0),
    /** Head world state partially rolled forward to the new head. */
    FORKCHOICE_ROLL_FORWARD(Phase.FORKCHOICE_UPDATED, Location.ACCOUNT_READ, 1),
    /** Head world state rolled forward but not yet committed. */
    FORKCHOICE_WORLD_STATE_COMMIT(Phase.FORKCHOICE_UPDATED, Location.WORLD_STATE_COMMIT, 0),
    /** Head world state committed, chain head not yet committed. */
    FORKCHOICE_HEAD_COMMIT(Phase.FORKCHOICE_UPDATED, Location.BLOCKCHAIN_COMMIT, 0);

    private final Phase phase;
    private final Location location;
    private final int skippedCalls;

    EngineFault(final Phase phase, final Location location, final int skippedCalls) {
      this.phase = phase;
      this.location = location;
      this.skippedCalls = skippedCalls;
    }
  }

  @TempDir private Path victimData;

  private final FaultInjector faults = new FaultInjector();
  private final List<Block> chain = new ArrayList<>();
  private String lastFailure = "none";

  @Test
  public void replayWithoutFaultMatchesChain() {
    assertNodeSurvivesAndRestarts(this::importBlock);
  }

  @Test
  public void engineReplayWithoutFaultMatchesChain() {
    assertNodeSurvivesAndRestarts(this::engineImport);
  }

  @ParameterizedTest
  @EnumSource(ImportFault.class)
  public void nodeSurvivesOutOfMemoryDuringBlockImport(final ImportFault fault) {
    faults.arm(Phase.IMPORT, fault.location, FAULTY_BLOCK, fault.skippedCalls);
    assertNodeSurvivesAndRestarts(this::importBlock);
  }

  @ParameterizedTest
  @EnumSource(ImportFault.class)
  public void restartAfterOutOfMemoryDuringBlockImportRecoversNode(final ImportFault fault) {
    faults.arm(Phase.IMPORT, fault.location, FAULTY_BLOCK, fault.skippedCalls);
    assertRestartRecoversNode(this::importBlock, this::tryImport);
  }

  @ParameterizedTest
  @EnumSource(EngineFault.class)
  public void nodeSurvivesOutOfMemoryDuringEngineImport(final EngineFault fault) {
    faults.arm(fault.phase, fault.location, FAULTY_BLOCK, fault.skippedCalls);
    assertNodeSurvivesAndRestarts(this::engineImport);
  }

  @ParameterizedTest
  @EnumSource(EngineFault.class)
  public void restartAfterOutOfMemoryDuringEngineImportRecoversNode(final EngineFault fault) {
    faults.arm(fault.phase, fault.location, FAULTY_BLOCK, fault.skippedCalls);
    assertRestartRecoversNode(
        this::engineImport,
        (node, block) ->
            tryNewPayload(node, block) && tryForkchoiceUpdated(node, block.getHeader()));
  }

  private void assertNodeSurvivesAndRestarts(final BiConsumer<Node, Block> importer) {
    generateChain();
    final StorageProvider victimStorage = createKeyValueStorageProvider(victimData);

    final Node victim = startNode(victimStorage, true);
    chain.subList(0, REPLAYED_BLOCKS).forEach(block -> importer.accept(victim, block));
    faults.assertFiredIfArmed();
    assertNodeMatchesChain(victim);

    final Node restarted = startNode(victimStorage, false);
    assertNodeMatchesChain(restarted);
    importer.accept(restarted, chain.getLast());
    assertHeadIs(restarted, chain.getLast());
  }

  private void assertRestartRecoversNode(
      final BiConsumer<Node, Block> importer, final BiPredicate<Node, Block> singleAttempt) {
    generateChain();
    final StorageProvider victimStorage = createKeyValueStorageProvider(victimData);

    final Node victim = startNode(victimStorage, true);
    chain.subList(0, (int) FAULTY_BLOCK - 1).forEach(block -> importer.accept(victim, block));
    assertThat(singleAttempt.test(victim, chain.get((int) FAULTY_BLOCK - 1))).isFalse();
    faults.assertFiredIfArmed();

    final Node restarted = startNode(victimStorage, false);
    chain
        .subList((int) FAULTY_BLOCK - 1, REPLAYED_BLOCKS)
        .forEach(block -> importer.accept(restarted, block));
    assertNodeMatchesChain(restarted);
    importer.accept(restarted, chain.getLast());
    assertHeadIs(restarted, chain.getLast());
  }

  /** Builds REPLAYED_BLOCKS + 1 blocks on the fault-free node of the base class. */
  private void generateChain() {
    for (long nonce = 0; nonce <= REPLAYED_BLOCKS; nonce++) {
      final Block block = forTransactions(List.of(burnTransaction(sender1, nonce, RECIPIENT)));
      final BlockProcessingResult result = executeBlock(archive.getWorldState(), block);
      assertThat(result.isSuccessful()).isTrue();
      chain.add(block);
    }
  }

  private void importBlock(final Node node, final Block block) {
    retry(block, "import", () -> tryImport(node, block));
  }

  private void engineImport(final Node node, final Block block) {
    retry(block, "newPayload", () -> tryNewPayload(node, block));
    retry(block, "forkchoiceUpdated", () -> tryForkchoiceUpdated(node, block.getHeader()));
  }

  /** A consensus client sends a request again while it fails with an error. */
  private void retry(final Block block, final String request, final BooleanSupplier attempt) {
    for (int i = 0; i < MAX_ATTEMPTS; i++) {
      if (attempt.getAsBoolean()) {
        return;
      }
    }
    fail(
        "%s of block %d failed %d times, last failure: %s",
        request, block.getHeader().getNumber(), MAX_ATTEMPTS, lastFailure);
  }

  private boolean tryImport(final Node node, final Block block) {
    faults.enter(Phase.IMPORT, block.getHeader().getNumber());
    try {
      final BlockProcessingResult result = validateAndProcess(node, block, true);
      if (result.isSuccessful()) {
        node.blockchain.appendBlock(block, result.getReceipts());
        return true;
      }
      lastFailure = result.toString();
    } catch (final RuntimeException | OutOfMemoryError e) {
      // the JVM keeps running after an OutOfMemoryError, so does the node
      lastFailure = e.toString();
    }
    return false;
  }

  /** Mirrors MergeCoordinator.rememberBlock. */
  private boolean tryNewPayload(final Node node, final Block block) {
    faults.enter(Phase.NEW_PAYLOAD, block.getHeader().getNumber());
    try {
      final BlockProcessingResult result = validateAndProcess(node, block, false);
      if (result.isSuccessful()) {
        node.blockchain.storeBlock(block, result.getReceipts(), Optional.empty());
        return true;
      }
      lastFailure = result.toString();
    } catch (final RuntimeException | OutOfMemoryError e) {
      lastFailure = e.toString();
    }
    return false;
  }

  /** Mirrors MergeCoordinator.setNewHead. */
  private boolean tryForkchoiceUpdated(final Node node, final BlockHeader newHead) {
    faults.enter(Phase.FORKCHOICE_UPDATED, newHead.getNumber());
    try {
      final MutableBlockchain blockchain = node.blockchain;
      if (newHead.getHash().equals(blockchain.getChainHeadHash())) {
        return true;
      }
      if (node.archive.getWorldState(withBlockHeaderAndUpdateNodeHead(newHead)).isEmpty()) {
        lastFailure = "world state of the new head is not available";
        return false;
      }
      if (newHead.getParentHash().equals(blockchain.getChainHeadHash())) {
        return blockchain.forwardToBlock(newHead);
      }
      return blockchain.rewindToBlock(newHead.getHash());
    } catch (final RuntimeException | OutOfMemoryError e) {
      lastFailure = e.toString();
    }
    return false;
  }

  private BlockProcessingResult validateAndProcess(
      final Node node, final Block block, final boolean shouldUpdateHead) {
    return protocolSchedule
        .getByBlockHeader(block.getHeader())
        .getBlockValidator()
        .validateAndProcessBlock(
            node.context,
            block,
            HeaderValidationMode.NONE,
            HeaderValidationMode.NONE,
            Optional.empty(),
            shouldUpdateHead);
  }

  private void assertHeadIs(final Node node, final Block head) {
    assertThat(node.blockchain.getChainHeadHash()).isEqualTo(head.getHash());
    assertThat(node.archive.getWorldState().rootHash()).isEqualTo(head.getHeader().getStateRoot());
  }

  private void assertNodeMatchesChain(final Node node) {
    final Block head = chain.get(REPLAYED_BLOCKS - 1);
    assertThat(node.context.getBadBlockManager().getBadBlocks()).isEmpty();
    assertHeadIs(node, head);
    for (final Block block : chain.subList(0, REPLAYED_BLOCKS)) {
      assertThat(node.blockchain.getBlockHashByNumber(block.getHeader().getNumber()))
          .as("canonical hash of block %d", block.getHeader().getNumber())
          .contains(block.getHash());
    }

    final MutableWorldState headState =
        node.archive
            .getWorldState(withBlockHeaderAndUpdateNodeHead(head.getHeader()))
            .orElseThrow();
    final MutableWorldState expectedHeadState =
        archive.getWorldState(withBlockHeaderAndNoUpdateNodeHead(head.getHeader())).orElseThrow();
    accountsOfInterest()
        .forEach(
            address ->
                assertAccountEquals(address, headState, expectedHeadState, head.getHeader()));

    // rolling back through every trie log must reproduce each historical state
    for (final Block block : chain.subList(0, REPLAYED_BLOCKS - 1)) {
      final BlockHeader header = block.getHeader();
      final MutableWorldState historical =
          node.archive.getWorldState(withBlockHeaderAndNoUpdateNodeHead(header)).orElseThrow();
      assertThat(historical.rootHash())
          .as("root of block %d", header.getNumber())
          .isEqualTo(header.getStateRoot());
      assertThat(historical.get(RECIPIENT).getBalance())
          .as("recipient balance at block %d", header.getNumber())
          .isEqualTo(ONE_ETHER.multiply(header.getNumber()));
    }
  }

  private static void assertAccountEquals(
      final Address address,
      final MutableWorldState actual,
      final MutableWorldState expected,
      final BlockHeader header) {
    final Account actualAccount = actual.get(address);
    final Account expectedAccount = expected.get(address);
    if (expectedAccount == null) {
      assertThat(actualAccount).as("account %s at block %d", address, header.getNumber()).isNull();
      return;
    }
    assertThat(actualAccount).as("account %s at block %d", address, header.getNumber()).isNotNull();
    assertThat(actualAccount.getNonce())
        .as("nonce of %s", address)
        .isEqualTo(expectedAccount.getNonce());
    assertThat(actualAccount.getBalance())
        .as("balance of %s", address)
        .isEqualTo(expectedAccount.getBalance());
    assertThat(actualAccount.getCodeHash())
        .as("code hash of %s", address)
        .isEqualTo(expectedAccount.getCodeHash());
  }

  private static Stream<Address> accountsOfInterest() {
    return Stream.concat(
        GenesisConfig.fromResource("/dev.json").streamAllocations().map(GenesisAccount::address),
        Stream.of(RECIPIENT, Address.ZERO));
  }

  /** Opens a node on the given database, as a fresh process would. */
  private Node startNode(final StorageProvider storageProvider, final boolean firstStart) {
    final BlockchainStorage blockchainStorage =
        spy(
            storageProvider.createBlockchainStorage(
                protocolSchedule,
                storageProvider.createVariablesStorage(),
                DataStorageConfiguration.DEFAULT_BONSAI_CONFIG));
    doAnswer(
            invocation -> {
              final BlockchainStorage.Updater updater =
                  spy((BlockchainStorage.Updater) invocation.callRealMethod());
              doAnswer(
                      commit -> {
                        faults.maybeThrow(Location.BLOCKCHAIN_COMMIT);
                        return commit.callRealMethod();
                      })
                  .when(updater)
                  .commit();
              return updater;
            })
        .when(blockchainStorage)
        .updater();
    final MutableBlockchain nodeChain =
        DefaultBlockchain.createMutable(
            genesisState.getBlock(), blockchainStorage, new NoOpMetricsSystem(), 0);

    final BonsaiWorldStateKeyValueStorage worldStateStorage =
        spy(
            new BonsaiWorldStateKeyValueStorage(
                storageProvider,
                new NoOpMetricsSystem(),
                DataStorageConfiguration.DEFAULT_BONSAI_CONFIG));
    final Thread importThread = Thread.currentThread();
    doAnswer(
            invocation -> {
              if (Thread.currentThread() == importThread) {
                faults.maybeThrow(Location.ACCOUNT_READ);
              }
              return invocation.callRealMethod();
            })
        .when(worldStateStorage)
        .getAccount(any());
    doAnswer(
            invocation -> {
              faults.maybeThrow(Location.STATE_UPDATER);
              final BonsaiWorldStateKeyValueStorage.Updater updater =
                  spy((BonsaiWorldStateKeyValueStorage.Updater) invocation.callRealMethod());
              doAnswer(
                      commit -> {
                        faults.maybeThrow(Location.WORLD_STATE_COMMIT);
                        return commit.callRealMethod();
                      })
                  .when(updater)
                  .commitComposedOnly();
              return updater;
            })
        .when(worldStateStorage)
        .updater();

    final BonsaiWorldStateProvider nodeArchive =
        new BonsaiWorldStateProvider(
            worldStateStorage,
            nodeChain,
            ImmutablePathBasedExtraStorageConfiguration.builder().maxLayersToLoad(16L).build(),
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            null,
            EvmConfiguration.DEFAULT,
            new PathBasedCodeCache());
    nodeArchive.getTrieLogManager().subscribe(__ -> faults.maybeThrow(Location.TRIE_LOG_SAVE));
    if (firstStart) {
      genesisState.writeStateTo(nodeArchive.getWorldState());
    }
    final ProtocolContext context =
        new ProtocolContext.Builder()
            .withBlockchain(nodeChain)
            .withWorldStateArchive(nodeArchive)
            .build();
    return new Node(nodeChain, nodeArchive, context);
  }

  private record Node(
      MutableBlockchain blockchain, BonsaiWorldStateProvider archive, ProtocolContext context) {}

  private static final class FaultInjector {
    private Phase phase = Phase.IMPORT;
    private long blockNumber;
    private Phase armedPhase;
    private Location armedLocation;
    private long armedBlockNumber;
    private int remainingSkips;
    private boolean fired;

    void arm(
        final Phase phase,
        final Location location,
        final long blockNumber,
        final int skippedCalls) {
      armedPhase = phase;
      armedLocation = location;
      armedBlockNumber = blockNumber;
      remainingSkips = skippedCalls;
    }

    void enter(final Phase phase, final long blockNumber) {
      this.phase = phase;
      this.blockNumber = blockNumber;
    }

    void maybeThrow(final Location location) {
      if (fired
          || location != armedLocation
          || phase != armedPhase
          || blockNumber != armedBlockNumber) {
        return;
      }
      if (remainingSkips-- > 0) {
        return;
      }
      fired = true;
      throw new OutOfMemoryError(
          "injected at " + location + " during " + phase + " of block " + blockNumber);
    }

    void assertFiredIfArmed() {
      if (armedLocation != null) {
        assertThat(fired)
            .as("fault at %s during %s was injected", armedLocation, armedPhase)
            .isTrue();
      }
    }
  }
}
