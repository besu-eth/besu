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
package org.hyperledger.besu.ethereum.mainnet.blockhash;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ZeroNonceStorageAccountsPreExecutionProcessorTest {

  private static final long FORK_TIMESTAMP = 15_000L;
  private static final Address WITH_STORAGE = Address.fromHexString("0x1001");
  private static final Address ABSENT = Address.fromHexString("0x1002");
  private static final Address NONCE_ONE = Address.fromHexString("0x1003");
  private static final Address NONCE_FIVE = Address.fromHexString("0x1004");
  private static final Address UNLISTED = Address.fromHexString("0x1005");
  private static final Address SYSTEM_CONTRACT = Address.fromHexString("0x1006");
  private static final List<Address> LISTED = List.of(WITH_STORAGE, ABSENT, NONCE_ONE, NONCE_FIVE);

  private final MutableWorldState worldState =
      InMemoryKeyValueStorageProvider.createInMemoryWorldState();
  private final BlockAccessListBuilder balBuilder = BlockAccessList.builder();
  private final SystemCallLikeDelegate delegate = new SystemCallLikeDelegate();
  private final ZeroNonceStorageAccountsPreExecutionProcessor processor =
      new ZeroNonceStorageAccountsPreExecutionProcessor(delegate, FORK_TIMESTAMP, LISTED);

  static Stream<Arguments> forkBlock() {
    return Stream.of(
        Arguments.of(FORK_TIMESTAMP, Optional.of(FORK_TIMESTAMP - 7)),
        Arguments.of(FORK_TIMESTAMP + 5, Optional.of(FORK_TIMESTAMP - 7)));
  }

  @ParameterizedTest(name = "{index}: timestamp {0}, parent {1}")
  @MethodSource("forkBlock")
  void forkBlockSetsListedNoncesToOneBeforeDelegating(
      final long timestamp, final Optional<Long> parentTimestamp) {
    seedState();
    final Optional<AccessLocationTracker> tracker =
        Optional.of(BlockAccessListBuilder.createPreExecutionAccessLocationTracker());
    final BlockProcessingContext context = context(timestamp, parentTimestamp);

    processor.process(context, tracker);

    delegate.assertCalledOnceWith(context, tracker);
    assertThat(delegate.withStorageNonceSeen).containsExactly(1L);
    for (final Address address : LISTED) {
      assertThat(worldState.get(address).getNonce()).isEqualTo(1L);
    }
    final Account withStorage = worldState.get(WITH_STORAGE);
    assertThat(withStorage.getStorageValue(UInt256.ONE)).isEqualTo(UInt256.ONE);
    assertThat(worldState.get(ABSENT).getBalance().isZero()).isTrue();
    assertThat(worldState.get(ABSENT).getCode().isEmpty()).isTrue();
    assertThat(worldState.get(UNLISTED).getNonce()).isZero();

    final Map<Address, AccountChanges> changes =
        balBuilder.build().accountChanges().stream()
            .collect(Collectors.toMap(AccountChanges::address, Function.identity()));
    final List<Address> expectedAddresses = new ArrayList<>(LISTED);
    expectedAddresses.add(SYSTEM_CONTRACT);
    assertThat(changes).containsOnlyKeys(expectedAddresses);
    // The delegate's system call re-applies the shared index 0, which first clears every touched
    // account's index-0 changes, so the bump must survive that re-diff.
    assertThat(changes.get(WITH_STORAGE).nonceChanges()).containsExactly(new NonceChange(0, 1));
    assertThat(changes.get(ABSENT).nonceChanges()).containsExactly(new NonceChange(0, 1));
    assertThat(changes.get(NONCE_FIVE).nonceChanges()).containsExactly(new NonceChange(0, 1));
    assertThat(changes.get(NONCE_ONE).nonceChanges()).isEmpty();
    assertThat(changes.get(WITH_STORAGE).storageReads()).isEmpty();
  }

  @Test
  void forkBlockWithoutTrackerBumpsStateAndLeavesBalEmpty() {
    seedState();

    final BlockProcessingContext context = context(FORK_TIMESTAMP, Optional.of(FORK_TIMESTAMP - 1));

    processor.process(context, Optional.empty());

    delegate.assertCalledOnceWith(context, Optional.empty());
    for (final Address address : LISTED) {
      assertThat(worldState.get(address).getNonce()).isEqualTo(1L);
    }
    assertThat(balBuilder.build().accountChanges()).isEmpty();
  }

  static Stream<Arguments> notForkBlock() {
    return Stream.of(
        Arguments.of(FORK_TIMESTAMP + 12, Optional.of(FORK_TIMESTAMP)),
        Arguments.of(FORK_TIMESTAMP + 24, Optional.of(FORK_TIMESTAMP + 12)),
        Arguments.of(FORK_TIMESTAMP - 1, Optional.of(FORK_TIMESTAMP - 13)),
        Arguments.of(FORK_TIMESTAMP, Optional.empty()));
  }

  @ParameterizedTest(name = "{index}: timestamp {0}, parent {1}")
  @MethodSource("notForkBlock")
  void otherBlocksOnlyDelegate(final long timestamp, final Optional<Long> parentTimestamp) {
    seedState();
    final Optional<AccessLocationTracker> tracker =
        Optional.of(BlockAccessListBuilder.createPreExecutionAccessLocationTracker());
    final BlockProcessingContext context = context(timestamp, parentTimestamp);

    processor.process(context, tracker);

    delegate.assertCalledOnceWith(context, tracker);
    assertThat(worldState.get(WITH_STORAGE).getNonce()).isZero();
    assertThat(worldState.get(NONCE_FIVE).getNonce()).isEqualTo(5L);
    assertThat(worldState.get(ABSENT)).isNull();
    assertThat(balBuilder.build().accountChanges())
        .extracting(AccountChanges::address)
        .containsExactly(SYSTEM_CONTRACT);
  }

  private void seedState() {
    final WorldUpdater updater = worldState.updater();
    updater.getOrCreate(WITH_STORAGE).setStorageValue(UInt256.ONE, UInt256.ONE);
    updater.getOrCreate(NONCE_ONE).setNonce(1L);
    updater.getOrCreate(NONCE_FIVE).setNonce(5L);
    updater.getOrCreate(UNLISTED).setStorageValue(UInt256.ONE, UInt256.ONE);
    updater.commit();
    worldState.persist(null);
  }

  private BlockProcessingContext context(
      final long timestamp, final Optional<Long> parentTimestamp) {
    return new BlockProcessingContext(
        new BlockHeaderTestFixture().timestamp(timestamp).buildHeader(),
        worldState,
        null,
        null,
        OperationTracer.NO_TRACING,
        Optional.of(balBuilder),
        parentTimestamp);
  }

  /**
   * Stands in for the Prague pre-execution processor with one system call that writes only its own
   * storage. Like SystemCallProcessor, it diffs a child of a block updater and applies the shared
   * tracker to the block access list builder before committing.
   */
  private static final class SystemCallLikeDelegate implements PreExecutionProcessor {
    private final List<BlockProcessingContext> contexts = new ArrayList<>();
    private final List<Optional<AccessLocationTracker>> trackers = new ArrayList<>();
    private final List<Long> withStorageNonceSeen = new ArrayList<>();

    @Override
    public Void process(
        final BlockProcessingContext context,
        final Optional<AccessLocationTracker> accessLocationTracker) {
      contexts.add(context);
      trackers.add(accessLocationTracker);
      final Account withStorage = context.getWorldState().get(WITH_STORAGE);
      withStorageNonceSeen.add(withStorage == null ? 0L : withStorage.getNonce());

      final WorldUpdater blockUpdater = context.getWorldState().updater();
      final WorldUpdater systemCallUpdater = blockUpdater.updater();
      accessLocationTracker.ifPresent(
          tracker -> tracker.addSlotAccessForAccount(SYSTEM_CONTRACT, UInt256.ONE));
      systemCallUpdater.getOrCreate(SYSTEM_CONTRACT).setStorageValue(UInt256.ONE, UInt256.ONE);
      accessLocationTracker.ifPresent(
          tracker ->
              context
                  .getBlockAccessListBuilder()
                  .ifPresent(builder -> builder.apply(tracker, systemCallUpdater)));
      systemCallUpdater.commit();
      blockUpdater.commit();
      return null;
    }

    @Override
    public BlockHashLookup createBlockHashLookup(
        final Blockchain blockchain, final ProcessableBlockHeader blockHeader) {
      throw new UnsupportedOperationException();
    }

    private void assertCalledOnceWith(
        final BlockProcessingContext context,
        final Optional<AccessLocationTracker> accessLocationTracker) {
      assertThat(contexts).containsExactly(context);
      assertThat(trackers).containsExactly(accessLocationTracker);
    }
  }
}
