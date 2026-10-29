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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.v2.worldstate.JournaledWorldUpdaterV2;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.evm.worldstate.WorldView;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.math.BigInteger;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A tracer can reach the updater of a frame other than the running one, which the journal EVM v2
 * shares between the call frames of a transaction cannot keep apart, so v2 uses it only when the
 * tracer does not access the world state.
 */
class EvmV2TracerWorldStateTest {

  private static final String GENESIS =
      """
      {"config": {"chainId": 42, "homesteadBlock": 0, "eip150Block": 0, "eip155Block": 0,
        "eip158Block": 0, "byzantiumBlock": 0, "constantinopleBlock": 0, "petersburgBlock": 0,
        "istanbulBlock": 0, "berlinBlock": 0, "londonBlock": 0, "terminalTotalDifficulty": 0,
        "shanghaiTime": 0, "cancunTime": 0}}
      """;
  private static final Address CALLER = Address.fromHexString("0x1000");
  private static final Address CALLEE = Address.fromHexString("0x2000");
  private static final Address COINBASE = Address.fromHexString("0x3000");
  // CALL the callee and store its result in slot 0
  private static final Bytes CALLER_CODE =
      Bytes.fromHexString(
          "0x60006000600060006000"
              + "73"
              + CALLEE.getBytes().toUnprefixedHexString()
              + "5af160005500");
  // store 1 in slot 0, then revert
  private static final Bytes CALLEE_CODE = Bytes.fromHexString("0x600160005560006000fd");
  private static final KeyPair KEY_PAIR = SignatureAlgorithmFactory.getInstance().generateKeyPair();

  /** Reads the account of the frame after each operation, as the debug_traceBlock streamer does. */
  private static final class FrameReadingTracer implements OperationTracer {
    private final boolean accessesWorldState;
    private WorldView transactionWorldView;

    FrameReadingTracer(final boolean accessesWorldState) {
      this.accessesWorldState = accessesWorldState;
    }

    @Override
    public boolean accessesWorldState() {
      return accessesWorldState;
    }

    @Override
    public void traceStartTransaction(
        final WorldView worldView, final org.hyperledger.besu.datatypes.Transaction transaction) {
      transactionWorldView = worldView;
    }

    @Override
    public void tracePostExecution(
        final MessageFrame frame, final OperationResult operationResult) {
      if (accessesWorldState) {
        // after CALL this is the calling frame, whose updater the called one is open on
        frame.getWorldUpdater().getAccount(frame.getRecipientAddress());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void aTracerReadingTheCallingFrameLeavesTheCalledFramesRevertAlone(final boolean evmV2) {
    final FrameReadingTracer tracer = new FrameReadingTracer(true);
    final WorldUpdater transactionUpdater = processCall(evmV2, tracer);

    assertThat(transactionUpdater.get(CALLEE).getStorageValue(UInt256.ZERO))
        .isEqualTo(UInt256.ZERO);
    assertThat(transactionUpdater.get(CALLER).getStorageValue(UInt256.ZERO))
        .isEqualTo(UInt256.ZERO);
    assertThat(tracer.transactionWorldView).isNotInstanceOf(JournaledWorldUpdaterV2.class);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void theJournalRunsOnlyForTracersThatDoNotAccessTheWorldState(final boolean evmV2) {
    final FrameReadingTracer tracer = new FrameReadingTracer(false);
    final WorldUpdater transactionUpdater = processCall(evmV2, tracer);

    assertThat(transactionUpdater.get(CALLEE).getStorageValue(UInt256.ZERO))
        .isEqualTo(UInt256.ZERO);
    assertThat(tracer.transactionWorldView instanceof JournaledWorldUpdaterV2).isEqualTo(evmV2);
  }

  private static WorldUpdater processCall(final boolean evmV2, final OperationTracer tracer) {
    final ProtocolSchedule protocolSchedule =
        new ProtocolScheduleBuilder(
                GenesisConfig.fromConfig(GENESIS).getConfigOptions(),
                Optional.of(BigInteger.valueOf(42)),
                ProtocolSpecAdapters.create(0, Function.identity()),
                false,
                new EvmConfiguration(
                    32_000L, EvmConfiguration.WorldUpdaterMode.STACKED, true, evmV2),
                MiningConfiguration.MINING_DISABLED,
                new BadBlockManager(),
                false,
                ImmutableBalConfiguration.builder().build(),
                new NoOpMetricsSystem())
            .createProtocolSchedule();
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .number(1)
            .timestamp(1)
            .baseFeePerGas(Wei.ZERO)
            .gasLimit(30_000_000)
            .buildHeader();
    final MainnetTransactionProcessor transactionProcessor =
        protocolSchedule.getByBlockHeader(header).getTransactionProcessor();

    final MutableWorldState worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();
    final WorldUpdater genesis = worldState.updater();
    genesis.createAccount(CALLER, 1, Wei.ZERO).setCode(CALLER_CODE);
    genesis.createAccount(CALLEE, 1, Wei.ZERO).setCode(CALLEE_CODE);
    genesis.commit();

    final Transaction transaction =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.valueOf(42))
            .nonce(0)
            .maxPriorityFeePerGas(Wei.ZERO)
            .maxFeePerGas(Wei.ZERO)
            .gasLimit(200_000)
            .to(CALLER)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .signAndBuild(KEY_PAIR);
    final WorldUpdater transactionUpdater = worldState.updater().updater();
    final TransactionProcessingResult result =
        transactionProcessor.processTransaction(
            transactionUpdater,
            header,
            transaction,
            COINBASE,
            tracer,
            (frame, number) -> Hash.ZERO,
            TransactionValidationParams.processingBlock(),
            Wei.ZERO);
    assertThat(result.isSuccessful()).as("transaction result %s", result).isTrue();
    return transactionUpdater;
  }
}
