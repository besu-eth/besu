/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.vm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams.withStateRootAndBlockHashAndUpdateNodeHead;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder.OpCodeTracerConfig;
import org.hyperledger.besu.evm.tracing.TraceFrame;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TraceTransactionIntegrationTest {

  private static final String CONTRACT_CREATION_TX =
      "0xf9014880808347b7608080b8fb608060405234801561001057600080fd5b5060dc8061001f6000396000f3006080604052600436106049576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff1680633fa4f24514604e57806355241077146076575b600080fd5b348015605957600080fd5b50606060a0565b6040518082815260200191505060405180910390f35b348015608157600080fd5b50609e6004803603810190808035906020019092919050505060a6565b005b60005481565b80600081905550505600a165627a7a723058202bdbba2e694dba8fff33d9d0976df580f57bff0a40e25a46c398f8063b4c003600291ca057095e0bd8b08b1311ce81cf202fbaebf1f5bafeadbe9870cec3c0f5dd93bd0ea03639e8e40eedd640eb3565097a96c6c0c553d7e74ecc7b4b571eb9875d23d871";

  private static final String CONTRACT_CREATION_DATA =
      "608060405234801561001057600080fd5b50610228806100206000396000f30060806040526004361061004c576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff168063368b87721461005157806360029940146100ba575b600080fd5b34801561005d57600080fd5b506100b8600480360381019080803590602001908201803590602001908080601f0160208091040260200160405190810160405280939291908181526020018383808284378201915050505050509192919290505050610123565b005b3480156100c657600080fd5b50610121600480360381019080803590602001908201803590602001908080601f016020809104026020016040519081016040528093929190818152602001838380828437820191505050505050919291929050505061013d565b005b8060009080519060200190610139929190610157565b5050565b8060019080519060200190610153929190610157565b5050565b828054600181600116156101000203166002900490600052602060002090601f016020900481019282601f1061019857805160ff19168380011785556101c6565b828001600101855582156101c6579182015b828111156101c55782518255916020019190600101906101aa565b5b5090506101d391906101d7565b5090565b6101f991905b808211156101f55760008160009055506001016101dd565b5090565b905600a165627a7a72305820e01bbaa933adf9c8cbeaa62f07aa3c6349ae777dcb32ae0bbb4c1a4809204aef0029";
  private static final String CALL_SET_OTHER =
      "0x60029940000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000036261720000000000000000000000000000000000000000000000000000000000";
  private MutableBlockchain blockchain;
  private WorldStateArchive worldStateArchive;
  private Block genesisBlock;
  private MainnetTransactionProcessor transactionProcessor;
  private BlockHashLookup blockHashLookup;

  @BeforeEach
  public void setUp() {
    final ExecutionContextTestFixture contextTestFixture =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource("/genesis-it.json")).build();
    genesisBlock = contextTestFixture.getGenesis();
    blockchain = contextTestFixture.getBlockchain();
    worldStateArchive = contextTestFixture.getStateArchive();
    final ProtocolSchedule protocolSchedule = contextTestFixture.getProtocolSchedule();
    final ProtocolSpec protocolSpec =
        protocolSchedule.getByBlockHeader(new BlockHeaderTestFixture().number(0L).buildHeader());

    transactionProcessor = protocolSpec.getTransactionProcessor();
    blockHashLookup =
        protocolSpec
            .getPreExecutionProcessor()
            .createBlockHashLookup(blockchain, genesisBlock.getHeader());
  }

  @Test
  public void shouldTraceSStoreOperation() {
    final KeyPair keyPair = SignatureAlgorithmFactory.getInstance().generateKeyPair();
    final Transaction createTransaction =
        Transaction.builder()
            .type(TransactionType.FRONTIER)
            .gasLimit(300_000)
            .gasPrice(Wei.ZERO)
            .nonce(0)
            .payload(Bytes.fromHexString(CONTRACT_CREATION_DATA))
            .value(Wei.ZERO)
            .signAndBuild(keyPair);

    final BlockHeader genesisBlockHeader = genesisBlock.getHeader();
    final MutableWorldState worldState =
        worldStateArchive
            .getWorldState(
                withStateRootAndBlockHashAndUpdateNodeHead(
                    genesisBlockHeader.getStateRoot(), genesisBlockHeader.getHash()))
            .get();
    final WorldUpdater createTransactionUpdater = worldState.updater();
    TransactionProcessingResult result =
        transactionProcessor.processTransaction(
            createTransactionUpdater,
            genesisBlockHeader,
            createTransaction,
            genesisBlockHeader.getCoinbase(),
            blockHashLookup,
            TransactionValidationParams.blockReplay(),
            Wei.ZERO);
    assertThat(result.isSuccessful()).isTrue();
    final Account createdContract =
        createTransactionUpdater.getTouchedAccounts().stream()
            .filter(account -> !account.getCode().isEmpty())
            .findAny()
            .get();
    createTransactionUpdater.commit();

    // Now call the transaction to execute the SSTORE.
    final DebugOperationTracer tracer =
        new DebugOperationTracer(
            OpCodeTracerConfigBuilder.createFrom(OpCodeTracerConfig.DEFAULT)
                .traceStorage(true)
                .traceMemory(true)
                .traceStack(true)
                .build(),
            false);
    final Transaction executeTransaction =
        Transaction.builder()
            .type(TransactionType.FRONTIER)
            .gasLimit(300_000)
            .gasPrice(Wei.ZERO)
            .nonce(1)
            .payload(Bytes.fromHexString(CALL_SET_OTHER))
            .to(createdContract.getAddress())
            .value(Wei.ZERO)
            .signAndBuild(keyPair);
    final WorldUpdater storeUpdater = worldState.updater();
    result =
        transactionProcessor.processTransaction(
            storeUpdater,
            genesisBlockHeader,
            executeTransaction,
            genesisBlockHeader.getCoinbase(),
            tracer,
            blockHashLookup,
            Wei.ZERO);

    assertThat(result.isSuccessful()).isTrue();

    // Non-storage opcodes produce no storage entry (per execution-apis spec).
    TraceFrame frame = tracer.getTraceFrames().get(170);
    assertThat(frame.getOpcode()).isEqualTo("DUP6");
    assertThat(frame.getStorage()).isEmpty();

    // Storage is emitted only for the SSTORE frame, showing the single slot touched.
    frame = tracer.getTraceFrames().get(171);
    assertThat(frame.getOpcode()).isEqualTo("SSTORE");
    assertStorageContainsExactly(
        frame, entry("0x01", "0x6261720000000000000000000000000000000000000000000000000000000006"));

    // After SSTORE, non-storage opcodes produce no storage entry (per execution-apis spec).
    frame = tracer.getTraceFrames().get(172);
    assertThat(frame.getOpcode()).isEqualTo("PUSH2");
    assertThat(frame.getStorage()).isEmpty();
  }

  @Test
  public void shouldTraceContractCreation() {
    final DebugOperationTracer tracer =
        new DebugOperationTracer(
            OpCodeTracerConfigBuilder.createFrom(OpCodeTracerConfig.DEFAULT)
                .traceStorage(true)
                .traceMemory(true)
                .traceStack(true)
                .build(),
            false);
    final Transaction transaction =
        Transaction.readFrom(
            new BytesValueRLPInput(Bytes.fromHexString(CONTRACT_CREATION_TX), false));
    final BlockHeader genesisBlockHeader = genesisBlock.getHeader();
    transactionProcessor.processTransaction(
        worldStateArchive
            .getWorldState(
                withStateRootAndBlockHashAndUpdateNodeHead(
                    genesisBlockHeader.getStateRoot(), genesisBlockHeader.getHash()))
            .get()
            .updater(),
        genesisBlockHeader,
        transaction,
        genesisBlockHeader.getCoinbase(),
        tracer,
        blockHashLookup,
        Wei.ZERO);

    final int expectedDepth = 0; // Reference impl returned 1. Why the difference?

    final List<TraceFrame> traceFrames = tracer.getTraceFrames();
    assertThat(traceFrames).hasSize(17);

    TraceFrame frame = traceFrames.get(0);
    assertThat(frame.getDepth()).isEqualTo(expectedDepth);
    assertThat(frame.getGasRemaining()).isEqualTo(4632748L);
    assertThat(frame.getGasCost()).isEqualTo(OptionalLong.of(3));
    assertThat(frame.getOpcode()).isEqualTo("PUSH1");
    assertThat(frame.getPc()).isEqualTo(0);
    assertStackContainsExactly(frame);

    frame = traceFrames.get(1);
    assertThat(frame.getDepth()).isEqualTo(expectedDepth);
    assertThat(frame.getGasRemaining()).isEqualTo(4632745L);
    assertThat(frame.getGasCost()).isEqualTo(OptionalLong.of(3L));
    assertThat(frame.getOpcode()).isEqualTo("PUSH1");
    assertThat(frame.getPc()).isEqualTo(2);
    assertStackContainsExactly(frame, "0x80");

    frame = traceFrames.get(2);
    assertThat(frame.getDepth()).isEqualTo(expectedDepth);
    assertThat(frame.getGasRemaining()).isEqualTo(4632742L);
    assertThat(frame.getGasCost()).isEqualTo(OptionalLong.of(12L));
    assertThat(frame.getOpcode()).isEqualTo("MSTORE");
    assertThat(frame.getPc()).isEqualTo(4);
    assertStackContainsExactly(frame, "80", "40");
    assertMemoryContainsExactly(
        frame,
        "0x0000000000000000000000000000000000000000000000000000000000000000",
        "0x0000000000000000000000000000000000000000000000000000000000000000",
        "0x0000000000000000000000000000000000000000000000000000000000000080");
    // Reference implementation actually records the memory after expansion but before the store.
    //    assertMemoryContainsExactly(frame,
    //        "0000000000000000000000000000000000000000000000000000000000000000",
    //        "0000000000000000000000000000000000000000000000000000000000000000",
    //        "0000000000000000000000000000000000000000000000000000000000000000");

    frame = traceFrames.get(3);
    assertThat(frame.getDepth()).isEqualTo(expectedDepth);
    assertThat(frame.getGasRemaining()).isEqualTo(4632730L);
    assertThat(frame.getGasCost()).isEqualTo(OptionalLong.of(2L));
    assertThat(frame.getOpcode()).isEqualTo("CALLVALUE");
    assertThat(frame.getPc()).isEqualTo(5);
    assertStackContainsExactly(frame);
    assertMemoryContainsExactly(
        frame,
        "0000000000000000000000000000000000000000000000000000000000000000",
        "0000000000000000000000000000000000000000000000000000000000000000",
        "0000000000000000000000000000000000000000000000000000000000000080");
  }

  private void assertStackContainsExactly(
      final TraceFrame frame, final String... stackEntriesAsHex) {
    assertThat(frame.getStack()).isPresent();
    final Bytes[] stackEntries =
        Stream.of(stackEntriesAsHex).map(Bytes::fromHexString).toArray(Bytes[]::new);
    assertThat(frame.getStack().get()).containsExactly(stackEntries);
  }

  private void assertMemoryContainsExactly(
      final TraceFrame frame, final String... memoryEntriesAsHex) {
    assertThat(frame.getMemory()).isPresent();
    final Bytes32[] memoryEntries =
        Stream.of(memoryEntriesAsHex).map(Bytes32::fromHexString).toArray(Bytes32[]::new);
    assertThat(frame.getMemory().get()).containsExactly(memoryEntries);
  }

  @SuppressWarnings("unchecked")
  @SafeVarargs
  private void assertStorageContainsExactly(
      final TraceFrame frame, final Map.Entry<String, String>... memoryEntriesAsHex) {
    assertThat(frame.getMemory()).isPresent();
    final Map.Entry<UInt256, UInt256>[] memoryEntries =
        Stream.of(memoryEntriesAsHex)
            .map(
                entry ->
                    entry(
                        UInt256.fromHexString(entry.getKey()),
                        UInt256.fromHexString(entry.getValue())))
            .toArray(Map.Entry[]::new);
    assertThat(frame.getStorage().get()).containsExactly(memoryEntries);
  }
}
