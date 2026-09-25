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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.flat;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecuteTransactionStep;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TraceBlock.ChainUpdater;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.vm.VmTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.vm.VmTraceGenerator;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder.OpCodeTracerConfig;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class PrecompileFlatTraceTest {
  private static final String GENESIS_RESOURCE =
      "/org/hyperledger/besu/ethereum/api/jsonrpc/trace/chain-data/genesis-osaka.json";
  private static final KeyPair KEY_PAIR =
      SignatureAlgorithmFactory.getInstance()
          .createKeyPair(
              SECPPrivateKey.create(
                  Bytes32.fromHexString(
                      "c87509a1c067bbde78beb793e6fa76530b6382a4c0241e5e4a9ec0a0f44dc0d3"),
                  SignatureAlgorithm.ALGORITHM));
  private static final String WORD_2A = Bytes32.leftPad(Bytes.of(0x2a)).toHexString();
  private static final String INITIALIZE_MEMORY = "602a600052";
  // Return the call's success bit as deployed code, independently checking EVM execution.
  private static final String RETURN_SUCCESS = "60405260206040f3";
  private ExecutionContextTestFixture fixture;
  private Block block;

  @BeforeEach
  void setUp() {
    fixture =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS_RESOURCE))
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
  }

  @ParameterizedTest
  @CsvSource({
    "CALL,0,0,false", "CALL,0,1,false", "CALL,1,0,false", "CALL,1,1,false",
    "CALLCODE,0,0,false", "CALLCODE,0,1,false", "CALLCODE,1,0,false", "CALLCODE,1,1,false",
    "DELEGATECALL,0,0,false", "DELEGATECALL,1,0,false", "STATICCALL,0,0,false",
        "STATICCALL,1,0,false",
    "CALL,0,0,true", "CALL,0,1,true", "CALL,1,0,true", "CALL,1,1,true",
    "CALLCODE,0,0,true", "CALLCODE,0,1,true", "CALLCODE,1,0,true", "CALLCODE,1,1,true",
    "DELEGATECALL,0,0,true", "DELEGATECALL,1,0,true", "STATICCALL,0,0,true", "STATICCALL,1,0,true"
  })
  void retainsOnlyNonzeroValuePrecompiles(
      final String opcode, final int outerValue, final int callValue, final boolean failure) {
    // Identity succeeds; bn128Add rejects the same non-curve input. Both execute as precompiles.
    final int target = failure ? 6 : 4;
    final TransactionTrace tx =
        execute(
            INITIALIZE_MEMORY + call(opcode, target, callValue, 100_000) + RETURN_SUCCESS,
            outerValue,
            2,
            null);
    assertThat(tx.getResult().isSuccessful()).as(tx.getResult().toString()).isTrue();
    assertThat(tx.getResult().getOutput()).isEqualTo(Bytes32.leftPad(Bytes.of(failure ? 0 : 1)));
    final List<FlatTrace> traces = flat(tx);
    final int value =
        switch (opcode) {
          case "CALL", "CALLCODE" -> callValue;
          case "DELEGATECALL" -> outerValue;
          default -> 0;
        };
    assertThat(traces).hasSize(value == 0 ? 1 : 2);
    assertThat(traces.getFirst().getError()).isNull();
    assertThat(traces.getFirst().getSubtraces()).isEqualTo(value == 0 ? 0 : 1);
    if (value != 0) {
      final FlatTrace child = traces.get(1);
      assertThat(child.getTraceAddress()).containsExactly(0);
      assertThat(child.getSubtraces()).isZero();
      assertThat(child.getAction().getCallType())
          .isEqualTo(opcode.toLowerCase(java.util.Locale.ROOT));
      assertThat(child.getAction().getFrom())
          .isEqualTo(Address.contractAddress(tx.getTransaction().getSender(), 0).toHexString());
      assertThat(child.getAction().getTo())
          .isEqualTo(Address.fromHexString("0x" + Integer.toHexString(target)).toHexString());
      assertThat(child.getAction().getValue()).isEqualTo("0x1");
      assertThat(child.getAction().getInput()).isEqualTo(WORD_2A);
      // CALL and CALLCODE add the value stipend; DELEGATECALL does not transfer value.
      assertThat(Long.decode(child.getAction().getGas()))
          .isEqualTo(opcode.equals("DELEGATECALL") ? 100_000L : 102_300L);
      if (failure) {
        assertThat(child.getError()).isNotNull();
        assertThat(child.getResult()).isNull();
      } else {
        assertThat(child.getError()).isNull();
        assertThat(child.getResult().get().getOutput()).isEqualTo(WORD_2A);
        assertThat(child.getResult().get().getGasUsed()).isEqualTo("0x12");
      }
    }
    if (!failure) {
      final VmTrace vm =
          (VmTrace) new VmTraceGenerator(tx).generateTraceStream().findFirst().orElseThrow();
      final var op =
          vm.getVmOperations().stream()
              .filter(
                  o ->
                      o.getVmOperationExecutionReport() != null
                          && o.getVmOperationExecutionReport().getMem() != null
                          && o.getVmOperationExecutionReport().getMem().getOff() == 32)
              .findFirst()
              .orElseThrow();
      assertThat(op.getVmOperationExecutionReport().getMem().getData()).isEqualTo(WORD_2A);
    }
  }

  @Test
  void numbersOnlyRetainedPrecompileSiblings() {
    final TransactionTrace tx =
        execute(
            INITIALIZE_MEMORY
                + call("CALL", 4, 0, 100_000)
                + "50"
                + call("CALL", 4, 1, 100_000)
                + "50"
                + call("CALL", 6, 0, 100_000)
                + "50"
                + call("CALL", 6, 1, 100_000)
                + "50"
                + call("CALL", 4, 1, 100_000)
                + RETURN_SUCCESS,
            0,
            3,
            null);
    final List<FlatTrace> traces = flat(tx);
    assertThat(traces).hasSize(4);
    assertThat(traces.getFirst().getSubtraces()).isEqualTo(3);
    assertThat(traces.getFirst().getError()).isNull();
    assertThat(traces.subList(1, 4))
        .extracting(FlatTrace::getTraceAddress)
        .containsExactly(List.of(0), List.of(1), List.of(2));
    assertThat(traces.get(1).getError()).isNull();
    assertThat(traces.get(2).getError()).isNotNull();
    assertThat(traces.get(3).getError()).isNull();
  }

  @Test
  void preservesParentContextAroundNestedPrecompiles() {
    final Address callee = Address.fromHexString("0x20");
    final var updater = fixture.getStateArchive().getWorldState().updater();
    final var account = updater.getOrCreate(callee);
    account.setBalance(Wei.of(2));
    account.setCode(
        Bytes.fromHexString(
            INITIALIZE_MEMORY
                + call("CALL", 4, 0, 100_000)
                + "50"
                + call("CALL", 6, 1, 100_000)
                + "50"
                + call("CALL", 4, 1, 100_000)
                + RETURN_SUCCESS));
    updater.commit();
    final TransactionTrace tx =
        execute(
            INITIALIZE_MEMORY
                + call("CALL", 32, 1, 500_000)
                + "50"
                + call("CALL", 4, 1, 100_000)
                + RETURN_SUCCESS,
            0,
            2,
            null);
    assertThat(tx.getResult().isSuccessful()).as(tx.getResult().toString()).isTrue();
    final List<FlatTrace> traces = flat(tx);
    assertThat(traces)
        .extracting(FlatTrace::getTraceAddress)
        .containsExactly(List.of(), List.of(0), List.of(0, 0), List.of(0, 1), List.of(1));
    assertThat(traces).extracting(FlatTrace::getSubtraces).containsExactly(2, 2, 0, 0, 0);
    assertThat(traces.get(1).getError()).isNull();
    assertThat(traces.get(2).getError()).isNotNull();
    assertThat(traces.get(3).getError()).isNull();
    assertThat(traces.get(2).getAction().getFrom()).isEqualTo(callee.toHexString());
    assertThat(traces.get(3).getAction().getFrom()).isEqualTo(callee.toHexString());
  }

  @Test
  void omitsCallRejectedBeforePrecompileExecution() {
    final TransactionTrace tx =
        execute(INITIALIZE_MEMORY + call("CALL", 4, 1, 100_000) + RETURN_SUCCESS, 0, 0, null);
    assertThat(tx.getResult().getOutput()).isEqualTo(Bytes32.ZERO);
    assertThat(tx.getTraceFrames()).noneMatch(f -> f.isPrecompile());
    assertThat(flat(tx)).hasSize(1);
    assertThat(flat(tx).getFirst().getSubtraces()).isZero();
  }

  @Test
  void retainsOutOfGasPrecompileWithoutHaltingCaller() {
    // Value stipend is only 2300; pairing requires 45000 even for empty input.
    final TransactionTrace tx =
        execute(INITIALIZE_MEMORY + call("CALL", 8, 1, 0) + RETURN_SUCCESS, 0, 1, null);
    assertThat(tx.getResult().isSuccessful()).as(tx.getResult().toString()).isTrue();
    assertThat(tx.getResult().getOutput()).isEqualTo(Bytes32.ZERO);
    final List<FlatTrace> traces = flat(tx);
    assertThat(traces).hasSize(2);
    assertThat(traces.getFirst().getError()).isNull();
    assertThat(traces.get(1).getAction().getGas()).isEqualTo("0x8fc");
    assertThat(traces.get(1).getError()).isEqualTo("Out of gas");
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1})
  void retainsRootPrecompiles(final int value) {
    final TransactionTrace tx =
        execute(WORD_2A.substring(2), value, 0, Address.fromHexString("0x4"));
    assertThat(tx.getResult().isSuccessful()).as(tx.getResult().toString()).isTrue();
    assertThat(flat(tx))
        .singleElement()
        .satisfies(
            t -> {
              assertThat(t.getTraceAddress()).isEmpty();
              assertThat(t.getSubtraces()).isZero();
            });
  }

  private static String call(
      final String opcode, final int target, final int value, final int gas) {
    final String op =
        switch (opcode) {
          case "CALL" -> "f1";
          case "CALLCODE" -> "f2";
          case "DELEGATECALL" -> "f4";
          case "STATICCALL" -> "fa";
          default -> throw new IllegalArgumentException(opcode);
        };
    return "6020602060206000"
        + (opcode.equals("CALL") || opcode.equals("CALLCODE") ? "60%02x".formatted(value) : "")
        + "60%02x62%06x".formatted(target, gas)
        + op;
  }

  private TransactionTrace execute(
      final String code, final int value, final int prefund, final Address to) {
    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(5))
            .maxFeePerGas(Wei.of(7))
            .gasLimit(1_000_000L)
            .value(Wei.of(value))
            .to(to)
            .payload(Bytes.fromHexString("0x" + code))
            .chainId(BigInteger.valueOf(42))
            .signAndBuild(KEY_PAIR);
    final var world = fixture.getStateArchive().getWorldState();
    if (prefund > 0) {
      final var updater = world.updater();
      updater.getOrCreate(Address.contractAddress(tx.getSender(), 0)).setBalance(Wei.of(prefund));
      updater.commit();
    }
    final BlockHeader genesis = fixture.getBlockchain().getChainHeadHeader();
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .number(genesis.getNumber() + 1)
            .parentHash(genesis.getHash())
            .gasLimit(30_000_000L)
            .baseFeePerGas(Wei.of(7))
            .buildHeader();
    block =
        new Block(header, new BlockBody(List.of(tx), Collections.emptyList(), Optional.empty()));
    final ProtocolSpec spec = fixture.getProtocolSchedule().getByBlockHeader(header);
    final DebugOperationTracer tracer =
        new DebugOperationTracer(
            OpCodeTracerConfigBuilder.createFrom(OpCodeTracerConfig.DEFAULT)
                .traceStorage(false)
                .traceMemory(false)
                .traceStack(true)
                .build(),
            false);
    return new ExecuteTransactionStep(
            new ChainUpdater(world),
            spec.getTransactionProcessor(),
            fixture.getBlockchain(),
            tracer,
            spec,
            block)
        .apply(new TransactionTrace(tx, Optional.of(block)));
  }

  private List<FlatTrace> flat(final TransactionTrace tx) {
    return FlatTraceGenerator.generateFromTransactionTrace(
            fixture.getProtocolSchedule(), tx, block, new AtomicInteger())
        .map(t -> (FlatTrace) t)
        .toList();
  }
}
