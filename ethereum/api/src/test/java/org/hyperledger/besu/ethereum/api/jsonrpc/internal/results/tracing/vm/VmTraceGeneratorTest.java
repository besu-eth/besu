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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.vm;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecuteTransactionStep;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TraceBlock.ChainUpdater;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
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

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Runs init code through the Osaka EVM and checks the compressed VM trace built from it. */
class VmTraceGeneratorTest {

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

  private ExecutionContextTestFixture fixture;

  @BeforeEach
  void setUp() {
    fixture =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS_RESOURCE))
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
  }

  @Test
  void mcopyReportsDestinationWrite() {
    // MSTORE(0, 0x2a); MCOPY(dst=0x20, src=0, len=0x20); STOP
    final VmTrace trace = vmTrace("0x602a600052602060006020" + "5e" + "00");

    final Mem mem = memAt(trace, 11);
    assertThat(mem).isNotNull();
    assertThat(mem.getOff()).isEqualTo(0x20);
    assertThat(mem.getData()).isEqualTo(WORD_2A);
  }

  @Test
  void zeroLengthMcopyAfterCallReportsNoMemoryWrite() {
    // MSTORE(0, 0x2a); push MCOPY len=0, src=0;
    // CALL(GAS, identity, 0, in=0..0x20, out=0x20..0x40); MCOPY(dst=success, src=0, len=0); STOP
    final VmTrace trace =
        vmTrace("0x602a600052" + "60006000" + "6020602060206000600060045af1" + "5e" + "00");

    final Mem callMem = memAt(trace, 22);
    assertThat(callMem).isNotNull();
    assertThat(callMem.getOff()).isEqualTo(0x20);
    assertThat(callMem.getData()).isEqualTo(WORD_2A);
    assertThat(memAt(trace, 23)).isNull();
  }

  @Test
  void outOfGasMcopyAfterCallReportsNoMemoryWrite() {
    // same as above, but MCOPY len=0xffffff so its memory expansion runs out of gas
    final VmTrace trace =
        vmTrace("0x602a600052" + "62ffffff6000" + "6020602060206000600060045af1" + "5e" + "00");
    assertThat(memAt(trace, 24)).isNotNull();
    final VmOperationExecutionReport report =
        operationAt(trace, 25).getVmOperationExecutionReport();
    if (report != null) {
      assertThat(report.getMem()).isNull();
    }
  }

  private VmTrace vmTrace(final String initCode) {
    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(5))
            .maxFeePerGas(Wei.of(7))
            .gasLimit(200_000L)
            .value(Wei.ZERO)
            .payload(Bytes.fromHexString(initCode))
            .chainId(BigInteger.valueOf(42))
            .signAndBuild(KEY_PAIR);
    final BlockHeader genesis = fixture.getBlockchain().getChainHeadHeader();
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .number(genesis.getNumber() + 1L)
            .parentHash(genesis.getHash())
            .gasLimit(30_000_000L)
            .baseFeePerGas(Wei.of(7))
            .buildHeader();
    final Block block =
        new Block(header, new BlockBody(List.of(tx), Collections.emptyList(), Optional.empty()));
    final ProtocolSpec protocolSpec = fixture.getProtocolSchedule().getByBlockHeader(header);
    // same tracer configuration as trace_replayBlockTransactions
    final DebugOperationTracer tracer =
        new DebugOperationTracer(
            OpCodeTracerConfigBuilder.createFrom(OpCodeTracerConfig.DEFAULT)
                .traceStorage(false)
                .traceMemory(false)
                .traceStack(true)
                .build(),
            false);
    final TransactionTrace transactionTrace =
        new ExecuteTransactionStep(
                new ChainUpdater(fixture.getStateArchive().getWorldState()),
                protocolSpec.getTransactionProcessor(),
                fixture.getBlockchain(),
                tracer,
                protocolSpec,
                block)
            .apply(new TransactionTrace(tx, Optional.of(block)));
    return (VmTrace)
        new VmTraceGenerator(transactionTrace).generateTraceStream().findFirst().orElseThrow();
  }

  private static Mem memAt(final VmTrace trace, final long pc) {
    return operationAt(trace, pc).getVmOperationExecutionReport().getMem();
  }

  private static VmOperation operationAt(final VmTrace trace, final long pc) {
    return trace.getVmOperations().stream()
        .filter(op -> op.getPc() == pc)
        .findFirst()
        .orElseThrow();
  }
}
