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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.calltrace;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.CallTracerResult;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.debug.TracerType;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.math.BigInteger;
import java.util.Map;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class CallTracerTopFrameHaltTest {

  private static final String GENESIS_RESOURCE =
      "/org/hyperledger/besu/ethereum/mainnet/genesis-bp-it.json";
  // Funded in GENESIS_RESOURCE, which activates Amsterdam at genesis.
  private static final String SENDER_PRIVATE_KEY =
      "3a4ff6d22d7502ef2452368165422861c01a0f72f851793b372b87888dc3c453";
  private static final BigInteger CHAIN_ID = BigInteger.valueOf(42);

  private final ExecutionContextTestFixture fixture =
      ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS_RESOURCE))
          .dataStorageFormat(DataStorageFormat.BONSAI)
          .build();
  private final BlockHeader header = fixture.getBlockchain().getChainHeadHeader();
  private final ProtocolSpec spec = fixture.getProtocolSchedule().getByBlockHeader(header);

  @Test
  void tracesContractCreationThatRunsOutOfGasBeforeExecutionStarts() {
    final CallTracer tracer =
        new CallTracer(new TraceOptions(TracerType.CALL_TRACER, null, Map.of()));
    final Transaction tx = contractCreationWithIntrinsicGasOnly();

    final TransactionProcessingResult result = process(tx, tracer);

    assertThat(result.isInvalid()).as(result.getValidationResult().toString()).isFalse();
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getExceptionalHaltReason()).contains(ExceptionalHaltReason.INSUFFICIENT_GAS);

    final CallTracerResult trace = tracer.buildResult(tx, result);
    assertThat(trace.getType()).isEqualTo("CREATE");
    assertThat(trace.getError()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS.getDescription());
    assertThat(trace.getCalls()).isNullOrEmpty();
  }

  @Test
  void haltedTopFrameIsEnteredAndExitedWithoutExecuting() {
    final OperationTracer tracer = mock(OperationTracer.class);

    final TransactionProcessingResult result =
        process(contractCreationWithIntrinsicGasOnly(), tracer);

    assertThat(result.getExceptionalHaltReason()).contains(ExceptionalHaltReason.INSUFFICIENT_GAS);
    final InOrder inOrder = inOrder(tracer);
    inOrder.verify(tracer).traceContextEnter(any());
    inOrder.verify(tracer).traceContextExit(any());
    verify(tracer, never()).traceContextReEnter(any());
    verify(tracer, never()).tracePreExecution(any());
  }

  private Transaction contractCreationWithIntrinsicGasOnly() {
    // Intrinsic gas excludes the EIP-8037 state gas of the created account, so the top frame
    // halts while charging it, before any code runs.
    final GasCalculator gasCalculator = spec.getGasCalculator();
    final Transaction template = contractCreation(1_000_000L);
    final long intrinsicGas =
        Math.max(
            gasCalculator.transactionIntrinsicGasCost(template, 0L),
            gasCalculator.transactionFloorCost(template));
    return contractCreation(intrinsicGas);
  }

  private TransactionProcessingResult process(final Transaction tx, final OperationTracer tracer) {
    return spec.getTransactionProcessor()
        .processTransaction(
            fixture.getStateArchive().getWorldState().updater(),
            header,
            tx,
            header.getCoinbase(),
            tracer,
            (frame, number) -> Hash.ZERO,
            Wei.ZERO);
  }

  private static Transaction contractCreation(final long gasLimit) {
    final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();
    final KeyPair keyPair =
        signatureAlgorithm.createKeyPair(
            signatureAlgorithm.createPrivateKey(Bytes32.fromHexString(SENDER_PRIVATE_KEY)));
    return Transaction.builder()
        .type(TransactionType.EIP1559)
        .nonce(0)
        .maxPriorityFeePerGas(Wei.ZERO)
        .maxFeePerGas(Wei.of(1_000_000_000L))
        .gasLimit(gasLimit)
        .value(Wei.ZERO)
        .payload(Bytes.fromHexString("0x00"))
        .chainId(CHAIN_ID)
        .signAndBuild(keyPair);
  }
}
