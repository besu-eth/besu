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

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.CallTracerResult;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.debug.TracerType;
import org.hyperledger.besu.ethereum.mainnet.TopFrameHaltTransactions;
import org.hyperledger.besu.ethereum.mainnet.TopFrameHaltTransactions.Halt;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;

import java.util.Map;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class CallTracerTopFrameHaltTest {

  private final TopFrameHaltTransactions transactions = new TopFrameHaltTransactions();

  @ParameterizedTest
  @EnumSource(Halt.class)
  void tracesTopFrameThatRunsOutOfGasBeforeExecutionStarts(final Halt halt) {
    final CallTracer tracer =
        new CallTracer(new TraceOptions(TracerType.CALL_TRACER, null, Map.of()));
    final Transaction tx = transactions.transaction(halt);

    final TransactionProcessingResult result = transactions.process(tx, tracer);

    assertThat(result.isInvalid()).as(result.getValidationResult().toString()).isFalse();
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getExceptionalHaltReason()).contains(ExceptionalHaltReason.INSUFFICIENT_GAS);

    final CallTracerResult trace = tracer.buildResult(tx, result);
    assertThat(trace.getType())
        .isEqualTo(halt.frameType() == MessageFrame.Type.CONTRACT_CREATION ? "CREATE" : "CALL");
    assertThat(trace.getError()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS.getDescription());
    assertThat(trace.getCalls()).isNullOrEmpty();
  }
}
