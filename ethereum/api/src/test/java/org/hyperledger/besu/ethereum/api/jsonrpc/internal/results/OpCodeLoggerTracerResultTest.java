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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.tracing.TraceFrame;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class OpCodeLoggerTracerResultTest {

  private final Transaction transaction = mock(Transaction.class);
  private final TransactionProcessingResult result = mock(TransactionProcessingResult.class);

  @Test
  void legacyTransferHasEmptyStructLogsAndZeroXReturnValue() {
    when(result.getOutput()).thenReturn(Bytes.EMPTY);
    when(result.isSuccessful()).thenReturn(true);

    final TransactionTrace trace =
        new TransactionTrace(
            transaction, result, List.of(virtualStop(Optional.of(Code.EMPTY_CODE))));

    final OpCodeLoggerTracerResult opCodeResult = new OpCodeLoggerTracerResult(trace);

    assertThat(opCodeResult.getReturnValue()).isEqualTo("0x");
    assertThat(opCodeResult.getStructLogs()).isEmpty();
  }

  @Test
  void fellOffEndVirtualStopIsRetained() {
    when(result.getOutput()).thenReturn(Bytes.EMPTY);
    when(result.isSuccessful()).thenReturn(true);

    final Code nonEmptyCode = new Code(Bytes.fromHexString("0x6000"));
    final TransactionTrace trace =
        new TransactionTrace(transaction, result, List.of(virtualStop(Optional.of(nonEmptyCode))));

    final OpCodeLoggerTracerResult opCodeResult = new OpCodeLoggerTracerResult(trace);

    assertThat(opCodeResult.getStructLogs()).hasSize(1);
    assertThat(opCodeResult.getStructLogs().get(0).op()).isEqualTo("STOP");
  }

  private TraceFrame virtualStop(final Optional<Code> maybeCode) {
    return TraceFrame.builder()
        .setPc(0)
        .setOpcode("STOP")
        .setOpcodeNumber(0x00)
        .setGasRemaining(0L)
        .setGasCost(OptionalLong.empty())
        .setGasRefund(0L)
        .setDepth(0)
        .setRecipient(null)
        .setValue(Wei.ZERO)
        .setInputData(Bytes.EMPTY)
        .setOutputData(Bytes.EMPTY)
        .setStack(Optional.of(new Bytes32[0]))
        .setWorldUpdater(null)
        .setStackItemsProduced(0)
        .setMaybeCode(maybeCode)
        .setVirtualOperation(true)
        .build();
  }
}
