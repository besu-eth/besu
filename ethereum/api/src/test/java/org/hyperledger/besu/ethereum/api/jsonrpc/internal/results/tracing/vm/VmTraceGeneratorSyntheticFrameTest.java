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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.tracing.TraceFrame;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class VmTraceGeneratorSyntheticFrameTest {

  @Test
  public void rootCreationThatFailsBeforeItsFirstOpcodeHasNoOperations() {
    // A root creation whose address collides halts before executing any opcode; the tracer then
    // records a synthetic frame carrying the halt.
    final Transaction transaction = mock(Transaction.class);
    when(transaction.getInit()).thenReturn(Optional.of(Bytes.fromHexString("0x60006000f3")));
    final TraceFrame collision =
        TraceFrame.builder()
            .setPc(0)
            .setOpcodeNumber(Integer.MAX_VALUE)
            .setGasRemaining(100_000L)
            .setDepth(0)
            .setExceptionalHaltReason(Optional.of(ExceptionalHaltReason.ILLEGAL_STATE_CHANGE))
            .setVirtualOperation(true)
            .build();
    final TransactionTrace transactionTrace =
        new TransactionTrace(
            transaction, mock(TransactionProcessingResult.class), List.of(collision));

    final VmTrace vmTrace =
        (VmTrace) new VmTraceGenerator(transactionTrace).generateTraceStream().findFirst().get();

    assertThat(vmTrace.getVmOperations()).isEmpty();
  }
}
