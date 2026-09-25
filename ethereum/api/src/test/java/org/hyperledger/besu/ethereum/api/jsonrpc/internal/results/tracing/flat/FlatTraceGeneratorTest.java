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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.flat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.Trace;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.tracing.TraceFrame;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class FlatTraceGeneratorTest {
  @Mock private Transaction transaction;
  @Mock private TransactionProcessingResult transactionProcessingResult;

  @Test
  public void testGenerateFromTransactionTraceWithRevertReason() {
    final Bytes revertReason = Bytes.random(32);
    Mockito.when(transaction.getSender()).thenReturn(Address.ZERO);
    Mockito.when(transactionProcessingResult.getRevertReason())
        .thenReturn(Optional.of(revertReason));

    final TransactionTrace transactionTrace =
        new TransactionTrace(transaction, transactionProcessingResult, Collections.emptyList());
    final Stream<Trace> traceStream =
        FlatTraceGenerator.generateFromTransactionTrace(
            null, transactionTrace, null, new AtomicInteger());
    final List<Trace> traces = traceStream.collect(Collectors.toList());

    Assertions.assertThat(traces.isEmpty()).isFalse();
    Assertions.assertThat(traces.get(0)).isNotNull();
    Assertions.assertThat(traces.get(0) instanceof FlatTrace).isTrue();
    Assertions.assertThat(((FlatTrace) traces.get(0)).getRevertReason())
        .isEqualTo(revertReason.toHexString());
  }

  @Test
  public void nestedCreateEndingInSelfDestructReportsEmptyCode() {
    final Address created = Address.fromHexString("0x0000000000000000000000000000000000001234");
    final Bytes initCode = Bytes.fromHexString("0x6000ff");
    Mockito.when(transaction.getSender()).thenReturn(Address.ZERO);
    Mockito.when(transaction.getValue()).thenReturn(Wei.ZERO);
    Mockito.when(transaction.getTo()).thenReturn(Optional.of(Address.ZERO));
    Mockito.when(transaction.getPayload()).thenReturn(Bytes.EMPTY);
    Mockito.when(transactionProcessingResult.getRevertReason()).thenReturn(Optional.empty());

    final TraceFrame create =
        TraceFrame.builder()
            .setOpcode("CREATE")
            .setGasRemaining(100_000L)
            .setGasCost(OptionalLong.of(32_000L))
            .setDepth(0)
            .setRecipient(Address.ZERO)
            .setValue(Wei.ZERO)
            .setMaybeCode(Optional.of(new Code(initCode)))
            .build();
    final TraceFrame selfDestruct =
        TraceFrame.builder()
            .setOpcode("SELFDESTRUCT")
            .setGasRemaining(60_000L)
            .setGasCost(OptionalLong.of(5_000L))
            .setDepth(1)
            .setRecipient(created)
            .setValue(Wei.ZERO)
            .setOutputData(Bytes.EMPTY)
            .setStack(Optional.of(new Bytes[] {Bytes32.ZERO}))
            .build();

    final TransactionTrace trace =
        new TransactionTrace(
            transaction, transactionProcessingResult, List.of(create, selfDestruct));
    final FlatTrace creation =
        FlatTraceGenerator.generateFromTransactionTrace(null, trace, null, new AtomicInteger())
            .map(FlatTrace.class::cast)
            .filter(flatTrace -> flatTrace.getType().equals("create"))
            .findFirst()
            .orElseThrow();

    Assertions.assertThat(creation.getAction().getInit()).isEqualTo(initCode.toHexString());
    Assertions.assertThat(creation.getResult().get().getCode()).isEqualTo("0x");
    Assertions.assertThat(creation.getResult().get().getOutput()).isNull();
  }
}
