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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.Trace;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class FlatTraceGeneratorTest {
  @Mock private Transaction transaction;
  @Mock private TransactionProcessingResult transactionProcessingResult;

  @ParameterizedTest
  @ValueSource(strings = {"0x", "0x123456"})
  public void retainsOutputForCallsWithoutOpcodeFrames(final String outputHex) {
    final Bytes output = Bytes.fromHexString(outputHex);
    Mockito.when(transaction.getSender()).thenReturn(Address.ZERO);
    Mockito.when(transaction.getTo()).thenReturn(Optional.of(Address.fromHexString("0x04")));
    Mockito.when(transaction.getPayload()).thenReturn(output);
    Mockito.when(transactionProcessingResult.getOutput()).thenReturn(output);
    Mockito.when(transactionProcessingResult.isSuccessful()).thenReturn(true);

    final TransactionTrace transactionTrace =
        new TransactionTrace(transaction, transactionProcessingResult, List.of());
    final List<Trace> traces =
        FlatTraceGenerator.generateFromTransactionTrace(
                null, transactionTrace, null, new AtomicInteger())
            .toList();

    Assertions.assertThat(traces).hasSize(1);
    final FlatTrace trace = (FlatTrace) traces.getFirst();
    Assertions.assertThat(trace.getResult().get().getOutput()).isEqualTo(outputHex);
    Assertions.assertThat(trace.getAction().getInput()).isEqualTo(outputHex);
  }

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
}
