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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.vm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.internal.MemoryEntry;
import org.hyperledger.besu.evm.tracing.TraceFrame;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class VmTraceGeneratorTest {
  @Test
  void mcopyIncludesUpdatedMemory() {
    final Bytes copied = Bytes.fromHexString("0x01020304");
    final TraceFrame frame =
        TraceFrame.builder()
            .setOpcode("MCOPY")
            .setMaybeUpdatedMemory(Optional.of(new MemoryEntry(29, copied)))
            .build();
    final TransactionTrace transactionTrace =
        new TransactionTrace(
            mock(Transaction.class), mock(TransactionProcessingResult.class), List.of(frame));
    final VmTrace trace =
        (VmTrace)
            new VmTraceGenerator(transactionTrace).generateTraceStream().findFirst().orElseThrow();
    assertThat(trace.getVmOperations()).hasSize(1);
    final Mem memory = trace.getVmOperations().getFirst().getVmOperationExecutionReport().getMem();
    assertThat(memory).isNotNull();
    assertThat(memory.getData()).isEqualTo(copied.toHexString());
    assertThat(memory.getOff()).isEqualTo(29);
  }
}
