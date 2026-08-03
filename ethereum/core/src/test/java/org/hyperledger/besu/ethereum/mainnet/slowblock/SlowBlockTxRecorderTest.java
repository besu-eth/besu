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
package org.hyperledger.besu.ethereum.mainnet.slowblock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.AbstractCallOperation;
import org.hyperledger.besu.evm.operation.AbstractCreateOperation;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.SLoadOperation;
import org.hyperledger.besu.evm.operation.SStoreOperation;
import org.hyperledger.besu.evm.operation.StopOperation;

import org.junit.jupiter.api.Test;

class SlowBlockTxRecorderTest {

  private static final Address CONTRACT =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");
  private static final Address EOA =
      Address.fromHexString("0x00000000000000000000000000000000000000bb");

  @Test
  void countsTheOperationsTheSpecAsksFor() {
    final SlowBlockTxRecorder recorder = new SlowBlockTxRecorder();

    execute(recorder, mock(SLoadOperation.class));
    execute(recorder, mock(SStoreOperation.class));
    execute(recorder, mock(SStoreOperation.class));
    execute(recorder, mock(AbstractCallOperation.class));
    execute(recorder, mock(AbstractCreateOperation.class));
    execute(recorder, mock(StopOperation.class));

    assertThat(recorder.sloadCount()).isEqualTo(1);
    assertThat(recorder.sstoreCount()).isEqualTo(2);
    assertThat(recorder.callCount()).isEqualTo(1);
    assertThat(recorder.createCount()).isEqualTo(1);
  }

  @Test
  void uniqueContractsCountCodeExecutionNotEveryCallTarget() {
    final SlowBlockTxRecorder recorder = new SlowBlockTxRecorder();

    recorder.traceContextEnter(frameWithCode(CONTRACT, 120));
    recorder.traceContextEnter(frameWithCode(CONTRACT, 120)); // same contract, entered twice
    recorder.traceContextEnter(frameWithCode(EOA, 0)); // value transfer or precompile

    assertThat(recorder.contractsExecuted()).containsExactly(CONTRACT);
  }

  @Test
  void mergingFoldsCountersIntoTheBlockAggregate() {
    final SlowBlockTxRecorder recorder = new SlowBlockTxRecorder();
    execute(recorder, mock(SLoadOperation.class));
    recorder.traceContextEnter(frameWithCode(CONTRACT, 10));

    final SlowBlockMetrics metrics = new SlowBlockMetrics(0);
    recorder.mergeInto(metrics);

    assertThat(metrics.sload()).isEqualTo(1);
    assertThat(metrics.uniqueContracts()).isEqualTo(1);
  }

  @Test
  void discardedRecordersLeaveTheBlockAggregateUntouched() {
    final SlowBlockTxRecorder used = new SlowBlockTxRecorder();
    execute(used, mock(SLoadOperation.class));

    final SlowBlockTxRecorder discarded = new SlowBlockTxRecorder();
    execute(discarded, mock(SLoadOperation.class));
    execute(discarded, mock(SStoreOperation.class));

    // speculative work that loses is never merged
    final SlowBlockMetrics metrics = new SlowBlockMetrics(0);
    metrics.mergeExecution(used, null);

    assertThat(metrics.sload()).isEqualTo(1);
    assertThat(metrics.sstore()).isZero();
  }

  private void execute(final SlowBlockTxRecorder recorder, final Operation operation) {
    final MessageFrame frame = mock(MessageFrame.class);
    when(frame.getCurrentOperation()).thenReturn(operation);
    recorder.tracePostExecution(frame, null);
  }

  private MessageFrame frameWithCode(final Address contract, final int codeSize) {
    final Code code = mock(Code.class);
    when(code.getSize()).thenReturn(codeSize);
    final MessageFrame frame = mock(MessageFrame.class);
    when(frame.getCode()).thenReturn(code);
    when(frame.getContractAddress()).thenReturn(contract);
    return frame;
  }
}
