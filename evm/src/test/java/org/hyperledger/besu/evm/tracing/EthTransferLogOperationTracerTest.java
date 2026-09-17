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
package org.hyperledger.besu.evm.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.MessageFrame;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EthTransferLogOperationTracerTest {

  private static final Address SENDER =
      Address.fromHexString("0x1000000000000000000000000000000000000001");
  private static final Address RECIPIENT =
      Address.fromHexString("0x2000000000000000000000000000000000000002");
  private static final Address OTHER =
      Address.fromHexString("0x3000000000000000000000000000000000000003");

  private EthTransferLogOperationTracer tracer;

  @Mock private MessageFrame parentFrame;
  @Mock private MessageFrame childFrame;

  @BeforeEach
  void setUp() {
    tracer = new EthTransferLogOperationTracer();
  }

  @Test
  void transferLogEmittedOnValueTransfer() {
    when(parentFrame.getValue()).thenReturn(Wei.of(1000));
    when(parentFrame.getSenderAddress()).thenReturn(SENDER);
    when(parentFrame.getRecipientAddress()).thenReturn(RECIPIENT);

    tracer.traceContextEnter(parentFrame);

    assertThat(tracer.getLogs()).hasSize(1);
  }

  @Test
  void noLogEmittedForZeroValueTransfer() {
    when(parentFrame.getValue()).thenReturn(Wei.ZERO);

    tracer.traceContextEnter(parentFrame);

    assertThat(tracer.getLogs()).isEmpty();
  }

  @Test
  void allLogsClearedWhenTopLevelFrameFails() {
    when(parentFrame.getValue()).thenReturn(Wei.of(1000));
    when(parentFrame.getSenderAddress()).thenReturn(SENDER);
    when(parentFrame.getRecipientAddress()).thenReturn(RECIPIENT);
    when(parentFrame.getState()).thenReturn(MessageFrame.State.COMPLETED_FAILED);

    tracer.traceContextEnter(parentFrame);
    assertThat(tracer.getLogs()).hasSize(1);

    tracer.traceContextExit(parentFrame);
    assertThat(tracer.getLogs()).isEmpty();
  }

  @Test
  void failedSubFrameDoesNotClearParentLogs() {
    // Parent CALL with value transfer
    when(parentFrame.getValue()).thenReturn(Wei.of(1000));
    when(parentFrame.getSenderAddress()).thenReturn(SENDER);
    when(parentFrame.getRecipientAddress()).thenReturn(RECIPIENT);

    tracer.traceContextEnter(parentFrame);
    assertThat(tracer.getLogs()).hasSize(1);

    // Child DELEGATECALL with zero value that fails
    when(childFrame.getValue()).thenReturn(Wei.ZERO);
    when(childFrame.getState()).thenReturn(MessageFrame.State.COMPLETED_FAILED);

    tracer.traceContextEnter(childFrame);
    assertThat(tracer.getLogs()).hasSize(1);

    tracer.traceContextExit(childFrame);

    // Parent log must still be present
    assertThat(tracer.getLogs()).hasSize(1);

    // Parent succeeds
    when(parentFrame.getState()).thenReturn(MessageFrame.State.COMPLETED_SUCCESS);
    tracer.traceContextExit(parentFrame);

    assertThat(tracer.getLogs()).hasSize(1);
  }

  @Test
  void failedSubFrameWithValueTransferRemovesOnlyItsLog() {
    // Parent CALL with value transfer
    when(parentFrame.getValue()).thenReturn(Wei.of(1000));
    when(parentFrame.getSenderAddress()).thenReturn(SENDER);
    when(parentFrame.getRecipientAddress()).thenReturn(RECIPIENT);

    tracer.traceContextEnter(parentFrame);
    assertThat(tracer.getLogs()).hasSize(1);

    // Child CALL with value transfer that fails
    when(childFrame.getValue()).thenReturn(Wei.of(500));
    when(childFrame.getSenderAddress()).thenReturn(RECIPIENT);
    when(childFrame.getRecipientAddress()).thenReturn(OTHER);
    when(childFrame.getState()).thenReturn(MessageFrame.State.COMPLETED_FAILED);

    tracer.traceContextEnter(childFrame);
    assertThat(tracer.getLogs()).hasSize(2);

    tracer.traceContextExit(childFrame);

    // Only parent log remains
    assertThat(tracer.getLogs()).hasSize(1);
  }
}
