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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.TopFrameHaltTransactions.Halt;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.tracing.EthTransferLogOperationTracer;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TopFrameHaltTest {

  private static final TopFrameHaltTransactions TRANSACTIONS = new TopFrameHaltTransactions();

  @ParameterizedTest
  @EnumSource(Halt.class)
  void haltedTopFrameIsEnteredAndExitedWithoutExecuting(final Halt halt) {
    final FrameEventTracer tracer = new FrameEventTracer();

    final TransactionProcessingResult result =
        TRANSACTIONS.process(TRANSACTIONS.transaction(halt), tracer);

    assertThat(result.isInvalid()).as(result.getValidationResult().toString()).isFalse();
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getExceptionalHaltReason()).contains(ExceptionalHaltReason.INSUFFICIENT_GAS);
    assertThat(tracer.events)
        .containsExactly("enter " + halt.frameType(), "exit COMPLETED_FAILED remainingGas=0");
  }

  @ParameterizedTest
  @EnumSource(Halt.class)
  void ethTransferLogTracerHandlesHaltedTopFrame(final Halt halt) {
    final EthTransferLogOperationTracer tracer = new EthTransferLogOperationTracer();

    final TransactionProcessingResult result =
        TRANSACTIONS.process(TRANSACTIONS.transaction(halt), tracer);

    assertThat(result.isInvalid()).as(result.getValidationResult().toString()).isFalse();
    assertThat(result.getExceptionalHaltReason()).contains(ExceptionalHaltReason.INSUFFICIENT_GAS);
    assertThat(tracer.getLogs()).isEmpty();
  }

  @Test
  void haltedValueTransferDoesNotCreateRecipient() {
    final WorldUpdater worldUpdater = TRANSACTIONS.newWorldUpdater();

    TRANSACTIONS.process(
        worldUpdater,
        TRANSACTIONS.transaction(Halt.VALUE_TO_EMPTY_RECIPIENT),
        OperationTracer.NO_TRACING);

    assertThat(worldUpdater.get(TopFrameHaltTransactions.EMPTY_ACCOUNT)).isNull();
  }

  @Test
  void haltedDelegationIsRolledBack() {
    final WorldUpdater worldUpdater = TRANSACTIONS.newWorldUpdater();
    final Transaction tx = TRANSACTIONS.transaction(Halt.DELEGATION_TO_NEW_AUTHORITY);

    final TransactionProcessingResult result =
        TRANSACTIONS.process(worldUpdater, tx, OperationTracer.NO_TRACING);

    assertThat(result.getExceptionalHaltReason()).contains(ExceptionalHaltReason.INSUFFICIENT_GAS);
    final Account authority = worldUpdater.get(TRANSACTIONS.authority());
    assertThat(authority == null || authority.getCode().isEmpty()).isTrue();
  }

  /** Records the frame events a tracer sees, with the frame's state on exit. */
  private static final class FrameEventTracer implements OperationTracer {
    private final List<String> events = new ArrayList<>();

    @Override
    public void traceContextEnter(final MessageFrame frame) {
      events.add("enter " + frame.getType());
    }

    @Override
    public void traceContextReEnter(final MessageFrame frame) {
      events.add("re-enter");
    }

    @Override
    public void tracePreExecution(final MessageFrame frame) {
      events.add("execute");
    }

    @Override
    public void traceContextExit(final MessageFrame frame) {
      events.add("exit " + frame.getState() + " remainingGas=" + frame.getRemainingGas());
    }
  }
}
