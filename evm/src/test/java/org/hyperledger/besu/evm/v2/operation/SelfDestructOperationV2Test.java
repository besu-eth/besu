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
package org.hyperledger.besu.evm.v2.operation;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.gascalculator.ConstantinopleGasCalculator;
import org.hyperledger.besu.evm.log.EIP7708TransferLogEmitter;
import org.hyperledger.besu.evm.log.TransferLogEmitter;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class SelfDestructOperationV2Test {

  private static final Address ORIGINATOR = Address.fromHexString("0xc0de");
  private static final Address BENEFICIARY = Address.fromHexString("0xbeef");

  private final WorldUpdater worldUpdater = new ToyWorld().updater();

  @Test
  void destroysTheAccountAndSendsItsBalanceBeforeCancun() {
    final MessageFrame frame = frame(false);

    final Operation.OperationResult result =
        new SelfDestructOperationV2(
                new ConstantinopleGasCalculator(), false, TransferLogEmitter.NOOP)
            .execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.getState()).isEqualTo(MessageFrame.State.CODE_SUCCESS);
    assertThat(frame.stackTopV2()).isZero();
    assertThat(worldUpdater.get(BENEFICIARY).getBalance()).isEqualTo(Wei.of(1_000));
    assertThat(worldUpdater.get(ORIGINATOR).getBalance()).isEqualTo(Wei.ZERO);
    assertThat(frame.getSelfDestructs()).containsExactly(ORIGINATOR);
    assertThat(frame.getRefunds()).containsEntry(BENEFICIARY, Wei.of(1_000));
    // 5000 for the operation and 25000 for sending value to a new account
    assertThat(result.getGasCost()).isEqualTo(30_000L);
  }

  @Test
  void onlySendsTheBalanceOfAnAccountNotCreatedInTheTransactionFromCancun() {
    final MessageFrame frame = frame(false);

    final Operation.OperationResult result =
        new SelfDestructOperationV2(new CancunGasCalculator(), true, TransferLogEmitter.NOOP)
            .execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(worldUpdater.get(BENEFICIARY).getBalance()).isEqualTo(Wei.of(1_000));
    assertThat(worldUpdater.get(ORIGINATOR).getBalance()).isEqualTo(Wei.ZERO);
    assertThat(frame.getSelfDestructs()).isEmpty();
  }

  @Test
  void destroysAnAccountCreatedInTheTransactionFromCancun() {
    final MessageFrame frame = frame(false);
    frame.addCreate(ORIGINATOR);

    new SelfDestructOperationV2(new CancunGasCalculator(), true, TransferLogEmitter.NOOP)
        .execute(frame, null);

    assertThat(frame.getSelfDestructs()).containsExactly(ORIGINATOR);
  }

  @Test
  void logsTheTransferOnAmsterdam() {
    worldUpdater.getOrCreate(BENEFICIARY).setBalance(Wei.ONE);
    final MessageFrame frame = frame(false);

    final Operation.OperationResult result =
        new SelfDestructOperationV2(
                new AmsterdamGasCalculator(), true, EIP7708TransferLogEmitter.INSTANCE)
            .execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.getLogs()).hasSize(1);
    assertThat(worldUpdater.get(BENEFICIARY).getBalance()).isEqualTo(Wei.of(1_001));
  }

  @Test
  void haltsInAStaticFrame() {
    final MessageFrame frame = frame(true);

    final Operation.OperationResult result =
        new SelfDestructOperationV2(new CancunGasCalculator(), true, TransferLogEmitter.NOOP)
            .execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    assertThat(worldUpdater.get(ORIGINATOR).getBalance()).isEqualTo(Wei.of(1_000));
  }

  @Test
  void haltsOnStackUnderflow() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2().address(ORIGINATOR).worldUpdater(worldUpdater).build();

    final Operation.OperationResult result =
        new SelfDestructOperationV2(new CancunGasCalculator(), true, TransferLogEmitter.NOOP)
            .execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  private MessageFrame frame(final boolean isStatic) {
    worldUpdater.getOrCreate(ORIGINATOR).setBalance(Wei.of(1_000));
    return new TestMessageFrameBuilderV2()
        .address(ORIGINATOR)
        .contract(ORIGINATOR)
        .worldUpdater(worldUpdater)
        .initialGas(100_000L)
        .isStatic(isStatic)
        .pushStackItem(Bytes32.leftPad(BENEFICIARY.getBytes()))
        .build();
  }
}
