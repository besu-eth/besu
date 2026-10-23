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
package org.hyperledger.besu.evm.frame;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.collections.undo.UndoSet;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.FrameTransactionContext.FrameInfo;
import org.hyperledger.besu.evm.frame.FrameTransactionContext.SignatureInfo;
import org.hyperledger.besu.evm.toy.ToyBlockValues;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class FrameTransactionContextTest {

  private static final Address SENDER = Address.fromHexString("0x5e4d");
  private static final Address PAYMASTER = Address.fromHexString("0x9a7e");
  private static final Address ORIGINATOR = Address.fromHexString("0x0419");
  private static final Bytes32 SIGNATURE_HASH = Bytes32.fromHexStringLenient("0x5167");
  private static final Bytes32 SLOT = Bytes32.fromHexStringLenient("0x01");
  private static final Bytes32 OTHER_SLOT = Bytes32.fromHexStringLenient("0x02");

  private static final FrameInfo VERIFY_FRAME =
      new FrameInfo(
          FrameTransactionContext.MODE_VERIFY,
          FrameTransactionContext.APPROVE_SCOPE_MASK,
          SENDER,
          50_000L,
          10_000L,
          Wei.ZERO,
          Bytes.fromHexString("0x01"));
  private static final FrameInfo SENDER_FRAME =
      new FrameInfo(
          FrameTransactionContext.MODE_SENDER,
          0,
          PAYMASTER,
          70_000L,
          20_000L,
          Wei.of(3),
          Bytes.fromHexString("0x0203"));

  private static final SignatureInfo SIGNATURE =
      new SignatureInfo(FrameTransactionContext.SCHEME_SECP256K1, SENDER, null, null);

  private static FrameTransactionContext context() {
    return new FrameTransactionContext(
        List.of(VERIFY_FRAME, SENDER_FRAME),
        List.of(SIGNATURE),
        SENDER,
        SIGNATURE_HASH,
        7L,
        List.of(UInt256.valueOf(5)),
        3L,
        Wei.of(2),
        Wei.of(11),
        Wei.of(13),
        Wei.of(1_000));
  }

  private static TxValues txValues(final FrameTransactionContext context) {
    final TxValues txValues =
        TxValues.forTransaction(
            (__, ___) -> Hash.ZERO,
            1024,
            UndoSet.of(new HashSet<>()),
            ORIGINATOR,
            Wei.ONE,
            Wei.ONE,
            new ToyBlockValues(),
            Address.ZERO,
            Optional.empty(),
            0L);
    if (context != null) {
      txValues.setFrameTransactionContext(context);
    }
    return txValues;
  }

  @Test
  void exposesTheTransactionInputs() {
    final FrameTransactionContext context = context();

    assertThat(context.frames()).containsExactly(VERIFY_FRAME, SENDER_FRAME);
    assertThat(context.signatures()).containsExactly(SIGNATURE);
    assertThat(context.sender()).isEqualTo(SENDER);
    assertThat(context.signatureHash()).isEqualTo(SIGNATURE_HASH);
    assertThat(context.nonce()).isEqualTo(7L);
    assertThat(context.nonceKeys()).containsExactly(UInt256.valueOf(5));
    assertThat(context.legacyNonce()).isEqualTo(3L);
    assertThat(context.maxPriorityFeePerGas()).isEqualTo(Wei.of(2));
    assertThat(context.maxFeePerGas()).isEqualTo(Wei.of(11));
    assertThat(context.maxFeePerBlobGas()).isEqualTo(Wei.of(13));
    assertThat(context.maxCost()).isEqualTo(Wei.of(1_000));
  }

  @Test
  void startsUnapprovedAtTheFirstFrame() {
    final FrameTransactionContext context = context();

    assertThat(context.currentFrameIndex()).isZero();
    assertThat(context.currentFrame()).isEqualTo(VERIFY_FRAME);
    assertThat(context.payer()).isNull();
    assertThat(context.senderApproved()).isFalse();
    for (int i = 0; i < 2; i++) {
      assertThat(context.frameStatus(i)).isEqualTo(FrameTransactionContext.STATUS_FAILURE);
      assertThat(context.frameExecutionGasUsed(i)).isZero();
      assertThat(context.frameStateGasUsed(i)).isZero();
    }
  }

  @Test
  void currentFrameFollowsTheFrameIndex() {
    final FrameTransactionContext context = context();

    context.setCurrentFrameIndex(1);

    assertThat(context.currentFrameIndex()).isEqualTo(1);
    assertThat(context.currentFrame()).isEqualTo(SENDER_FRAME);
  }

  @Test
  void recordsCompletedAndSkippedFrames() {
    final FrameTransactionContext context = context();

    context.recordFrameCompletion(0, FrameTransactionContext.STATUS_SUCCESS, 21_000L, 4_000L);
    context.recordFrameCompletion(1, FrameTransactionContext.STATUS_SUCCESS, 9_000L, 1_000L);
    context.recordFrameSkipped(1);

    assertThat(context.frameStatus(0)).isEqualTo(FrameTransactionContext.STATUS_SUCCESS);
    assertThat(context.frameExecutionGasUsed(0)).isEqualTo(21_000L);
    assertThat(context.frameStateGasUsed(0)).isEqualTo(4_000L);
    assertThat(context.frameStatus(1)).isEqualTo(FrameTransactionContext.STATUS_SKIPPED);
    assertThat(context.frameExecutionGasUsed(1)).isZero();
    assertThat(context.frameStateGasUsed(1)).isZero();
  }

  @Test
  void reducesTheOwningFramesStateGasReceipt() {
    final FrameTransactionContext context = context();
    context.recordFrameCompletion(0, FrameTransactionContext.STATUS_SUCCESS, 21_000L, 4_000L);

    context.reduceFrameStateGasUsed(0, 1_500L);

    assertThat(context.frameStateGasUsed(0)).isEqualTo(2_500L);
    assertThat(context.frameStateGasUsed(1)).isZero();
  }

  @Test
  void slotChargesBelongToTheFrameThatMadeThem() {
    final FrameTransactionContext context = context();
    context.recordSlotCharge(SENDER, SLOT);
    context.setCurrentFrameIndex(1);
    context.recordSlotCharge(SENDER, OTHER_SLOT);

    assertThat(context.removeSlotCharge(SENDER, SLOT)).isZero();
    assertThat(context.removeSlotCharge(SENDER, OTHER_SLOT)).isEqualTo(1);
    // removing consumes the outstanding charge
    assertThat(context.removeSlotCharge(SENDER, SLOT)).isNull();
    assertThat(context.removeSlotCharge(PAYMASTER, SLOT)).isNull();
  }

  @Test
  void txValuesUndoRollsTheApprovalContextBack() {
    final FrameTransactionContext context = context();
    final TxValues txValues = txValues(context);
    context.recordFrameCompletion(0, FrameTransactionContext.STATUS_SUCCESS, 21_000L, 4_000L);
    context.recordSlotCharge(SENDER, SLOT);
    final long mark = txValues.transientStorage().mark();

    context.setCurrentFrameIndex(1);
    context.approveSender();
    context.setPayer(PAYMASTER);
    context.recordSlotCharge(PAYMASTER, OTHER_SLOT);
    assertThat(context.removeSlotCharge(SENDER, SLOT)).isZero();
    context.reduceFrameStateGasUsed(0, 4_000L);
    context.recordFrameCompletion(1, FrameTransactionContext.STATUS_SUCCESS, 9_000L, 2_000L);

    txValues.undoChanges(mark);

    assertThat(context.senderApproved()).isFalse();
    assertThat(context.payer()).isNull();
    assertThat(context.frameStateGasUsed(0)).isEqualTo(4_000L);
    assertThat(context.frameStateGasUsed(1)).isZero();
    // the removed charge is outstanding again and the later one is gone
    assertThat(context.removeSlotCharge(PAYMASTER, OTHER_SLOT)).isNull();
    assertThat(context.removeSlotCharge(SENDER, SLOT)).isZero();
    // statuses and execution gas are final once recorded, so they are not journaled
    assertThat(context.frameStatus(1)).isEqualTo(FrameTransactionContext.STATUS_SUCCESS);
    assertThat(context.frameExecutionGasUsed(1)).isEqualTo(9_000L);
    // the frame index is driven by the processor, not by the journal
    assertThat(context.currentFrameIndex()).isEqualTo(1);
  }

  @Test
  void txValuesUndoKeepsChangesMadeBeforeTheMark() {
    final FrameTransactionContext context = context();
    final TxValues txValues = txValues(context);
    context.approveSender();
    context.setPayer(SENDER);
    final long mark = txValues.transientStorage().mark();

    txValues.undoChanges(mark);

    assertThat(context.senderApproved()).isTrue();
    assertThat(context.payer()).isEqualTo(SENDER);
  }

  @Test
  void txValuesCarryTheContextOnlyOnceAttached() {
    final TxValues plain = txValues(null);
    final long mark = plain.transientStorage().mark();
    plain.stateGasUsed().set(5L);

    plain.undoChanges(mark);

    assertThat(plain.frameTransactionContext()).isEmpty();
    assertThat(plain.stateGasUsed().get()).isZero();

    final FrameTransactionContext context = context();
    assertThat(txValues(context).frameTransactionContext()).containsSame(context);
  }

  @Test
  void originatorCanBeRebound() {
    final TxValues txValues = txValues(context());

    txValues.setOriginator(SENDER);

    assertThat(txValues.originator()).isEqualTo(SENDER);
  }
}
