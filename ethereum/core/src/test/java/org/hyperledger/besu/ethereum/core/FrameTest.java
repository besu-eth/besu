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
package org.hyperledger.besu.ethereum.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

/** The helpers of the EIP-8141 {@link Frame} and {@link FrameSignature} records. */
class FrameTest {

  private static final Address SENDER =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address TARGET =
      Address.fromHexString("0x2222222222222222222222222222222222222222");

  private static Frame frameWithFlags(final int flags, final Optional<Address> target) {
    return new Frame(Frame.MODE_DEFAULT, flags, target, 0L, 0L, Wei.ZERO, Bytes.EMPTY);
  }

  @Test
  void allowedApprovalScopeMasksTheApprovalBits() {
    assertThat(frameWithFlags(0, Optional.empty()).allowedApprovalScope()).isZero();
    assertThat(frameWithFlags(Frame.FLAG_APPROVE_PAYMENT, Optional.empty()).allowedApprovalScope())
        .isEqualTo(Frame.FLAG_APPROVE_PAYMENT);
    assertThat(
            frameWithFlags(Frame.FLAG_APPROVE_EXECUTION, Optional.empty()).allowedApprovalScope())
        .isEqualTo(Frame.FLAG_APPROVE_EXECUTION);
    assertThat(
            frameWithFlags(Frame.APPROVE_SCOPE_MASK | Frame.ATOMIC_BATCH_FLAG, Optional.empty())
                .allowedApprovalScope())
        .isEqualTo(Frame.APPROVE_SCOPE_MASK);
  }

  @Test
  void isAtomicBatchReadsTheBatchFlag() {
    assertThat(frameWithFlags(0, Optional.empty()).isAtomicBatch()).isFalse();
    assertThat(frameWithFlags(Frame.APPROVE_SCOPE_MASK, Optional.empty()).isAtomicBatch())
        .isFalse();
    assertThat(frameWithFlags(Frame.ATOMIC_BATCH_FLAG, Optional.empty()).isAtomicBatch()).isTrue();
  }

  @Test
  void resolvedTargetDefaultsToTheSender() {
    assertThat(frameWithFlags(0, Optional.empty()).resolvedTarget(SENDER)).isEqualTo(SENDER);
    assertThat(frameWithFlags(0, Optional.of(TARGET)).resolvedTarget(SENDER)).isEqualTo(TARGET);
  }

  @Test
  void signsCanonicalHashWhenTheMessageIsEmpty() {
    assertThat(
            new FrameSignature(
                    FrameSignature.SCHEME_SECP256K1, Bytes.EMPTY, Bytes.EMPTY, Bytes.EMPTY)
                .signsCanonicalHash())
        .isTrue();
    assertThat(
            new FrameSignature(
                    FrameSignature.SCHEME_SECP256K1,
                    Bytes.EMPTY,
                    Bytes32.leftPad(Bytes.of(1)),
                    Bytes.EMPTY)
                .signsCanonicalHash())
        .isFalse();
  }

  @Test
  void resolvedSignerDefaultsToTheSender() {
    assertThat(
            new FrameSignature(FrameSignature.SCHEME_P256, Bytes.EMPTY, Bytes.EMPTY, Bytes.EMPTY)
                .resolvedSigner(SENDER))
        .isEqualTo(SENDER);
    assertThat(
            new FrameSignature(
                    FrameSignature.SCHEME_P256, TARGET.getBytes(), Bytes.EMPTY, Bytes.EMPTY)
                .resolvedSigner(SENDER))
        .isEqualTo(TARGET);
  }

  @Test
  void recordsCompareByValue() {
    final Frame a =
        new Frame(
            Frame.MODE_SENDER,
            0,
            Optional.of(TARGET),
            1L,
            2L,
            Wei.ONE,
            Bytes.fromHexString("0x01"));
    final Frame b =
        new Frame(
            Frame.MODE_SENDER,
            0,
            Optional.of(Address.wrap(TARGET.getBytes().copy())),
            1L,
            2L,
            Wei.of(1),
            Bytes.of(1));
    assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
    assertThat(
            new FrameSignature(
                FrameSignature.SCHEME_ARBITRARY, Bytes.EMPTY, Bytes.EMPTY, Bytes.of(9)))
        .isEqualTo(
            new FrameSignature(
                FrameSignature.SCHEME_ARBITRARY,
                Bytes.EMPTY,
                Bytes.EMPTY,
                Bytes.fromHexString("0x09")));
  }
}
