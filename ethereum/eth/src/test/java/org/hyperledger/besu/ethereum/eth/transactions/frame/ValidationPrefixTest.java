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
package org.hyperledger.besu.ethereum.eth.transactions.frame;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionValidator;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class ValidationPrefixTest {

  static final Address SENDER = Address.fromHexString("0x00000000000000000000000000000000000000aa");
  static final Address PAYMASTER =
      Address.fromHexString("0x00000000000000000000000000000000000000bb");

  static Frame verify(final int flags, final Address target) {
    return new Frame(
        Frame.MODE_VERIFY, flags, Optional.ofNullable(target), 30_000L, 0L, Wei.ZERO, Bytes.EMPTY);
  }

  static Frame selfVerify() {
    return verify(Frame.APPROVE_SCOPE_MASK, null);
  }

  static Frame deploy() {
    return new Frame(
        Frame.MODE_DEFAULT, 0, Optional.of(PAYMASTER), 30_000L, 100_000L, Wei.ZERO, Bytes.EMPTY);
  }

  static Frame expiry(final long deadline) {
    return new Frame(
        Frame.MODE_VERIFY,
        0,
        Optional.of(MainnetTransactionValidator.EXPIRY_VERIFIER),
        5_000L,
        0L,
        Wei.ZERO,
        Bytes.ofUnsignedLong(deadline));
  }

  static Frame userOp() {
    return new Frame(
        Frame.MODE_SENDER, 0, Optional.of(PAYMASTER), 50_000L, 0L, Wei.ZERO, Bytes.EMPTY);
  }

  static Transaction tx(final Frame... frames) {
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(BigInteger.ONE)
        .nonce(0)
        .sender(SENDER)
        .frames(List.of(frames))
        .frameSignatures(List.of())
        .maxPriorityFeePerGas(Wei.of(1))
        .maxFeePerGas(Wei.of(10))
        .maxFeePerBlobGas(Wei.ZERO)
        .build();
  }

  @Test
  void recognizesSelfRelay() throws Exception {
    final ValidationPrefix prefix = ValidationPrefix.classify(tx(selfVerify(), userOp()));
    assertThat(prefix.length()).isEqualTo(1);
    assertThat(prefix.payFrame()).isZero();
    assertThat(prefix.paymaster()).isFalse();
    assertThat(prefix.hasDeploy()).isFalse();
  }

  @Test
  void recognizesDeployThenSelfVerify() throws Exception {
    final ValidationPrefix prefix = ValidationPrefix.classify(tx(deploy(), selfVerify()));
    assertThat(prefix.deployFrame()).isZero();
    assertThat(prefix.payFrame()).isEqualTo(1);
  }

  @Test
  void recognizesPaymasterShapesBehindAnExpiryVerifier() throws Exception {
    final ValidationPrefix prefix =
        ValidationPrefix.classify(
            tx(
                expiry(5_000L),
                deploy(),
                verify(Frame.FLAG_APPROVE_EXECUTION, SENDER),
                verify(Frame.FLAG_APPROVE_PAYMENT, PAYMASTER),
                userOp()));
    assertThat(prefix.expiryDeadline()).hasValue(5_000L);
    assertThat(prefix.deployFrame()).isEqualTo(1);
    assertThat(prefix.payFrame()).isEqualTo(3);
    assertThat(prefix.length()).isEqualTo(4);
    assertThat(prefix.paymaster()).isTrue();
  }

  @Test
  void rejectsUnrecognizedShapes() {
    assertThatThrownBy(() -> ValidationPrefix.classify(tx(userOp(), selfVerify())))
        .hasMessageContaining("no recognized shape");
    // only_verify must target the sender.
    assertThatThrownBy(
            () ->
                ValidationPrefix.classify(
                    tx(
                        verify(Frame.FLAG_APPROVE_EXECUTION, PAYMASTER),
                        verify(Frame.FLAG_APPROVE_PAYMENT, PAYMASTER))))
        .hasMessageContaining("no recognized shape");
  }

  @Test
  void rejectsVerifyFramesAfterThePrefix() {
    assertThatThrownBy(() -> ValidationPrefix.classify(tx(selfVerify(), userOp(), selfVerify())))
        .hasMessageContaining("after the validation prefix");
  }

  @Test
  void rejectsAtomicBatchesInThePrefix() {
    final Frame batched =
        new Frame(
            Frame.MODE_DEFAULT,
            Frame.ATOMIC_BATCH_FLAG,
            Optional.of(PAYMASTER),
            30_000L,
            0L,
            Wei.ZERO,
            Bytes.EMPTY);
    assertThatThrownBy(() -> ValidationPrefix.classify(tx(batched, selfVerify())))
        .isInstanceOf(ValidationPrefix.UnrecognizedPrefixException.class);
  }

  @Test
  void rejectsPrefixesOverTheVerifyGasBudget() {
    final Frame heavy =
        new Frame(
            Frame.MODE_VERIFY,
            Frame.APPROVE_SCOPE_MASK,
            Optional.empty(),
            ValidationPrefix.MAX_VERIFY_GAS + 1,
            0L,
            Wei.ZERO,
            Bytes.EMPTY);
    assertThatThrownBy(() -> ValidationPrefix.classify(tx(heavy)))
        .hasMessageContaining("validation work");
  }

  static Frame postTx(final long executionGas, final long stateGas) {
    return new Frame(
        Frame.MODE_POST_TX,
        0,
        Optional.of(PAYMASTER),
        executionGas,
        stateGas,
        Wei.ZERO,
        Bytes.EMPTY);
  }

  @Test
  void acceptsTrailingPostTxFramesOutsideThePrefixAndItsBudgets() throws Exception {
    // EIP-7906 assertions follow the prefix. Their budgets are far above MAX_VERIFY_GAS and
    // MAX_VERIFY_STATE_GAS, and count toward neither (geth ClassifyPrefix, i <= prefix.End).
    final Frame heavyPostTx =
        postTx(ValidationPrefix.MAX_VERIFY_GAS * 100, ValidationPrefix.MAX_VERIFY_STATE_GAS * 10);

    final ValidationPrefix selfRelay =
        ValidationPrefix.classify(tx(selfVerify(), userOp(), heavyPostTx, heavyPostTx));
    assertThat(selfRelay.length()).isEqualTo(1);
    assertThat(selfRelay.payFrame()).isZero();

    final ValidationPrefix paymaster =
        ValidationPrefix.classify(
            tx(
                verify(Frame.FLAG_APPROVE_EXECUTION, SENDER),
                verify(Frame.FLAG_APPROVE_PAYMENT, PAYMASTER),
                userOp(),
                heavyPostTx));
    assertThat(paymaster.length()).isEqualTo(2);
    assertThat(paymaster.payFrame()).isEqualTo(1);
    assertThat(paymaster.paymaster()).isTrue();

    // Nothing but the prefix is required: the assertion may follow it directly.
    assertThat(ValidationPrefix.classify(tx(selfVerify(), heavyPostTx)).length()).isEqualTo(1);
  }

  @Test
  void rejectsAPostTxFrameInsideThePrefix() {
    assertThatThrownBy(() -> ValidationPrefix.classify(tx(postTx(10_000L, 0L), selfVerify())))
        .hasMessageContaining("no recognized shape");
    assertThatThrownBy(
            () ->
                ValidationPrefix.classify(
                    tx(
                        verify(Frame.FLAG_APPROVE_EXECUTION, SENDER),
                        postTx(10_000L, 0L),
                        verify(Frame.FLAG_APPROVE_PAYMENT, PAYMASTER))))
        .hasMessageContaining("no recognized shape");
  }
}
