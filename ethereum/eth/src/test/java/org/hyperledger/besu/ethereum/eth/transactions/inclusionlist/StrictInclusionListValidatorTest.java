/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.eth.transactions.inclusionlist;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StrictInclusionListValidatorTest {

  private StrictInclusionListValidator validator;

  // Fake bytes for testing byte-level operations (present-in-payload checks, byte size limits)
  private static final Bytes TX_A = Bytes.fromHexString("0xaa");
  private static final Bytes TX_B = Bytes.fromHexString("0xbb");
  private static final Bytes TX_C = Bytes.fromHexString("0xcc");
  private static final Bytes TX_D = Bytes.fromHexString("0xdd");

  // Real encoded transactions for 3-step algorithm tests
  private static KeyPair senderKeys;
  private static Transaction realTx;
  private static Bytes realTxBytes;
  private static Address senderAddress;

  @BeforeAll
  static void setupRealTransactions() {
    senderKeys = SignatureAlgorithmFactory.getInstance().generateKeyPair();
    realTx =
        new TransactionTestFixture()
            .type(TransactionType.FRONTIER)
            .nonce(0)
            .gasLimit(21000)
            .gasPrice(Wei.of(1_000_000_000L))
            .value(Wei.ZERO)
            .createTransaction(senderKeys);
    realTxBytes = TransactionEncoder.encodeOpaqueBytes(realTx, EncodingContext.BLOCK_BODY);
    senderAddress = realTx.getSender();
  }

  @BeforeEach
  public void setUp() {
    validator = new StrictInclusionListValidator();
  }

  // ===== Existing tests using fake bytes (presence checks and byte limits) =====

  @Test
  public void validWhenEmptyInclusionList() {
    final InclusionListValidationResult result =
        validator.validate(List.of(TX_A, TX_B), Collections.emptyList());
    assertThat(result.isValid()).isTrue();
    assertThat(result.getStatus()).isEqualTo(InclusionListValidationResult.Status.VALID);
  }

  @Test
  public void validWhenNullInclusionList() {
    final InclusionListValidationResult result = validator.validate(List.of(TX_A, TX_B), null);
    assertThat(result.isValid()).isTrue();
  }

  @Test
  public void validWhenAllILTransactionsPresent() {
    // All IL txs are in the payload — step 1 skips all (no decoding needed)
    final InclusionListValidationResult result =
        validator.validate(List.of(TX_A, TX_B, TX_C), List.of(TX_A, TX_B, TX_C));
    assertThat(result.isValid()).isTrue();
  }

  @Test
  public void validWhenILTransactionsInterleavedInPayload() {
    // Payload has extra txs interleaved: D, A, D, B, D, C — all IL txs found via set lookup
    final InclusionListValidationResult result =
        validator.validate(List.of(TX_D, TX_A, TX_D, TX_B, TX_D, TX_C), List.of(TX_A, TX_B, TX_C));
    assertThat(result.isValid()).isTrue();
  }

  @Test
  public void validWhenILTransactionsInDifferentOrder() {
    // IL requires A, B but payload has B, A — per EIP-7805 "anywhere-in-block", order doesn't
    // matter
    final InclusionListValidationResult result =
        validator.validate(List.of(TX_B, TX_A), List.of(TX_A, TX_B));
    assertThat(result.isValid()).isTrue();
  }

  @Test
  public void invalidWhenILExceedsMaxBytes() {
    // Create an IL transaction that exceeds MAX_BYTES_PER_INCLUSION_LIST
    final Bytes largeTx =
        Bytes.wrap(new byte[InclusionListConfiguration.MAX_BYTES_PER_INCLUSION_LIST + 1]);
    final InclusionListValidationResult result =
        validator.validate(List.of(largeTx), List.of(largeTx));
    assertThat(result.getStatus()).isEqualTo(InclusionListValidationResult.Status.INVALID);
    assertThat(result.getErrorMessage()).isPresent();
    assertThat(result.getErrorMessage().get()).contains("MAX_BYTES_PER_INCLUSION_LIST");
  }

  @Test
  public void validWhenILExactlyAtMaxBytes() {
    // Create IL transactions that sum to exactly MAX_BYTES_PER_INCLUSION_LIST
    // These are present in the payload so step 1 skips them (no decoding needed)
    final Bytes exactTx =
        Bytes.wrap(new byte[InclusionListConfiguration.MAX_BYTES_PER_INCLUSION_LIST]);
    final InclusionListValidationResult result =
        validator.validate(List.of(exactTx), List.of(exactTx));
    assertThat(result.isValid()).isTrue();
  }

  @Test
  public void invalidWhenMultipleILTransactionsExceedMaxBytes() {
    // Two transactions that individually fit but together exceed the limit
    final int halfPlus = (InclusionListConfiguration.MAX_BYTES_PER_INCLUSION_LIST / 2) + 1;
    final Bytes tx1 = Bytes.wrap(new byte[halfPlus]);
    final Bytes tx2 = Bytes.wrap(new byte[halfPlus]);
    final InclusionListValidationResult result =
        validator.validate(List.of(tx1, tx2), List.of(tx1, tx2));
    assertThat(result.getStatus()).isEqualTo(InclusionListValidationResult.Status.INVALID);
  }

  @Test
  public void invalidWhenILContainsUndecodableTransaction() {
    // Fake bytes (not a valid transaction encoding) that are NOT in the payload
    final InclusionListValidationResult result =
        validator.validate(
            Collections.emptyList(), List.of(TX_A), InclusionListValidationContext.unlimited());
    assertThat(result.getStatus()).isEqualTo(InclusionListValidationResult.Status.INVALID);
    assertThat(result.getErrorMessage()).isPresent();
    assertThat(result.getErrorMessage().get()).contains("undecodable transaction");
  }

  // ===== New 3-step algorithm tests using real encoded transactions =====

  /**
   * A transaction that is NOT in the payload but passes all 3 steps (gas fits, nonce/balance valid)
   * must result in UNSATISFIED.
   */
  @Test
  public void unsatisfiedWhenMissingValidTransaction() {
    // Set up context: sender has correct nonce (0) and sufficient balance
    final Wei sufficientBalance = realTx.getUpfrontCost(0).add(Wei.of(1));
    final InclusionListValidationContext context =
        new InclusionListValidationContext(
            Long.MAX_VALUE, // plenty of gas
            Map.of(senderAddress, 0L), // correct nonce
            Map.of(senderAddress, sufficientBalance));

    // realTx is NOT in the payload
    final InclusionListValidationResult result =
        validator.validate(Collections.emptyList(), List.of(realTxBytes), context);

    assertThat(result.getStatus()).isEqualTo(InclusionListValidationResult.Status.UNSATISFIED);
    assertThat(result.getErrorMessage()).isPresent();
  }

  /**
   * A transaction that is NOT in the payload but has gasLimit exceeding gas_left should be skipped
   * (step 2). The overall result should be VALID.
   */
  @Test
  public void validWhenMissingTxExceedsGasLeft() {
    // gasLeft is less than the transaction's gasLimit (21000)
    final long insufficientGasLeft = realTx.getGasLimit() - 1;
    final Wei sufficientBalance = realTx.getUpfrontCost(0).add(Wei.of(1));
    final InclusionListValidationContext context =
        new InclusionListValidationContext(
            insufficientGasLeft,
            Map.of(senderAddress, 0L),
            Map.of(senderAddress, sufficientBalance));

    // realTx is NOT in the payload, but gasLimit exceeds gasLeft → step 2 skips it
    final InclusionListValidationResult result =
        validator.validate(Collections.emptyList(), List.of(realTxBytes), context);

    assertThat(result.isValid()).isTrue();
    assertThat(result.getStatus()).isEqualTo(InclusionListValidationResult.Status.VALID);
  }

  /**
   * A transaction that is NOT in the payload but has a wrong nonce should be skipped (step 3 nonce
   * check). The overall result should be VALID.
   */
  @Test
  public void validWhenMissingTxHasInvalidNonce() {
    // Context says sender has nonce 5, but the transaction has nonce 0 → mismatch
    final Wei sufficientBalance = realTx.getUpfrontCost(0).add(Wei.of(1));
    final InclusionListValidationContext context =
        new InclusionListValidationContext(
            Long.MAX_VALUE,
            Map.of(senderAddress, 5L), // expected nonce is 5, tx.nonce is 0
            Map.of(senderAddress, sufficientBalance));

    final InclusionListValidationResult result =
        validator.validate(Collections.emptyList(), List.of(realTxBytes), context);

    assertThat(result.isValid()).isTrue();
    assertThat(result.getStatus()).isEqualTo(InclusionListValidationResult.Status.VALID);
  }

  /**
   * A transaction that is NOT in the payload but the sender has insufficient balance should be
   * skipped (step 3 balance check). The overall result should be VALID.
   */
  @Test
  public void validWhenMissingTxHasInsufficientBalance() {
    // Context says sender has zero balance → cannot pay upfront cost
    final InclusionListValidationContext context =
        new InclusionListValidationContext(
            Long.MAX_VALUE,
            Map.of(senderAddress, 0L), // correct nonce
            Map.of(senderAddress, Wei.ZERO)); // insufficient balance

    final InclusionListValidationResult result =
        validator.validate(Collections.emptyList(), List.of(realTxBytes), context);

    assertThat(result.isValid()).isTrue();
    assertThat(result.getStatus()).isEqualTo(InclusionListValidationResult.Status.VALID);
  }

  /**
   * A transaction that IS present in the payload should be skipped by step 1, even with an
   * unfavorable context (insufficient balance). The overall result should be VALID.
   */
  @Test
  public void validWhenILTransactionPresentInPayload() {
    // Even though context has zero balance, the tx is in the payload — step 1 skips it
    final InclusionListValidationContext context =
        new InclusionListValidationContext(0L, Map.of(), Map.of());

    final InclusionListValidationResult result =
        validator.validate(List.of(realTxBytes), List.of(realTxBytes), context);

    assertThat(result.isValid()).isTrue();
  }
}
