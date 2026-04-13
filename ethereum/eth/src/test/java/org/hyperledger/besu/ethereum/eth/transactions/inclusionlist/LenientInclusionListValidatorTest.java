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

import java.util.Collections;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LenientInclusionListValidatorTest {

  private LenientInclusionListValidator validator;

  private static final Bytes TX_A = Bytes.fromHexString("0xaa");
  private static final Bytes TX_B = Bytes.fromHexString("0xbb");
  private static final Bytes TX_C = Bytes.fromHexString("0xcc");
  private static final Bytes TX_D = Bytes.fromHexString("0xdd");

  @BeforeEach
  public void setUp() {
    validator = new LenientInclusionListValidator();
  }

  @Test
  public void validWhenEmptyInclusionList() {
    final InclusionListValidationResult result =
        validator.validate(List.of(TX_A, TX_B), Collections.emptyList());
    assertThat(result.isValid()).isTrue();
    assertThat(validator.getViolationCount()).isZero();
  }

  @Test
  public void validWhenNullInclusionList() {
    final InclusionListValidationResult result = validator.validate(List.of(TX_A, TX_B), null);
    assertThat(result.isValid()).isTrue();
    assertThat(validator.getViolationCount()).isZero();
  }

  @Test
  public void validWhenAllILTransactionsPresent() {
    final InclusionListValidationResult result =
        validator.validate(List.of(TX_A, TX_B, TX_C), List.of(TX_A, TX_B, TX_C));
    assertThat(result.isValid()).isTrue();
    assertThat(validator.getViolationCount()).isZero();
  }

  @Test
  public void validWhenILTransactionsInterleavedInPayload() {
    final InclusionListValidationResult result =
        validator.validate(List.of(TX_D, TX_A, TX_D, TX_B, TX_D, TX_C), List.of(TX_A, TX_B, TX_C));
    assertThat(result.isValid()).isTrue();
    assertThat(validator.getViolationCount()).isZero();
  }

  @Test
  public void validEvenWhenILTransactionMissing() {
    final InclusionListValidationResult result =
        validator.validate(List.of(TX_A, TX_C), List.of(TX_A, TX_B, TX_C));
    assertThat(result.isValid()).isTrue();
    assertThat(result.getStatus()).isEqualTo(InclusionListValidationResult.Status.VALID);
    assertThat(validator.getViolationCount()).isEqualTo(1);
  }

  @Test
  public void validWhenILTransactionsInDifferentOrder() {
    // Per EIP-7805 "anywhere-in-block", order doesn't matter — no violation
    final InclusionListValidationResult result =
        validator.validate(List.of(TX_B, TX_A), List.of(TX_A, TX_B));
    assertThat(result.isValid()).isTrue();
    assertThat(validator.getViolationCount()).isZero();
  }

  @Test
  public void validEvenWhenPayloadEmpty() {
    final InclusionListValidationResult result =
        validator.validate(Collections.emptyList(), List.of(TX_A));
    assertThat(result.isValid()).isTrue();
    assertThat(validator.getViolationCount()).isEqualTo(1);
  }

  @Test
  public void validEvenWhenILExceedsMaxBytes() {
    final Bytes largeTx =
        Bytes.wrap(new byte[InclusionListConfiguration.MAX_BYTES_PER_INCLUSION_LIST + 1]);
    final InclusionListValidationResult result =
        validator.validate(List.of(largeTx), List.of(largeTx));
    assertThat(result.isValid()).isTrue();
    assertThat(validator.getViolationCount()).isEqualTo(1);
  }

  @Test
  public void violationCountAccumulatesAcrossMultipleValidations() {
    // First violation: missing tx
    validator.validate(List.of(TX_A), List.of(TX_A, TX_B));
    assertThat(validator.getViolationCount()).isEqualTo(1);

    // Second violation: byte limit exceeded
    final Bytes largeTx =
        Bytes.wrap(new byte[InclusionListConfiguration.MAX_BYTES_PER_INCLUSION_LIST + 1]);
    validator.validate(List.of(largeTx), List.of(largeTx));
    assertThat(validator.getViolationCount()).isEqualTo(2);

    // No violation: valid case
    validator.validate(List.of(TX_A, TX_B), List.of(TX_A, TX_B));
    assertThat(validator.getViolationCount()).isEqualTo(2);
  }
}
