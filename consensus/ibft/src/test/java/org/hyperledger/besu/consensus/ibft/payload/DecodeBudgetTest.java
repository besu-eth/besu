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
package org.hyperledger.besu.consensus.ibft.payload;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.rlp.RLPException;

import org.junit.jupiter.api.Test;

public class DecodeBudgetTest {

  @Test
  public void permitsExactlyMaxCharges() {
    final DecodeBudget budget = new DecodeBudget(3);
    assertThatCode(
            () -> {
              budget.chargeSignedPayload();
              budget.chargeSignedPayload();
              budget.chargeSignedPayload();
            })
        .doesNotThrowAnyException();
  }

  @Test
  public void throwsWhenChargedBeyondMax() {
    final DecodeBudget budget = new DecodeBudget(2);
    budget.chargeSignedPayload();
    budget.chargeSignedPayload();

    assertThatThrownBy(budget::chargeSignedPayload)
        .isInstanceOf(RLPException.class)
        .hasMessageContaining("exceed the maximum permitted total");
  }

  @Test
  public void zeroBudgetRejectsFirstCharge() {
    final DecodeBudget budget = new DecodeBudget(0);
    assertThatThrownBy(budget::chargeSignedPayload).isInstanceOf(RLPException.class);
  }

  @Test
  public void forIbftMessagePermitsExactlyTheHonestMaximum() {
    // A valid IBFT2 proposal for V validators holds up to (V + 1)^2 signed payloads, and no more.
    final int validators = 30;
    final int honestMaximum = (validators + 1) * (validators + 1); // 961
    assertThatCode(() -> chargeN(DecodeBudget.forIbftMessage(validators), honestMaximum))
        .doesNotThrowAnyException();
    assertThatThrownBy(() -> chargeN(DecodeBudget.forIbftMessage(validators), honestMaximum + 1))
        .isInstanceOf(RLPException.class);
  }

  @Test
  public void forIbftMessageRejectsAFloodForASmallValidatorSet() {
    // forIbftMessage(4) permits (4 + 1)^2 = 25 payloads; a flood of far more must be rejected.
    assertThatThrownBy(() -> chargeN(DecodeBudget.forIbftMessage(4), 100_000))
        .isInstanceOf(RLPException.class);
  }

  private static void chargeN(final DecodeBudget decodeBudget, final int n) {
    for (int i = 0; i < n; i++) {
      decodeBudget.chargeSignedPayload();
    }
  }
}
