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

import org.junit.jupiter.api.Test;

public class InclusionListValidationResultTest {

  @Test
  public void validResultHasValidStatus() {
    final InclusionListValidationResult result = InclusionListValidationResult.valid();
    assertThat(result.getStatus()).isEqualTo(InclusionListValidationResult.Status.VALID);
    assertThat(result.isValid()).isTrue();
    assertThat(result.getErrorMessage()).isEmpty();
  }

  @Test
  public void invalidResultHasInvalidStatus() {
    final InclusionListValidationResult result =
        InclusionListValidationResult.invalid("bad encoding");
    assertThat(result.getStatus()).isEqualTo(InclusionListValidationResult.Status.INVALID);
    assertThat(result.isValid()).isFalse();
    assertThat(result.getErrorMessage()).contains("bad encoding");
  }

  @Test
  public void unsatisfiedResultHasUnsatisfiedStatus() {
    final InclusionListValidationResult result =
        InclusionListValidationResult.unsatisfied("missing transaction 0xaabb");
    assertThat(result.getStatus()).isEqualTo(InclusionListValidationResult.Status.UNSATISFIED);
    assertThat(result.isValid()).isFalse();
    assertThat(result.getErrorMessage()).contains("missing transaction 0xaabb");
  }

  @Test
  public void statusEnumHasThreeValues() {
    assertThat(InclusionListValidationResult.Status.values()).hasSize(3);
    assertThat(InclusionListValidationResult.Status.values())
        .containsExactly(
            InclusionListValidationResult.Status.VALID,
            InclusionListValidationResult.Status.INVALID,
            InclusionListValidationResult.Status.UNSATISFIED);
  }
}
