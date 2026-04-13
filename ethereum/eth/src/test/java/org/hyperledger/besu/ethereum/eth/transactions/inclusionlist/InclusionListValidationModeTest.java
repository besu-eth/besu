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

class InclusionListValidationModeTest {

  @Test
  void strictModeCreatesStrictValidator() {
    final InclusionListValidator validator = InclusionListValidationMode.STRICT.createValidator();
    assertThat(validator).isInstanceOf(StrictInclusionListValidator.class);
  }

  @Test
  void lenientModeCreatesLenientValidator() {
    final InclusionListValidator validator = InclusionListValidationMode.LENIENT.createValidator();
    assertThat(validator).isInstanceOf(LenientInclusionListValidator.class);
  }

  @Test
  void defaultIsStrict() {
    assertThat(InclusionListValidationMode.STRICT.ordinal()).isZero();
  }

  @Test
  void valueOfStrict() {
    assertThat(InclusionListValidationMode.valueOf("STRICT"))
        .isEqualTo(InclusionListValidationMode.STRICT);
  }

  @Test
  void valueOfLenient() {
    assertThat(InclusionListValidationMode.valueOf("LENIENT"))
        .isEqualTo(InclusionListValidationMode.LENIENT);
  }
}
