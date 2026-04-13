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

class InclusionListConfigurationTest {

  @Test
  void defaultConfigurationHasStrictValidationAndDefaultSelector() {
    final InclusionListConfiguration config = InclusionListConfiguration.DEFAULT;
    assertThat(config.validationMode()).isEqualTo(InclusionListValidationMode.STRICT);
    assertThat(config.selector()).isInstanceOf(DefaultInclusionListSelector.class);
  }

  @Test
  void customConfigurationPreservesValues() {
    final DefaultInclusionListSelector selector = new DefaultInclusionListSelector();
    final InclusionListConfiguration config =
        new InclusionListConfiguration(InclusionListValidationMode.LENIENT, selector);
    assertThat(config.validationMode()).isEqualTo(InclusionListValidationMode.LENIENT);
    assertThat(config.selector()).isSameAs(selector);
  }

  @Test
  void createValidatorReturnsStrictForStrictMode() {
    final InclusionListConfiguration config =
        new InclusionListConfiguration(
            InclusionListValidationMode.STRICT, new DefaultInclusionListSelector());
    assertThat(config.createValidator()).isInstanceOf(StrictInclusionListValidator.class);
  }

  @Test
  void createValidatorReturnsLenientForLenientMode() {
    final InclusionListConfiguration config =
        new InclusionListConfiguration(
            InclusionListValidationMode.LENIENT, new DefaultInclusionListSelector());
    assertThat(config.createValidator()).isInstanceOf(LenientInclusionListValidator.class);
  }
}
