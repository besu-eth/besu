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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.List;

import org.junit.jupiter.api.Test;

class ContributedForkActivationsTest {

  @Test
  void haveCanonicalValueSemantics() {
    final ContributedForkActivations canonical =
        new ContributedForkActivations(List.of(10L, 11L), List.of(1000L, 2000L));
    final ContributedForkActivations reorderedWithDuplicates =
        new ContributedForkActivations(List.of(11L, 10L, 11L), List.of(2000L, 1000L, 2000L));

    assertThat(reorderedWithDuplicates).isEqualTo(canonical);
    assertThat(reorderedWithDuplicates.blockNumbers()).containsExactly(10L, 11L);
    assertThat(reorderedWithDuplicates.timestamps()).containsExactly(1000L, 2000L);
  }

  @Test
  void rejectNegativeValues() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new ContributedForkActivations(List.of(-1L), List.of()))
        .withMessageContaining("negative");
  }
}
