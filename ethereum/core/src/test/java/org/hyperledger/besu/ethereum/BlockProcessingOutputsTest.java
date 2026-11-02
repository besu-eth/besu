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
package org.hyperledger.besu.ethereum;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.hyperledger.besu.ethereum.mainnet.inclusionlist.InclusionListValidationResult;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class BlockProcessingOutputsTest {

  @Test
  void inclusionListIsSatisfiedWhenProcessedWithoutOne() {
    final BlockProcessingOutputs outputs =
        new BlockProcessingOutputs(null, List.of(), Optional.empty(), Optional.empty(), 0);

    assertThat(outputs.getInclusionListValidationResult().isValid()).isTrue();
  }

  @Test
  void keepsTheInclusionListValidationResult() {
    final InclusionListValidationResult unsatisfied =
        InclusionListValidationResult.unsatisfied("unsatisfied");
    final BlockProcessingOutputs outputs =
        new BlockProcessingOutputs(
            null, List.of(), Optional.empty(), Optional.empty(), 0, Map.of(), unsatisfied);

    assertThat(outputs.getInclusionListValidationResult()).isSameAs(unsatisfied);
  }

  @Test
  void rejectsNullInclusionListValidationResult() {
    assertThatNullPointerException()
        .isThrownBy(
            () ->
                new BlockProcessingOutputs(
                    null, List.of(), Optional.empty(), Optional.empty(), 0, Map.of(), null));
  }
}
