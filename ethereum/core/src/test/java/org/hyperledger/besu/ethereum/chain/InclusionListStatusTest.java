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
package org.hyperledger.besu.ethereum.chain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class InclusionListStatusTest {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void encodedStatusDecodesToTheSameStatus(final boolean satisfied) {
    final InclusionListStatus status = new InclusionListStatus(Bytes32.random(), satisfied);

    assertThat(InclusionListStatus.decode(status.encode())).isEqualTo(status);
  }

  @Test
  void isOnlyForTheInclusionListItWasComputedAgainst() {
    final Bytes32 checksum = Bytes32.random();
    final InclusionListStatus status = new InclusionListStatus(checksum, false);

    assertThat(status.isFor(checksum)).isTrue();
    assertThat(status.isFor(Bytes32.random())).isFalse();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void decodesStatusStoredWithoutChecksum(final boolean satisfied) {
    final InclusionListStatus status = InclusionListStatus.decode(Bytes.of(satisfied ? 1 : 0));

    assertThat(status.satisfied()).isEqualTo(satisfied);
    // it does not match any inclusion list, so the block is validated again
    assertThat(status.inclusionListChecksum()).isEqualTo(InclusionListStatus.UNKNOWN_CHECKSUM);
  }

  @Test
  void rejectsInvalidEncoding() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> InclusionListStatus.decode(Bytes.of(1, 2)));
  }
}
