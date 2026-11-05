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
  @ValueSource(ints = {0, 1, 32, 34})
  void rejectsAnEncodingThatIsNot33Bytes(final int size) {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> InclusionListStatus.decode(Bytes.wrap(new byte[size])));
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 0xff})
  void rejectsASatisfiedFlagThatIsNotZeroOrOne(final int flag) {
    final Bytes encoded = Bytes.concatenate(Bytes32.random(), Bytes.of(flag));

    assertThatIllegalArgumentException().isThrownBy(() -> InclusionListStatus.decode(encoded));
  }
}
