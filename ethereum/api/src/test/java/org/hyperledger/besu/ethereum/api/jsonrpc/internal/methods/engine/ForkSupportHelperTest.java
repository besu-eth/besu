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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PRAGUE;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.ForkSupportHelper.validateForkSupported;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;

import org.junit.jupiter.api.Test;

class ForkSupportHelperTest {

  @Test
  void validForkIfMilestoneOlderThanBlock() {
    assertThat(validateForkSupported(PRAGUE, 0L, 1)).isEqualTo(ValidationResult.valid());
  }

  @Test
  void validForkIfMilestoneEqualToBlock() {
    assertThat(validateForkSupported(PRAGUE, 0L, 0)).isEqualTo(ValidationResult.valid());
  }

  @Test
  void validForkWhenTimestampOverflowsSignedLong() {
    long unsignedLongMaxValue = Long.parseUnsignedLong("18446744073709551615");
    assertThat(validateForkSupported(PRAGUE, 1L, unsignedLongMaxValue))
        .isEqualTo(ValidationResult.valid());
  }

  @Test
  void unsupportedForkIfBlockOlderThanMilestone() {
    assertThat(validateForkSupported(PRAGUE, 1L, 0))
        .isEqualTo(
            ValidationResult.invalid(RpcErrorType.UNSUPPORTED_FORK, "message equality ignored"));
  }
}
