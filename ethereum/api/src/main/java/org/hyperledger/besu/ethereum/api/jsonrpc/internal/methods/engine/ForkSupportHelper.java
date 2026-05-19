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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;

import java.util.Optional;

public class ForkSupportHelper {

  public static ValidationResult<RpcErrorType> validateForkSupported(
      final HardforkId firstSupportedHardforkId,
      final Long firstSupportedForkMilestone,
      final long blockTimestamp) {

    if (Long.compareUnsigned(blockTimestamp, firstSupportedForkMilestone) < 0) {
      return ValidationResult.invalid(
          RpcErrorType.UNSUPPORTED_FORK,
          firstSupportedHardforkId.name()
              + " configured to start at timestamp: "
              + firstSupportedForkMilestone);
    }

    return ValidationResult.valid();
  }

  public static ValidationResult<RpcErrorType> validateForkSupported(
      final HardforkId firstSupportedHardforkId,
      final Optional<Long> maybeFirstSupportedForkMilestone,
      final HardforkId firstUnsupportedHardforkId,
      final Optional<Long> maybeFirstUnsupportedMilestone,
      final long blockTimestamp) {

    if (maybeFirstSupportedForkMilestone.isPresent()) {
      var result =
          validateForkSupported(
              firstSupportedHardforkId, maybeFirstSupportedForkMilestone.get(), blockTimestamp);

      if (!result.isValid()) {
        return result;
      }
    }

    if (maybeFirstUnsupportedMilestone.isPresent()
        && Long.compareUnsigned(blockTimestamp, maybeFirstUnsupportedMilestone.get()) >= 0) {
      return ValidationResult.invalid(
          RpcErrorType.UNSUPPORTED_FORK,
          "block timestamp "
              + blockTimestamp
              + " is after the first unsupported milestone: "
              + firstUnsupportedHardforkId.name()
              + " at timestamp "
              + maybeFirstUnsupportedMilestone.get());
    }

    return ValidationResult.valid();
  }
}
