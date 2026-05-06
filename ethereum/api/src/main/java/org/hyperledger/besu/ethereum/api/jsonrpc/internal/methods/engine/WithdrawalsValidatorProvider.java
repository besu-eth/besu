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

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.WithdrawalsValidator;

import java.util.Optional;

public class WithdrawalsValidatorProvider {

  static WithdrawalsValidator getWithdrawalsValidator(
      final ProtocolSchedule protocolSchedule, final long blockTimestamp, final long blockNumber) {

    final BlockHeader blockHeader =
        BlockHeaderBuilder.createDefault()
            .timestamp(blockTimestamp)
            .number(blockNumber)
            .buildBlockHeader();
    return getWithdrawalsValidator(protocolSchedule.getByBlockHeader(blockHeader));
  }

  public static WithdrawalsValidator getWithdrawalsValidator(
      final ProtocolSchedule protocolSchedule,
      final BlockHeader parentBlockHeader,
      final long timestampForNextBlock) {

    return getWithdrawalsValidator(
        protocolSchedule.getForNextBlockHeader(parentBlockHeader, timestampForNextBlock));
  }

  private static WithdrawalsValidator getWithdrawalsValidator(final ProtocolSpec protocolSchedule) {
    return Optional.ofNullable(protocolSchedule)
        .map(ProtocolSpec::getWithdrawalsValidator)
        .orElseGet(WithdrawalsValidator.ProhibitedWithdrawals::new);
  }
}
