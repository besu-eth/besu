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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;

/**
 * Pays no rewards and leaves the world state untouched. Use {@link
 * BlockRewardProcessor#NO_REWARDS}.
 */
final class NoBlockRewardProcessor implements BlockRewardProcessor {

  NoBlockRewardProcessor() {}

  @Override
  public boolean rewardBeneficiaries(
      final MutableWorldState worldState,
      final ProcessableBlockHeader header,
      final List<BlockHeader> ommers,
      final Address miningBeneficiary) {
    return true;
  }

  @Override
  public Wei getBlockReward() {
    return Wei.ZERO;
  }

  @Override
  public Wei getCoinbaseReward(final int numberOfOmmers) {
    return Wei.ZERO;
  }

  @Override
  public Wei getOmmerReward(final long blockNumber, final long ommerBlockNumber) {
    return Wei.ZERO;
  }
}
