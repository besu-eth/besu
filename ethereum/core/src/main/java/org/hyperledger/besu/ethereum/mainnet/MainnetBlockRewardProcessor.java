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

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pays the mainnet rewards on every block: the block reward plus 1/32 of it per ommer to the mining
 * beneficiary, and the block reward less 1/8 of it per block of distance to each ommer's coinbase.
 *
 * <p>The block reward must not be zero: crediting a zero reward would still create the beneficiary
 * as an empty account. A fork that pays nothing uses {@link BlockRewardProcessor#NO_REWARDS}, and a
 * configured reward goes through {@link BlockRewardProcessor#of}.
 */
public class MainnetBlockRewardProcessor implements BlockRewardProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(MainnetBlockRewardProcessor.class);

  private static final int MAX_GENERATION = 6;

  private final Wei blockReward;

  /**
   * @param blockReward the block reward, which must not be zero
   */
  public MainnetBlockRewardProcessor(final Wei blockReward) {
    checkArgument(
        !blockReward.isZero(),
        "A zero block reward pays nothing: use BlockRewardProcessor.NO_REWARDS or BlockRewardProcessor.of");
    this.blockReward = blockReward;
  }

  @Override
  public boolean rewardBeneficiaries(
      final MutableWorldState worldState,
      final ProcessableBlockHeader header,
      final List<BlockHeader> ommers,
      final Address miningBeneficiary) {
    for (final BlockHeader ommerHeader : ommers) {
      final long distance = header.getNumber() - ommerHeader.getNumber();
      if (distance < 1 || distance > MAX_GENERATION) {
        LOG.info(
            "Block processing error: ommer block number {} is not within {} generations of block number {}",
            ommerHeader.getNumber(),
            MAX_GENERATION,
            header.getNumber());
        return false;
      }
    }

    final WorldUpdater updater = worldState.updater();
    updater.getOrCreate(miningBeneficiary).incrementBalance(getCoinbaseReward(ommers.size()));
    for (final BlockHeader ommerHeader : ommers) {
      final MutableAccount ommerCoinbase = updater.getOrCreate(ommerHeader.getCoinbase());
      ommerCoinbase.incrementBalance(getOmmerReward(header.getNumber(), ommerHeader.getNumber()));
    }

    updater.commit();
    return true;
  }

  @Override
  public Wei getBlockReward() {
    return blockReward;
  }

  @Override
  public Wei getCoinbaseReward(final int numberOfOmmers) {
    return blockReward.add(blockReward.multiply(numberOfOmmers).divide(32));
  }

  @Override
  public Wei getOmmerReward(final long blockNumber, final long ommerBlockNumber) {
    final long distance = blockNumber - ommerBlockNumber;
    return blockReward.subtract(blockReward.multiply(distance).divide(8));
  }
}
