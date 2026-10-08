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
 * Pays a block's rewards: the reward to the block's mining beneficiary and to the coinbase of each
 * of its ommers. Each {@link ProtocolSpec} provides one, and block import and block creation both
 * use it, so they apply the same rules.
 *
 * <p>Proof-of-work forks pay both rewards; IBFT 2.0 and QBFT pay a configurable block reward, with
 * no ommers. Proof of stake and Clique pay nothing.
 *
 * <p>The reward formulas used to be default methods on {@link BlockProcessor}, overridable per
 * processor. The last override was {@code ClassicBlockProcessor}, which computed Ethereum Classic's
 * era-based rewards (ECIP-1017) from the block number; it was removed with Classic support (#9671).
 * A fork that needs different rewards provides its own implementation.
 */
public interface BlockRewardProcessor {

  /** Frontier through Spurious Dragon: 5 ETH. */
  BlockRewardProcessor FRONTIER = new MainnetBlockRewardProcessor(Wei.fromEth(5));

  /** Byzantium: 3 ETH (EIP-649). */
  BlockRewardProcessor BYZANTIUM = new MainnetBlockRewardProcessor(Wei.fromEth(3));

  /** Constantinople until the Merge: 2 ETH (EIP-1234). */
  BlockRewardProcessor CONSTANTINOPLE = new MainnetBlockRewardProcessor(Wei.fromEth(2));

  /** Pays no rewards and leaves the world state untouched, as on proof of stake and Clique. */
  BlockRewardProcessor NO_REWARDS = new NoBlockRewardProcessor();

  /**
   * Returns the processor for a configured block reward: {@link #NO_REWARDS} if it is zero, so a
   * zero reward leaves the world state untouched, and the mainnet reward rules otherwise. Use it
   * whenever the reward comes from configuration, as the {@link MainnetBlockRewardProcessor}
   * constructor rejects a zero reward.
   *
   * @param blockReward the configured block reward
   * @return the block reward processor
   */
  static BlockRewardProcessor of(final Wei blockReward) {
    return blockReward.isZero() ? NO_REWARDS : new MainnetBlockRewardProcessor(blockReward);
  }

  /**
   * Credits the mining beneficiary and the coinbase of each ommer.
   *
   * <p>Block import pays rewards before block body validation checks the ommers, so the ommers may
   * still be invalid. An ommer that is not one to six blocks older than the block would get a
   * negative or overflowing reward, so the processor rejects it instead of crediting anything.
   *
   * @param worldState the world state to credit
   * @param header the header of the block being rewarded
   * @param ommers the ommers included in the block
   * @param miningBeneficiary the address that receives the block's reward
   * @return {@code false} if an ommer is not one to six blocks older than the block, in which case
   *     the world state is left untouched
   */
  boolean rewardBeneficiaries(
      MutableWorldState worldState,
      ProcessableBlockHeader header,
      List<BlockHeader> ommers,
      Address miningBeneficiary);

  /**
   * Returns the reward paid to the block's mining beneficiary when the block has no ommers.
   *
   * @return the block reward
   */
  Wei getBlockReward();

  /**
   * Returns the reward paid to the block's mining beneficiary.
   *
   * @param numberOfOmmers the number of ommers included in the block
   * @return the coinbase reward
   */
  Wei getCoinbaseReward(int numberOfOmmers);

  /**
   * Returns the reward paid to an ommer's coinbase.
   *
   * @param blockNumber the number of the block that includes the ommer
   * @param ommerBlockNumber the number of the ommer
   * @return the ommer reward
   */
  Wei getOmmerReward(long blockNumber, long ommerBlockNumber);
}
