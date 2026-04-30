/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.config;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import org.apache.tuweni.units.bigints.UInt256;

/** The interface Genesis config options. */
public interface GenesisConfigOptions {

  /**
   * Is eth hash boolean.
   *
   * @return the boolean
   */
  boolean isEthHash();

  /**
   * Is ibft legacy boolean (NOTE this is a deprecated feature).
   *
   * @return the boolean
   */
  boolean isIbftLegacy();

  /**
   * Is ibft 2 boolean.
   *
   * @return the boolean
   */
  boolean isIbft2();

  /**
   * Is qbft boolean.
   *
   * @return the boolean
   */
  boolean isQbft();

  /**
   * Is clique boolean.
   *
   * @return the boolean
   */
  boolean isClique();

  /**
   * Is a Proof of Authority network.
   *
   * @return the boolean
   */
  boolean isPoa();

  /**
   * If the network has support for transitioning to Proof of Stake. It is not important when the
   * transition has happened or will happen, but only that there is support for it in the genesis
   * file.
   *
   * @return the boolean
   */
  boolean hasPos();

  /**
   * Is consensus migration boolean.
   *
   * @return the boolean
   */
  default boolean isConsensusMigration() {
    return (isIbft2() || isIbftLegacy()) && isQbft();
  }

  /**
   * Gets consensus engine.
   *
   * @return the consensus engine
   */
  String getConsensusEngine();

  /**
   * Gets ibft legacy config options.
   *
   * @return the ibft legacy config options
   */
  IbftLegacyConfigOptions getIbftLegacyConfigOptions();

  /**
   * Gets checkpoint options.
   *
   * @return the checkpoint options
   */
  CheckpointConfigOptions getCheckpointOptions();

  /**
   * Gets clique config options.
   *
   * @return the clique config options
   */
  CliqueConfigOptions getCliqueConfigOptions();

  /**
   * Gets bft config options.
   *
   * @return the bft config options
   */
  BftConfigOptions getBftConfigOptions();

  /**
   * Gets qbft config options.
   *
   * @return the qbft config options
   */
  QbftConfigOptions getQbftConfigOptions();

  /**
   * Gets discovery options.
   *
   * @return the discovery options
   */
  DiscoveryOptions getDiscoveryOptions();

  /**
   * Gets ethash config options.
   *
   * @return the ethash config options
   */
  EthashConfigOptions getEthashConfigOptions();

  /**
   * Gets homestead block number.
   *
   * @return the homestead block number
   */
  OptionalLong getHomesteadBlockNumber();

  /**
   * Gets dao fork block.
   *
   * @return the dao fork block
   */
  OptionalLong getDaoForkBlock();

  /**
   * Gets tangerine whistle block number.
   *
   * @return the tangerine whistle block number
   */
  OptionalLong getTangerineWhistleBlockNumber();

  /**
   * Gets spurious dragon block number.
   *
   * @return the spurious dragon block number
   */
  OptionalLong getSpuriousDragonBlockNumber();

  /**
   * Gets byzantium block number.
   *
   * @return the byzantium block number
   */
  OptionalLong getByzantiumBlockNumber();

  /**
   * Gets constantinople block number.
   *
   * @return the constantinople block number
   */
  OptionalLong getConstantinopleBlockNumber();

  /**
   * Gets petersburg block number.
   *
   * @return the petersburg block number
   */
  OptionalLong getPetersburgBlockNumber();

  /**
   * Gets istanbul block number.
   *
   * @return the istanbul block number
   */
  OptionalLong getIstanbulBlockNumber();

  /**
   * Gets muir glacier block number.
   *
   * @return the muir glacier block number
   */
  OptionalLong getMuirGlacierBlockNumber();

  /**
   * Gets berlin block number.
   *
   * @return the berlin block number
   */
  OptionalLong getBerlinBlockNumber();

  /**
   * Gets london block number.
   *
   * @return the london block number
   */
  OptionalLong getLondonBlockNumber();

  /**
   * Gets arrow glacier block number.
   *
   * @return the arrow glacier block number
   */
  OptionalLong getArrowGlacierBlockNumber();

  /**
   * Gets gray glacier block number.
   *
   * @return the gray glacier block number
   */
  OptionalLong getGrayGlacierBlockNumber();

  /**
   * Gets merge net split block number.
   *
   * @return the merge net split block number
   */
  OptionalLong getMergeNetSplitBlockNumber();

  /**
   * Gets shanghai time.
   *
   * @return the shanghai time
   */
  OptionalLong getShanghaiTime();

  /**
   * Gets cancun time.
   *
   * @return the cancun time
   */
  OptionalLong getCancunTime();

  /**
   * Gets prague time.
   *
   * @return the prague time
   */
  OptionalLong getPragueTime();

  /**
   * Gets Osaka time.
   *
   * @return the osaka time
   */
  OptionalLong getOsakaTime();

  /**
   * Gets bpo1 time.
   *
   * @return the bpo1 time
   */
  OptionalLong getBpo1Time();

  /**
   * Gets bpo2 time.
   *
   * @return the bpo2 time
   */
  OptionalLong getBpo2Time();

  /**
   * Gets bpo3 time.
   *
   * @return the bpo3 time
   */
  OptionalLong getBpo3Time();

  /**
   * Gets bpo4 time.
   *
   * @return the bpo4 time
   */
  OptionalLong getBpo4Time();

  /**
   * Gets bpo5 time.
   *
   * @return the bpo5 time
   */
  OptionalLong getBpo5Time();

  /**
   * Gets Amsterdam time.
   *
   * @return the Amsterdam time
   */
  OptionalLong getAmsterdamTime();

  /**
   * Gets Bogota time.
   *
   * @return the Bogota time
   */
  OptionalLong getBogotaTime();

  /**
   * Gets future eips time.
   *
   * @return the future eips time
   */
  OptionalLong getFutureEipsTime();

  /**
   * Gets experimental eips time.
   *
   * @return the experimental eips time
   */
  OptionalLong getExperimentalEipsTime();

  /**
   * Gets base fee per gas.
   *
   * @return the base fee per gas
   */
  Optional<Wei> getBaseFeePerGas();

  /**
   * Gets terminal total difficulty.
   *
   * @return the terminal total difficulty
   */
  Optional<UInt256> getTerminalTotalDifficulty();

  /**
   * Gets terminal block number.
   *
   * @return the terminal block number
   */
  OptionalLong getTerminalBlockNumber();

  /**
   * Gets terminal block hash.
   *
   * @return the terminal block hash
   */
  Optional<Hash> getTerminalBlockHash();

  /**
   * Gets fork block numbers.
   *
   * @return the fork block numbers
   */
  List<Long> getForkBlockNumbers();

  /**
   * Gets fork block timestamps.
   *
   * @return the fork block timestamps
   */
  List<Long> getForkBlockTimestamps();

  /**
   * Gets chain id.
   *
   * @return the chain id
   */
  Optional<BigInteger> getChainId();

  /**
   * Gets contract size limit.
   *
   * @return the contract size limit
   */
  OptionalInt getContractSizeLimit();

  /**
   * Gets evm stack size.
   *
   * @return the evm stack size
   */
  OptionalInt getEvmStackSize();

  /**
   * As map map.
   *
   * @return the map
   */
  Map<String, Object> asMap();

  /**
   * Gets transitions.
   *
   * @return the transitions
   */
  TransitionsConfigOptions getTransitions();

  /**
   * The PoW algorithm associated with the genesis file.
   *
   * @return the PoW algorithm in use.
   */
  PowAlgorithm getPowAlgorithm();

  /**
   * The elliptic curve which should be used in SignatureAlgorithm.
   *
   * @return the name of the elliptic curve.
   */
  Optional<String> getEcCurve();

  /**
   * Set a Zero Base Fee network so that free gas can be used with London/EIP-1559. Once the chain
   * has a zero base fee, you cannot go back to a non-zero base fee.
   *
   * @return true, if you want the next block to use zero for the base fee.
   */
  boolean isZeroBaseFee();

  /**
   * Force a Base Fee as Gas Price network to used with London/EIP-1559.
   *
   * @return true, if you want the next block to use the base fee as gas price.
   */
  boolean isFixedBaseFee();

  /**
   * The withdrawal request predeploy address
   *
   * @return the withdrawal request predeploy address
   */
  Optional<Address> getWithdrawalRequestContractAddress();

  /**
   * The deposit contract address that should be in the logger field in Receipt of Deposit
   * transaction
   *
   * @return the deposit address
   */
  Optional<Address> getDepositContractAddress();

  /**
   * The consolidation request contract address
   *
   * @return the consolidation request contract address
   */
  Optional<Address> getConsolidationRequestContractAddress();

  /**
   * The blob schedule is a list of hardfork names and their associated target and max blob values.
   *
   * @return the blob schedule
   */
  Optional<BlobScheduleOptions> getBlobScheduleOptions();
}
