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
package org.hyperledger.besu.consensus.qbft.core.types;

import org.hyperledger.besu.consensus.common.bft.BlockTimer;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.RoundTimer;
import org.hyperledger.besu.consensus.common.bft.network.ValidatorMulticaster;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.datatypes.Address;

import java.time.Clock;
import java.util.Collection;

/** This is the full data set, or context, required for many of the aspects of QBFT workflows. */
public interface QbftFinalState {

  /**
   * Gets validator multicaster.
   *
   * @return the validator multicaster
   */
  ValidatorMulticaster getValidatorMulticaster();

  /**
   * Gets node key.
   *
   * @return the node key
   */
  NodeKey getNodeKey();

  /**
   * Gets round timer.
   *
   * @return the round timer
   */
  RoundTimer getRoundTimer();

  /**
   * Is local node validator.
   *
   * @return true if the local node is a validator, false otherwise
   */
  boolean isLocalNodeValidator();

  /**
   * Gets validators.
   *
   * @return the validators
   */
  Collection<Address> getValidators();

  /**
   * Gets local address.
   *
   * @return the local address
   */
  Address getLocalAddress();

  /**
   * Gets clock.
   *
   * @return the clock
   */
  Clock getClock();

  /**
   * Gets block creator factory.
   *
   * @return the block creator factory
   */
  QbftBlockCreatorFactory getBlockCreatorFactory();

  /**
   * Gets quorum.
   *
   * @return the quorum
   */
  int getQuorum();

  /**
   * Gets block timer.
   *
   * @return the block timer
   */
  BlockTimer getBlockTimer();

  /**
   * Is local node proposer for round.
   *
   * @param roundIdentifier the round identifier
   * @return true if the local node is the proposer for the given round, false otherwise
   */
  boolean isLocalNodeProposerForRound(ConsensusRoundIdentifier roundIdentifier);
}
