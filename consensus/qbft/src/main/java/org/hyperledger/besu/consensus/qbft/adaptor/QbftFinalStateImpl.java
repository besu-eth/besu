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
package org.hyperledger.besu.consensus.qbft.adaptor;

import org.hyperledger.besu.consensus.common.bft.BftHelpers;
import org.hyperledger.besu.consensus.common.bft.BlockTimer;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.RoundTimer;
import org.hyperledger.besu.consensus.common.bft.blockcreation.ProposerSelector;
import org.hyperledger.besu.consensus.common.bft.network.ValidatorMulticaster;
import org.hyperledger.besu.consensus.common.validator.ValidatorProvider;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockCreatorFactory;
import org.hyperledger.besu.consensus.qbft.core.types.QbftFinalState;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.datatypes.Address;

import java.time.Clock;
import java.util.Collection;

/** Besu implementation of QbftFinalState for maintaining the state of a QBFT network. */
public class QbftFinalStateImpl implements QbftFinalState {
  private final ValidatorProvider validatorProvider;
  private final NodeKey nodeKey;
  private final Address localAddress;
  private final ProposerSelector proposerSelector;
  private final ValidatorMulticaster validatorMulticaster;
  private final RoundTimer roundTimer;
  private final BlockTimer blockTimer;
  private final QbftBlockCreatorFactory blockCreatorFactory;
  private final Clock clock;

  /**
   * Constructs a new QBFT final state.
   *
   * @param validatorProvider the validator provider
   * @param nodeKey the node key
   * @param localAddress the local address
   * @param proposerSelector the proposer selector
   * @param validatorMulticaster the validator multicaster
   * @param roundTimer the round timer
   * @param blockTimer the block timer
   * @param blockCreatorFactory the block creator factory
   * @param clock the clock
   */
  public QbftFinalStateImpl(
      final ValidatorProvider validatorProvider,
      final NodeKey nodeKey,
      final Address localAddress,
      final ProposerSelector proposerSelector,
      final ValidatorMulticaster validatorMulticaster,
      final RoundTimer roundTimer,
      final BlockTimer blockTimer,
      final QbftBlockCreatorFactory blockCreatorFactory,
      final Clock clock) {
    this.validatorProvider = validatorProvider;
    this.nodeKey = nodeKey;
    this.localAddress = localAddress;
    this.proposerSelector = proposerSelector;
    this.validatorMulticaster = validatorMulticaster;
    this.roundTimer = roundTimer;
    this.blockTimer = blockTimer;
    this.blockCreatorFactory = blockCreatorFactory;
    this.clock = clock;
  }

  /**
   * Gets validators.
   *
   * @return the validators
   */
  @Override
  public Collection<Address> getValidators() {
    return validatorProvider.getValidatorsAtHead();
  }

  /**
   * Gets the validator multicaster.
   *
   * @return the validator multicaster
   */
  @Override
  public ValidatorMulticaster getValidatorMulticaster() {
    return validatorMulticaster;
  }

  /**
   * Gets node key.
   *
   * @return the node key
   */
  @Override
  public NodeKey getNodeKey() {
    return nodeKey;
  }

  /**
   * Gets local address.
   *
   * @return the local address
   */
  @Override
  public Address getLocalAddress() {
    return localAddress;
  }

  /**
   * Is local node validator.
   *
   * @return the boolean
   */
  @Override
  public boolean isLocalNodeValidator() {
    return getValidators().contains(localAddress);
  }

  /**
   * Gets round timer.
   *
   * @return the round timer
   */
  @Override
  public RoundTimer getRoundTimer() {
    return roundTimer;
  }

  /**
   * Gets block creator factory.
   *
   * @return the block creator factory
   */
  @Override
  public QbftBlockCreatorFactory getBlockCreatorFactory() {
    return blockCreatorFactory;
  }

  @Override
  public int getQuorum() {
    return BftHelpers.calculateRequiredValidatorQuorum(getValidators().size());
  }

  @Override
  public BlockTimer getBlockTimer() {
    return blockTimer;
  }

  /**
   * Is local node proposer for round.
   *
   * @param roundIdentifier the round identifier
   * @return the boolean
   */
  @Override
  public boolean isLocalNodeProposerForRound(final ConsensusRoundIdentifier roundIdentifier) {
    return getProposerForRound(roundIdentifier).equals(localAddress);
  }

  /**
   * Gets proposer for round.
   *
   * @param roundIdentifier the round identifier
   * @return the proposer for round
   */
  public Address getProposerForRound(final ConsensusRoundIdentifier roundIdentifier) {
    return proposerSelector.selectProposerForRound(roundIdentifier);
  }

  /**
   * Gets clock.
   *
   * @return the clock
   */
  @Override
  public Clock getClock() {
    return clock;
  }
}
