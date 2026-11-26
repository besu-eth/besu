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
package org.hyperledger.besu.consensus.qbft.core.statemachine;

import org.hyperledger.besu.consensus.common.bft.BftHelpers;
import org.hyperledger.besu.consensus.qbft.core.payload.MessageFactory;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockHeader;
import org.hyperledger.besu.consensus.qbft.core.types.QbftFinalState;
import org.hyperledger.besu.consensus.qbft.core.types.QbftValidatorModeTransitionLogger;
import org.hyperledger.besu.consensus.qbft.core.types.QbftValidatorProvider;
import org.hyperledger.besu.consensus.qbft.core.validation.MessageValidatorFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Qbft block height manager factory. */
public class QbftBlockHeightManagerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(QbftBlockHeightManagerFactory.class);

  private final QbftRoundFactory roundFactory;
  private final QbftFinalState finalState;
  private final MessageValidatorFactory messageValidatorFactory;
  private final MessageFactory messageFactory;
  private final QbftValidatorProvider validatorProvider;
  private final QbftValidatorModeTransitionLogger validatorModeTransitionLogger;
  private boolean isEarlyRoundChangeEnabled = false;

  /**
   * Instantiates a new Qbft block height manager factory.
   *
   * @param finalState the final state
   * @param roundFactory the round factory
   * @param messageValidatorFactory the message validator factory
   * @param messageFactory the message factory
   * @param validatorProvider the validator provider
   * @param validatorModeTransitionLogger the validator mode transition logger
   */
  public QbftBlockHeightManagerFactory(
      final QbftFinalState finalState,
      final QbftRoundFactory roundFactory,
      final MessageValidatorFactory messageValidatorFactory,
      final MessageFactory messageFactory,
      final QbftValidatorProvider validatorProvider,
      final QbftValidatorModeTransitionLogger validatorModeTransitionLogger) {
    this.roundFactory = roundFactory;
    this.finalState = finalState;
    this.messageValidatorFactory = messageValidatorFactory;
    this.messageFactory = messageFactory;
    this.validatorProvider = validatorProvider;
    this.validatorModeTransitionLogger = validatorModeTransitionLogger;
  }

  /**
   * Create base qbft block height manager.
   *
   * @param parentHeader the parent header
   * @return the base qbft block height manager
   */
  public BaseQbftBlockHeightManager create(final QbftBlockHeader parentHeader) {
    validatorModeTransitionLogger.logTransitionChange(parentHeader);

    if (finalState.isLocalNodeValidator()) {
      LOG.debug("Local node is a validator");
      return createFullBlockHeightManager(parentHeader);
    } else {
      LOG.debug("Local node is a non-validator");
      return createNoOpBlockHeightManager(parentHeader);
    }
  }

  /**
   * Sets early round change enabled.
   *
   * @param isEarlyRoundChangeEnabled the is early round change enabled
   */
  public void isEarlyRoundChangeEnabled(final boolean isEarlyRoundChangeEnabled) {
    this.isEarlyRoundChangeEnabled = isEarlyRoundChangeEnabled;
  }

  /**
   * Creates a no-op height manager
   *
   * @param parentHeader the parent header
   * @return the no-op height manager
   */
  protected BaseQbftBlockHeightManager createNoOpBlockHeightManager(
      final QbftBlockHeader parentHeader) {
    return new NoOpBlockHeightManager(parentHeader);
  }

  private BaseQbftBlockHeightManager createFullBlockHeightManager(
      final QbftBlockHeader parentHeader) {

    QbftBlockHeightManager qbftBlockHeightManager;
    RoundChangeManager roundChangeManager;

    if (isEarlyRoundChangeEnabled) {
      roundChangeManager =
          new RoundChangeManager(
              BftHelpers.calculateRequiredValidatorQuorum(finalState.getValidators().size()),
              BftHelpers.calculateRequiredFutureRCQuorum(finalState.getValidators().size()),
              messageValidatorFactory.createRoundChangeMessageValidator(
                  parentHeader.getNumber() + 1L, parentHeader),
              finalState.getLocalAddress());
      qbftBlockHeightManager =
          new QbftBlockHeightManager(
              parentHeader,
              finalState,
              roundChangeManager,
              roundFactory,
              finalState.getClock(),
              messageValidatorFactory,
              messageFactory,
              validatorProvider,
              true);
    } else {
      roundChangeManager =
          new RoundChangeManager(
              BftHelpers.calculateRequiredValidatorQuorum(finalState.getValidators().size()),
              messageValidatorFactory.createRoundChangeMessageValidator(
                  parentHeader.getNumber() + 1L, parentHeader),
              finalState.getLocalAddress());
      qbftBlockHeightManager =
          new QbftBlockHeightManager(
              parentHeader,
              finalState,
              roundChangeManager,
              roundFactory,
              finalState.getClock(),
              messageValidatorFactory,
              messageFactory,
              validatorProvider);
    }

    return qbftBlockHeightManager;
  }
}
