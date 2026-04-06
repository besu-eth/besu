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
package org.hyperledger.besu.consensus.qbft.validator;

import org.hyperledger.besu.config.QbftConfigOptions;
import org.hyperledger.besu.consensus.common.ForkSpec;
import org.hyperledger.besu.consensus.common.ForksSchedule;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Validator mode transition logger. */
public class ValidatorModeTransitionLogger {

  private static final Logger LOG = LoggerFactory.getLogger(ValidatorModeTransitionLogger.class);

  private final ForksSchedule<QbftConfigOptions> forksSchedule;
  private final Consumer<String> msgConsumer;

  /**
   * Instantiates a new Validator mode transition logger.
   *
   * @param forksSchedule the forks schedule
   */
  public ValidatorModeTransitionLogger(final ForksSchedule<QbftConfigOptions> forksSchedule) {
    this.forksSchedule = forksSchedule;
    this.msgConsumer = LOG::info;
  }

  /**
   * Instantiates a new Validator mode transition logger.
   *
   * @param forksSchedule the forks schedule
   * @param msgConsumer the msg consumer
   */
  @VisibleForTesting
  ValidatorModeTransitionLogger(
      final ForksSchedule<QbftConfigOptions> forksSchedule, final Consumer<String> msgConsumer) {
    this.forksSchedule = forksSchedule;
    this.msgConsumer = msgConsumer;
  }

  /**
   * Log transition change.
   *
   * @param parentHeader the parent header
   */
  public void logTransitionChange(final BlockHeader parentHeader) {
    final ForkSpec<QbftConfigOptions> currentForkSpec =
        forksSchedule.getFork(parentHeader.getNumber(), parentHeader.getTimestamp());
    final ForkSpec<QbftConfigOptions> nextForkSpec =
        forksSchedule.getFork(parentHeader.getNumber() + 1L, parentHeader.getTimestamp());

    final QbftConfigOptions currentConfigOptions = currentForkSpec.getValue();
    final QbftConfigOptions nextConfigOptions = nextForkSpec.getValue();

    if (hasChangedConfig(currentConfigOptions, nextConfigOptions)) {
      msgConsumer.accept(
          String.format(
              "Transitioning validator selection mode from %s to %s",
              parseConfigToLog(currentConfigOptions), parseConfigToLog(nextConfigOptions)));
    }
  }

  private boolean hasChangedConfig(
      final QbftConfigOptions currentConfig, final QbftConfigOptions nextConfig) {
    return !currentConfig
        .getValidatorContractAddress()
        .equals(nextConfig.getValidatorContractAddress());
  }

  private String parseConfigToLog(final QbftConfigOptions configOptions) {
    if (configOptions.getValidatorContractAddress().isPresent()) {
      return String.format(
          "contract (address: %s)", configOptions.getValidatorContractAddress().get());
    } else {
      return "blockheader";
    }
  }
}
