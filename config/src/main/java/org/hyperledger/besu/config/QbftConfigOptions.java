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

import java.util.Optional;
import java.util.OptionalLong;

/** The interface Qbft config options. */
public interface QbftConfigOptions extends BftConfigOptions {

  /**
   * Gets validator contract address.
   *
   * @return the validator contract address
   */
  Optional<String> getValidatorContractAddress();

  /**
   * Is validator contract mode.
   *
   * @return true of validator contract mode, false otherwise.
   */
  default boolean isValidatorContractMode() {
    return getValidatorContractAddress().isPresent();
  }

  /**
   * Gets start block.
   *
   * @return the start block
   */
  OptionalLong getStartBlock();

  /**
   * Whether GoQuorum compatibility mode is enabled. When enabled, the block gas-limit range and
   * delta validation is skipped so that chains produced by GoQuorum's QBFT (which permits arbitrary
   * per-block gas-limit changes) can be imported.
   *
   * @return true if goQuorum compatibility mode is enabled, false otherwise.
   */
  default boolean isGoQuorumCompatibilityMode() {
    return false;
  }
}
