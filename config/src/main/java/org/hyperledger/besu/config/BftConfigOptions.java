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

import java.math.BigInteger;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/** The interface Bft config options. */
public interface BftConfigOptions {

  /**
   * The number of blocks in an epoch.
   *
   * @return the epoch length
   */
  long getEpochLength();

  /**
   * Gets block period seconds.
   *
   * @return the block period seconds
   */
  int getBlockPeriodSeconds();

  /**
   * Gets empty block period seconds.
   *
   * @return the empty block period seconds
   */
  int getEmptyBlockPeriodSeconds();

  /**
   * Gets block period milliseconds. For TESTING only. If set then blockperiodseconds is ignored.
   *
   * @return the block period milliseconds
   */
  long getBlockPeriodMilliseconds();

  /**
   * Gets request timeout seconds.
   *
   * @return the request timeout seconds
   */
  int getRequestTimeoutSeconds();

  /**
   * Gets gossiped history limit.
   *
   * @return the gossiped history limit
   */
  int getGossipedHistoryLimit();

  /**
   * Gets message queue limit.
   *
   * @return the message queue limit
   */
  int getMessageQueueLimit();

  /**
   * Gets duplicate message limit.
   *
   * @return the duplicate message limit
   */
  int getDuplicateMessageLimit();

  /**
   * Gets future messages limit.
   *
   * @return the future messages limit
   */
  int getFutureMessagesLimit();

  /**
   * Gets future messages max distance.
   *
   * @return the future messages max distance
   */
  int getFutureMessagesMaxDistance();

  /**
   * Gets mining beneficiary.
   *
   * @return the mining beneficiary
   */
  Optional<Address> getMiningBeneficiary();

  /**
   * Gets block reward wei.
   *
   * @return the block reward wei
   */
  BigInteger getBlockRewardWei();

  /**
   * Gets the per-transaction gas limit override.
   *
   * @return the per-transaction gas limit override, or empty if not configured
   */
  default OptionalLong getTransactionGasLimit() {
    return OptionalLong.empty();
  }

  /**
   * As map.
   *
   * @return the map
   */
  Map<String, Object> asMap();
}
