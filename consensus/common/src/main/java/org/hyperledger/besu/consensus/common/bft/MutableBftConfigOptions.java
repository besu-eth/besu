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
package org.hyperledger.besu.consensus.common.bft;

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.config.BftConfigOptions;
import org.hyperledger.besu.consensus.common.ForksSchedule;
import org.hyperledger.besu.datatypes.Address;

import java.math.BigInteger;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * A mutable {@link BftConfigOptions} that is used for building config for transitions in the {@link
 * ForksSchedule}*.
 */
public class MutableBftConfigOptions implements BftConfigOptions {
  private long epochLength;
  private int blockPeriodSeconds;
  private int emptyBlockPeriodSeconds;
  private long blockPeriodMilliseconds;
  private int requestTimeoutSeconds;
  private int gossipedHistoryLimit;
  private int messageQueueLimit;
  private int duplicateMessageLimit;
  private int futureMessagesLimit;
  private int futureMessageMaxDistance;
  private Optional<Address> miningBeneficiary;
  private BigInteger blockRewardWei;
  private OptionalLong transactionGasLimit;

  /**
   * Instantiates a new Mutable bft config options.
   *
   * @param bftConfigOptions the bft config options
   */
  public MutableBftConfigOptions(final BftConfigOptions bftConfigOptions) {
    this.epochLength = bftConfigOptions.getEpochLength();
    this.blockPeriodSeconds = bftConfigOptions.getBlockPeriodSeconds();
    this.emptyBlockPeriodSeconds = bftConfigOptions.getEmptyBlockPeriodSeconds();
    this.blockPeriodMilliseconds = bftConfigOptions.getBlockPeriodMilliseconds();
    this.requestTimeoutSeconds = bftConfigOptions.getRequestTimeoutSeconds();
    this.gossipedHistoryLimit = bftConfigOptions.getGossipedHistoryLimit();
    this.messageQueueLimit = bftConfigOptions.getMessageQueueLimit();
    this.duplicateMessageLimit = bftConfigOptions.getMessageQueueLimit();
    this.futureMessagesLimit = bftConfigOptions.getFutureMessagesLimit();
    this.futureMessageMaxDistance = bftConfigOptions.getFutureMessagesMaxDistance();
    this.miningBeneficiary = bftConfigOptions.getMiningBeneficiary();
    this.blockRewardWei = bftConfigOptions.getBlockRewardWei();
    this.transactionGasLimit = bftConfigOptions.getTransactionGasLimit();
  }

  @Override
  public long getEpochLength() {
    return epochLength;
  }

  @Override
  public int getBlockPeriodSeconds() {
    return blockPeriodSeconds;
  }

  @Override
  public int getEmptyBlockPeriodSeconds() {
    return emptyBlockPeriodSeconds;
  }

  @Override
  public long getBlockPeriodMilliseconds() {
    return blockPeriodMilliseconds;
  }

  @Override
  public int getRequestTimeoutSeconds() {
    return requestTimeoutSeconds;
  }

  @Override
  public int getGossipedHistoryLimit() {
    return gossipedHistoryLimit;
  }

  @Override
  public int getMessageQueueLimit() {
    return messageQueueLimit;
  }

  @Override
  public int getDuplicateMessageLimit() {
    return duplicateMessageLimit;
  }

  @Override
  public int getFutureMessagesLimit() {
    return futureMessagesLimit;
  }

  @Override
  public int getFutureMessagesMaxDistance() {
    return futureMessageMaxDistance;
  }

  @Override
  public Optional<Address> getMiningBeneficiary() {
    return miningBeneficiary;
  }

  @Override
  public BigInteger getBlockRewardWei() {
    return blockRewardWei;
  }

  @Override
  public OptionalLong getTransactionGasLimit() {
    return transactionGasLimit;
  }

  /**
   * Sets transaction gas limit.
   *
   * @param transactionGasLimit the transaction gas limit
   */
  public void setTransactionGasLimit(final OptionalLong transactionGasLimit) {
    this.transactionGasLimit = transactionGasLimit;
  }

  @Override
  public Map<String, Object> asMap() {
    return Map.of();
  }

  /**
   * Sets epoch length.
   *
   * @param epochLength the epoch length
   */
  public void setEpochLength(final long epochLength) {
    this.epochLength = epochLength;
  }

  /**
   * Sets block period seconds.
   *
   * @param blockPeriodSeconds the block period seconds
   */
  public void setBlockPeriodSeconds(final int blockPeriodSeconds) {
    this.blockPeriodSeconds = blockPeriodSeconds;
  }

  /**
   * Sets empty block period seconds.
   *
   * @param emptyBlockPeriodSeconds the empty block period seconds
   */
  public void setEmptyBlockPeriodSeconds(final int emptyBlockPeriodSeconds) {
    this.emptyBlockPeriodSeconds = emptyBlockPeriodSeconds;
  }

  /**
   * Sets block period milliseconds. Experimental for test scenarios. Not for use on production
   * systems.
   *
   * @param blockPeriodMilliseconds the block period milliseconds
   */
  public void setBlockPeriodMilliseconds(final long blockPeriodMilliseconds) {
    this.blockPeriodMilliseconds = blockPeriodMilliseconds;
  }

  /**
   * Sets request timeout seconds.
   *
   * @param requestTimeoutSeconds the request timeout seconds
   */
  public void setRequestTimeoutSeconds(final int requestTimeoutSeconds) {
    this.requestTimeoutSeconds = requestTimeoutSeconds;
  }

  /**
   * Sets gossiped history limit.
   *
   * @param gossipedHistoryLimit the gossiped history limit
   */
  public void setGossipedHistoryLimit(final int gossipedHistoryLimit) {
    this.gossipedHistoryLimit = gossipedHistoryLimit;
  }

  /**
   * Sets message queue limit.
   *
   * @param messageQueueLimit the message queue limit
   */
  public void setMessageQueueLimit(final int messageQueueLimit) {
    this.messageQueueLimit = messageQueueLimit;
  }

  /**
   * Sets duplicate message limit.
   *
   * @param duplicateMessageLimit the duplicate message limit
   */
  public void setDuplicateMessageLimit(final int duplicateMessageLimit) {
    this.duplicateMessageLimit = duplicateMessageLimit;
  }

  /**
   * Sets future messages limit.
   *
   * @param futureMessagesLimit the future messages limit
   */
  public void setFutureMessagesLimit(final int futureMessagesLimit) {
    this.futureMessagesLimit = futureMessagesLimit;
  }

  /**
   * Sets future message max distance.
   *
   * @param futureMessageMaxDistance the future message max distance
   */
  public void setFutureMessageMaxDistance(final int futureMessageMaxDistance) {
    this.futureMessageMaxDistance = futureMessageMaxDistance;
  }

  /**
   * Sets mining beneficiary.
   *
   * @param miningBeneficiary the mining beneficiary
   */
  public void setMiningBeneficiary(final Optional<Address> miningBeneficiary) {
    checkNotNull(miningBeneficiary);
    this.miningBeneficiary = miningBeneficiary;
  }

  /**
   * Sets block reward wei.
   *
   * @param blockRewardWei the block reward wei
   */
  public void setBlockRewardWei(final BigInteger blockRewardWei) {
    this.blockRewardWei = blockRewardWei;
  }
}
