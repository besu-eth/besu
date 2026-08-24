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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters;

import org.hyperledger.besu.ethereum.core.Withdrawal;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.tuweni.units.bigints.UInt64;

public final class PayloadAttributesV4 extends PayloadAttributesV3 {

  private final Long slotNumber;
  private final Long targetGasLimit;

  @JsonCreator
  public PayloadAttributesV4(
      @JsonProperty("timestamp") final String timestamp,
      @JsonProperty("prevRandao") final String prevRandao,
      @JsonProperty("suggestedFeeRecipient") final String suggestedFeeRecipient,
      @JsonProperty("withdrawals") final List<Withdrawal> withdrawals,
      @JsonProperty("parentBeaconBlockRoot") final String parentBeaconBlockRoot,
      @JsonProperty("slotNumber") final String slotNumber,
      @JsonProperty("targetGasLimit") final String targetGasLimit) {
    super(timestamp, prevRandao, suggestedFeeRecipient, withdrawals, parentBeaconBlockRoot);
    this.slotNumber = parseSlotNumber(slotNumber);
    this.targetGasLimit = targetGasLimit != null ? Long.decode(targetGasLimit) : null;
  }

  /**
   * Parses the uint64 QUANTITY {@code slotNumber}, matching how {@code engine_newPayload} reads the
   * same field: the whole range is accepted, with the top half carried as a negative long ({@link
   * Long#decode} would throw above {@link Long#MAX_VALUE}). A missing or malformed value yields
   * {@code null}, which the caller reports as the more specific "Invalid slotNumber".
   */
  private static Long parseSlotNumber(final String slotNumber) {
    if (slotNumber == null) {
      return null;
    }
    try {
      return UInt64.fromHexString(slotNumber).toBytes().toLong();
    } catch (final IllegalArgumentException e) {
      return null;
    }
  }

  public Long getSlotNumber() {
    return slotNumber;
  }

  public Long getTargetGasLimit() {
    return targetGasLimit;
  }
}
