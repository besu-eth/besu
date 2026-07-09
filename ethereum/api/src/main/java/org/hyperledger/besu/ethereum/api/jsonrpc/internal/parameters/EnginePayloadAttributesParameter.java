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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters;

import org.hyperledger.besu.datatypes.Address;

import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.vertx.core.json.JsonObject;
import org.apache.tuweni.bytes.Bytes32;

public class EnginePayloadAttributesParameter {

  final Long timestamp;
  final Bytes32 prevRandao;
  final Address suggestedFeeRecipient;
  final List<WithdrawalParameter> withdrawals;
  private final Bytes32 parentBeaconBlockRoot;
  private final Long slotNumber;
  private final Long targetGasLimit;

  @JsonCreator
  public EnginePayloadAttributesParameter(
      @JsonProperty("timestamp") final String timestamp,
      @JsonProperty("prevRandao") final String prevRandao,
      @JsonProperty("suggestedFeeRecipient") final String suggestedFeeRecipient,
      @JsonProperty("withdrawals") final List<WithdrawalParameter> withdrawals,
      @JsonProperty("parentBeaconBlockRoot") final String parentBeaconBlockRoot,
      @JsonProperty("slotNumber") final String slotNumber,
      @JsonProperty("targetGasLimit") final String targetGasLimit) {
    this.timestamp = Long.decode(timestamp);
    this.prevRandao = Bytes32.fromHexString(prevRandao);
    this.suggestedFeeRecipient = Address.fromHexString(suggestedFeeRecipient);
    this.withdrawals = withdrawals;
    this.parentBeaconBlockRoot =
        parentBeaconBlockRoot == null ? null : Bytes32.fromHexString(parentBeaconBlockRoot);
    this.slotNumber = slotNumber == null ? null : Long.decode(slotNumber);
    this.targetGasLimit = targetGasLimit == null ? null : Long.decode(targetGasLimit);
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public Bytes32 getPrevRandao() {
    return prevRandao;
  }

  public Address getSuggestedFeeRecipient() {
    return suggestedFeeRecipient;
  }

  public Bytes32 getParentBeaconBlockRoot() {
    return parentBeaconBlockRoot;
  }

  public List<WithdrawalParameter> getWithdrawals() {
    return withdrawals;
  }

  public Long getSlotNumber() {
    return slotNumber;
  }

  public Long getTargetGasLimit() {
    return targetGasLimit;
  }

  public String serialize() {
    final JsonObject json =
        new JsonObject()
            .put("timestamp", timestamp)
            .put("prevRandao", prevRandao.toHexString())
            .put("suggestedFeeRecipient", suggestedFeeRecipient.getBytes().toHexString());
    if (withdrawals != null) {
      json.put(
          "withdrawals",
          withdrawals.stream().map(WithdrawalParameter::asJsonObject).collect(Collectors.toList()));
    }
    if (parentBeaconBlockRoot != null) {
      json.put("parentBeaconBlockRoot", parentBeaconBlockRoot.toHexString());
    }
    if (slotNumber != null) {
      json.put("slotNumber", slotNumber);
    }
    if (targetGasLimit != null) {
      json.put("targetGasLimit", targetGasLimit);
    }
    return json.encode();
  }
}
