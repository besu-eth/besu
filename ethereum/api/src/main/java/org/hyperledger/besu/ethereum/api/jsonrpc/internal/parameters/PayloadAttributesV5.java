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

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.tuweni.bytes.Bytes;

public final class PayloadAttributesV5 extends PayloadAttributesV4 {

  private final List<Bytes> inclusionListTransactions;

  @JsonCreator
  public PayloadAttributesV5(
      @JsonProperty("timestamp") final String timestamp,
      @JsonProperty("prevRandao") final String prevRandao,
      @JsonProperty("suggestedFeeRecipient") final String suggestedFeeRecipient,
      @JsonProperty("withdrawals") final List<Withdrawal> withdrawals,
      @JsonProperty("parentBeaconBlockRoot") final String parentBeaconBlockRoot,
      @JsonProperty("slotNumber") final String slotNumber,
      @JsonProperty("targetGasLimit") final String targetGasLimit,
      @JsonProperty("inclusionListTransactions") final List<String> inclusionListTransactions) {
    super(
        timestamp,
        prevRandao,
        suggestedFeeRecipient,
        withdrawals,
        parentBeaconBlockRoot,
        slotNumber,
        targetGasLimit);
    this.inclusionListTransactions = parseInclusionListTransactions(inclusionListTransactions);
  }

  private static List<Bytes> parseInclusionListTransactions(final List<String> hexTransactions) {
    if (hexTransactions == null) {
      return null;
    }

    final List<Bytes> txBytes = new ArrayList<>(hexTransactions.size());
    for (final String hexTransaction : hexTransactions) {
      if (hexTransaction == null || hexTransaction.isEmpty()) {
        throw new IllegalArgumentException("Inclusion list transaction cannot be null or empty");
      }
      try {
        txBytes.add(Bytes.fromHexString(hexTransaction));
      } catch (final IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid inclusion list transaction format", e);
      }
    }
    return txBytes;
  }

  public List<Bytes> getInclusionListTransactions() {
    return inclusionListTransactions;
  }
}
