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
package org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum SubscriptionType {
  @JsonProperty("newHeads")
  NEW_BLOCK_HEADERS("newHeads"),

  @JsonProperty("logs")
  LOGS("logs"),

  @JsonProperty("newPendingTransactions")
  NEW_PENDING_TRANSACTIONS("newPendingTransactions"),

  @JsonProperty("droppedPendingTransactions")
  DROPPED_PENDING_TRANSACTIONS("droppedPendingTransactions"),

  @JsonProperty("syncing")
  SYNCING("syncing"),

  @JsonProperty("transactionReceipts")
  TRANSACTION_RECEIPTS("transactionReceipts");

  private final String code;

  SubscriptionType(final String code) {
    this.code = code;
  }

  public String getCode() {
    return code;
  }

  public static SubscriptionType fromCode(final String code) {
    for (final SubscriptionType subscriptionType : SubscriptionType.values()) {
      if (code.equals(subscriptionType.getCode())) {
        return subscriptionType;
      }
    }

    throw new IllegalArgumentException(String.format("Invalid subscription type '%s'", code));
  }
}
