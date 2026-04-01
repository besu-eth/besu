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
package org.hyperledger.besu.ethereum.eth;

import org.hyperledger.besu.util.number.ByteUnits;

import org.immutables.value.Value;

@Value.Immutable
public interface EthProtocolConfiguration {
  int DEFAULT_MAX_MESSAGE_SIZE = 10 * ByteUnits.MEGABYTE;
  int DEFAULT_MAX_TRANSACTIONS_MESSAGE_SIZE = ByteUnits.MEGABYTE;
  int DEFAULT_MAX_GET_BLOCK_HEADERS = 512;
  int DEFAULT_MAX_GET_BLOCK_BODIES = 128;
  int DEFAULT_MAX_GET_BLOCK_ACCESS_LISTS = 128;
  int DEFAULT_MAX_GET_RECEIPTS = 256;
  int DEFAULT_MAX_GET_NODE_DATA = 384;
  int DEFAULT_MAX_GET_POOLED_TRANSACTIONS = 256;
  int DEFAULT_MAX_CAPABILITY = Integer.MAX_VALUE;
  int DEFAULT_MIN_CAPABILITY = 0;

  EthProtocolConfiguration DEFAULT = ImmutableEthProtocolConfiguration.builder().build();

  @Value.Default
  default int getMaxMessageSize() {
    return DEFAULT_MAX_MESSAGE_SIZE;
  }

  @Value.Default
  default int getMaxTransactionsMessageSize() {
    return DEFAULT_MAX_TRANSACTIONS_MESSAGE_SIZE;
  }

  @Value.Default
  default int getMaxGetBlockHeaders() {
    return DEFAULT_MAX_GET_BLOCK_HEADERS;
  }

  @Value.Default
  default int getMaxGetBlockBodies() {
    return DEFAULT_MAX_GET_BLOCK_BODIES;
  }

  @Value.Default
  default int getMaxGetBlockAccessLists() {
    return DEFAULT_MAX_GET_BLOCK_ACCESS_LISTS;
  }

  @Value.Default
  default int getMaxGetReceipts() {
    return DEFAULT_MAX_GET_RECEIPTS;
  }

  @Value.Default
  default int getMaxGetNodeData() {
    return DEFAULT_MAX_GET_NODE_DATA;
  }

  @Value.Default
  default int getMaxGetPooledTransactions() {
    return DEFAULT_MAX_GET_POOLED_TRANSACTIONS;
  }

  @Value.Default
  default int getMaxEthCapability() {
    return DEFAULT_MAX_CAPABILITY;
  }

  @Value.Default
  default int getMinEthCapability() {
    return DEFAULT_MIN_CAPABILITY;
  }
}
