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
package org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.transactionreceipts;

import org.hyperledger.besu.datatypes.Hash;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TransactionReceiptsFilterParameter {

  private static final int MAX_TRANSACTION_HASHES = 200;
  private final List<Hash> transactionHashes;

  @JsonCreator
  public TransactionReceiptsFilterParameter(
      @JsonProperty("transactionHashes") final List<Hash> transactionHashes) {
    if (transactionHashes != null && transactionHashes.size() > MAX_TRANSACTION_HASHES) {
      throw new IllegalArgumentException(
          "transactionHashes filter limited to " + MAX_TRANSACTION_HASHES + " entries");
    }
    this.transactionHashes =
        transactionHashes == null ? Collections.emptyList() : transactionHashes;
  }

  public List<Hash> getTransactionHashes() {
    return transactionHashes;
  }
}
