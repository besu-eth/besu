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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonGetter;

public class TransactionPendingResult extends TransactionBaseResult {

  private final String publicKey;
  private final String raw;

  public TransactionPendingResult(final PendingTransaction pendingTransaction) {
    this(pendingTransaction.getTransaction());
  }

  public TransactionPendingResult(final Transaction transaction) {
    super(transaction, Optional.empty());
    this.publicKey = transaction.getPublicKey().orElse(null);
    this.raw =
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.POOLED_TRANSACTION)
            .toString();
  }

  @JsonGetter(value = "publicKey")
  public String getPublicKey() {
    return publicKey;
  }

  @JsonGetter(value = "raw")
  public String getRaw() {
    return raw;
  }
}
