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
package org.hyperledger.besu.ethereum.eth.core.transactions;

import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.eth.messages.PooledTransactionsMessage;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.util.List;

public class DevP2PUtils {

  public static PooledTransactionsMessage createPooledTransactionsMessage(
      final List<Transaction> transactions) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.writeList(
        transactions,
        (transaction, rlpOutput) -> {
          TransactionEncoder.encodeRLP(transaction, rlpOutput, EncodingContext.POOLED_TRANSACTION);
        });
    return PooledTransactionsMessage.createUnsafe(out.encoded());
  }
}
