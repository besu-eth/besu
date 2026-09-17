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
package org.hyperledger.besu.ethereum.chain;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Objects;

/**
 * Specifies the location of a transaction within the blockchain (where the transaction was included
 * in the block and location within this block's transactions list).
 */
public class TransactionLocation {

  private final Hash blockHash;
  private final int transactionIndex;

  public TransactionLocation(final Hash blockHash, final int transactionIndex) {
    this.blockHash = blockHash;
    this.transactionIndex = transactionIndex;
  }

  public Hash getBlockHash() {
    return blockHash;
  }

  public int getTransactionIndex() {
    return transactionIndex;
  }

  public void writeTo(final RLPOutput out) {
    out.startList();

    out.writeBytes(blockHash.getBytes());
    out.writeIntScalar(transactionIndex);

    out.endList();
  }

  public static TransactionLocation readFrom(final RLPInput input) {
    input.enterList();
    final TransactionLocation txLocation =
        new TransactionLocation(Hash.wrap(input.readBytes32()), input.readIntScalar());
    input.leaveList();
    return txLocation;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof TransactionLocation other)) {
      return false;
    }
    return getTransactionIndex() == other.getTransactionIndex()
        && getBlockHash().equals(other.getBlockHash());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getBlockHash(), getTransactionIndex());
  }
}
