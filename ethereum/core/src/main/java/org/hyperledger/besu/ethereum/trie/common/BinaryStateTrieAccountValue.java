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
package org.hyperledger.besu.ethereum.trie.common;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Objects;
import java.util.Optional;

/**
 * Represents the raw values associated with an account in the world state Binary Trie. Unlike PMT
 * accounts, Binary Trie accounts include code size but do not have a storage root.
 */
public class BinaryStateTrieAccountValue extends AbstractStateTrieAccountValue
    implements AccountValue {

  protected final Optional<Long> codeSize;

  public BinaryStateTrieAccountValue(
      final long nonce, final Wei balance, final Hash codeHash, final Optional<Long> codeSize) {
    super(nonce, balance, codeHash);
    this.codeSize = codeSize;
  }

  public BinaryStateTrieAccountValue(
      final long nonce, final Wei balance, final Hash codeHash, final long codeSize) {
    this(nonce, balance, codeHash, Optional.of(codeSize));
  }

  @Override
  public Hash getStorageRoot() {
    return Hash.EMPTY;
  }

  @Override
  public Optional<Long> getCodeSize() {
    return codeSize;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BinaryStateTrieAccountValue that = (BinaryStateTrieAccountValue) o;
    return nonce == that.nonce
        && Objects.equals(balance, that.balance)
        && Objects.equals(codeHash, that.codeHash)
        && Objects.equals(codeSize, that.codeSize);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nonce, balance, codeHash, codeSize);
  }

  @Override
  public void writeTo(final RLPOutput out) {
    out.startList();
    out.writeLongScalar(nonce);
    out.writeUInt256Scalar(balance);
    out.writeBytes(codeHash.getBytes());
    codeSize.ifPresent(out::writeLongScalar);
    out.endList();
  }

  public static BinaryStateTrieAccountValue readFrom(final RLPInput in) {
    in.enterList();
    final long nonce = in.readLongScalar();
    final Wei balance = Wei.of(in.readUInt256Scalar());
    Hash codeHash;
    final Optional<Long> codeSize;
    if (in.nextIsNull()) {
      codeHash = Hash.EMPTY;
      in.skipNext();
    } else {
      codeHash = Hash.wrap(in.readBytes32());
    }
    if (in.nextIsNull()) {
      codeSize = Optional.empty();
      in.skipNext();
    } else {
      codeSize = Optional.of(in.readLongScalar());
    }
    in.leaveList();

    return new BinaryStateTrieAccountValue(nonce, balance, codeHash, codeSize);
  }
}
