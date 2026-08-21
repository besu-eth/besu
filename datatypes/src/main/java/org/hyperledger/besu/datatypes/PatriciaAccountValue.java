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
package org.hyperledger.besu.datatypes;

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Objects;

import org.apache.tuweni.bytes.Bytes32;

/** The raw values associated with an account in the world state Patricia Merkle trie. */
public class PatriciaAccountValue implements AccountValue {

  private final long nonce;
  private final Wei balance;
  private final Hash storageRoot;
  private final Hash codeHash;

  /**
   * Instantiates a new PatriciaAccountValue.
   *
   * @param nonce the account nonce.
   * @param balance the account balance.
   * @param storageRoot the hash of the root node of the storage trie.
   * @param codeHash the hash of the account code.
   */
  public PatriciaAccountValue(
      final long nonce, final Wei balance, final Hash storageRoot, final Hash codeHash) {
    checkNotNull(balance, "balance cannot be null");
    checkNotNull(storageRoot, "storageRoot cannot be null");
    checkNotNull(codeHash, "codeHash cannot be null");
    this.nonce = nonce;
    this.balance = balance;
    this.storageRoot = storageRoot;
    this.codeHash = codeHash;
  }

  @Override
  public long getNonce() {
    return nonce;
  }

  @Override
  public Wei getBalance() {
    return balance;
  }

  /**
   * The hash of the root of the storage trie associated with this account.
   *
   * @return the hash of the root node of the storage trie.
   */
  public Hash getStorageRoot() {
    return storageRoot;
  }

  @Override
  public Hash getCodeHash() {
    return codeHash;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final PatriciaAccountValue that = (PatriciaAccountValue) o;
    return nonce == that.nonce
        && Objects.equals(balance, that.balance)
        && Objects.equals(storageRoot, that.storageRoot)
        && Objects.equals(codeHash, that.codeHash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nonce, balance, storageRoot, codeHash);
  }

  @Override
  public void writeTo(final RLPOutput out) {
    out.startList();

    out.writeLongScalar(nonce);
    out.writeUInt256Scalar(balance);
    out.writeBytes(storageRoot.getBytes());
    out.writeBytes(codeHash.getBytes());
    out.endList();
  }

  /**
   * Reads the account value from the provided RLP input.
   *
   * @param in the input from which to decode the account value.
   * @return the read account value.
   */
  public static PatriciaAccountValue readFrom(final RLPInput in) {
    in.enterList();

    final long nonce = in.readLongScalar();
    final Wei balance = Wei.of(in.readUInt256Scalar());
    Bytes32 storageRoot;
    Bytes32 codeHash;
    if (in.nextIsNull()) {
      storageRoot = Bytes32.wrap(Hash.EMPTY_TRIE_HASH.getBytes());
      in.skipNext();
    } else {
      storageRoot = in.readBytes32();
    }
    if (in.nextIsNull()) {
      codeHash = Bytes32.wrap(Hash.EMPTY.getBytes());
      in.skipNext();
    } else {
      codeHash = in.readBytes32();
    }
    in.leaveList();

    return new PatriciaAccountValue(nonce, balance, Hash.wrap(storageRoot), Hash.wrap(codeHash));
  }
}
