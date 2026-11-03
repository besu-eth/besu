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

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Objects;

import org.apache.tuweni.bytes.Bytes32;

/** Represents the raw values associated with an account in the world state patricia merkle trie. */
public class PatriciaTrieAccountValue implements TrieAccountValue {

  protected final long nonce;
  protected final Wei balance;
  protected final Hash storageRoot;
  protected final Hash codeHash;

  public PatriciaTrieAccountValue(
      final long nonce, final Wei balance, final Hash storageRoot, final Hash codeHash) {
    checkNotNull(balance, "balance cannot be null");
    checkNotNull(storageRoot, "storageRoot cannot be null");
    checkNotNull(codeHash, "codeHash cannot be null");
    this.nonce = nonce;
    this.balance = balance;
    this.storageRoot = storageRoot;
    this.codeHash = codeHash;
  }

  /**
   * The account nonce, that is the number of transactions sent from that account.
   *
   * @return the account nonce.
   */
  @Override
  public long getNonce() {
    return nonce;
  }

  /**
   * The available balance of that account.
   *
   * @return the balance, in Wei, of the account.
   */
  @Override
  public Wei getBalance() {
    return balance;
  }

  /**
   * The hash of the root of the storage trie associated with this account.
   *
   * @return the hash of the root node of the storage trie.
   */
  @Override
  public Hash getStorageRoot() {
    return storageRoot;
  }

  /**
   * The hash of the EVM bytecode associated with this account.
   *
   * @return the hash of the account code (which may be {@link Hash#EMPTY}).
   */
  @Override
  public Hash getCodeHash() {
    return codeHash;
  }

  @Override
  public StorageRootStrategy storageRootStrategy() {
    return new MptStorageRootStrategy(storageRoot);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PatriciaTrieAccountValue that = (PatriciaTrieAccountValue) o;
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

  public static PatriciaTrieAccountValue readFrom(final RLPInput in) {
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

    return new PatriciaTrieAccountValue(
        nonce, balance, Hash.wrap(storageRoot), Hash.wrap(codeHash));
  }
}
