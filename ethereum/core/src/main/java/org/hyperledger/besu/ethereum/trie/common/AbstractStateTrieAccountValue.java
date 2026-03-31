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

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Optional;

/** Represents the raw values associated with an account in the world state trie. */
public abstract class AbstractStateTrieAccountValue implements AccountValue {

  protected final long nonce;
  protected final Wei balance;
  protected final Hash codeHash;

  public AbstractStateTrieAccountValue(final long nonce, final Wei balance, final Hash codeHash) {
    checkNotNull(balance, "balance cannot be null");
    checkNotNull(codeHash, "codeHash cannot be null");
    this.nonce = nonce;
    this.balance = balance;
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
   * The hash of the EVM bytecode associated with this account.
   *
   * @return the hash of the account code (which may be {@link Hash#EMPTY}).
   */
  @Override
  public Hash getCodeHash() {
    return codeHash;
  }

  /**
   * The hash of the root of the storage trie associated with this account.
   *
   * @return the hash of the root node of the storage trie.
   */
  @Override
  public Hash getStorageRoot() {
    return Hash.EMPTY_TRIE_HASH;
  }

  /**
   * The size of the EVM bytecode associated with this account.
   *
   * @return the size of the account code (which may be empty).
   */
  @Override
  public Optional<Long> getCodeSize() {
    return Optional.empty();
  }

  @Override
  public abstract void writeTo(final RLPOutput out);
}
