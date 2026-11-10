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
package org.hyperledger.besu.evm.account;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;

import java.util.NavigableMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * An account state.
 *
 * <p>An account has four properties associated with it:
 *
 * <ul>
 *   <li><b>Nonce:</b> the number of transactions sent (and committed) from the account.
 *   <li><b>Balance:</b> the amount of Wei (10^18 Ether) owned by the account.
 *   <li><b>Storage:</b> a key-value mapping between 256-bit integer values. Note that formally, a
 *       key always has an associated value in that mapping, with 0 being the default (and thus, in
 *       practice, only non-zero mappings are stored and setting a key to the value 0 is akin to
 *       "removing" that key).
 *   <li><b>Code:</b> arbitrary-length sequence of bytes that corresponds to EVM bytecode.
 *   <li><b>Version:</b> the version of the EVM bytecode.
 * </ul>
 */
public interface AccountState {

  /**
   * The Keccak-256 hash of the account address.
   *
   * <p>Note that the world state does not store account addresses, only their hashes, and so this
   * is how account are truly identified. So while accounts can be queried by their address (through
   * first computing their hash), one cannot infer the address from an account simply from the world
   * state.
   *
   * @return the Keccak-256 hash of the account address.
   */
  Hash getAddressHash();

  /**
   * The account nonce, that is the number of transactions sent from that account.
   *
   * @return the account nonce.
   */
  long getNonce();

  /**
   * The available balance of that account.
   *
   * @return the balance, in Wei, of the account.
   */
  Wei getBalance();

  /**
   * The EVM bytecode associated with this account.
   *
   * @return the account code (which can be empty).
   */
  Bytes getCode();

  /**
   * The EVM bytecode associated with this account, wrapped in a {@link Code} object.
   *
   * <p>This is the default implementation that returns a {@link Code} object wrapping the {@link
   * #getCode()} result. It can be overridden to provide a different implementation of {@link Code}.
   *
   * @return the account code wrapped in a {@link Code} object.
   */
  default Code getOrCreateCachedCode() {
    return new Code(this.getCode());
  }

  /**
   * The hash of the EVM bytecode associated with this account.
   *
   * @return the hash of the account code (which may be {@link Hash#EMPTY}).
   */
  Hash getCodeHash();

  /**
   * Whether the account has (non empty) EVM bytecode associated to it.
   *
   * <p>This is functionally equivalent to {@code !getCode().isEmpty()}, but only reads the code
   * hash, so it does not load the code.
   *
   * @return Whether the account has EVM bytecode associated to it.
   */
  default boolean hasCode() {
    return !Hash.EMPTY.equals(getCodeHash());
  }

  /**
   * Retrieves a value in the account storage given its key.
   *
   * @param key the key to retrieve in the account storage.
   * @return the value associated to {@code key} in the account storage. Note that this is never
   *     {@code null}, but 0 acts as a default value.
   */
  UInt256 getStorageValue(UInt256 key);

  /**
   * Retrieves the original value from before the current transaction in the account storage given
   * its key.
   *
   * @param key the key to retrieve in the account storage.
   * @return the original value associated to {@code key} in the account storage. Note that this is
   *     never {@code null}, but 0 acts as a default value.
   */
  UInt256 getOriginalStorageValue(UInt256 key);

  /**
   * Whether the account is "empty".
   *
   * <p>An account is defined as empty if:
   *
   * <ul>
   *   <li>its nonce is 0;
   *   <li>its balance is 0;
   *   <li>its associated code is empty (the zero-length byte sequence).
   * </ul>
   *
   * @return {@code true} if the account is empty with regard to the definition above, {@code false}
   *     otherwise.
   */
  default boolean isEmpty() {
    return getNonce() == 0 && getBalance().isZero() && !hasCode();
  }

  /**
   * Retrieve up to {@code limit} storage entries beginning from the first entry with hash equal to
   * or greater than {@code startKeyHash}.
   *
   * @param startKeyHash the first key hash to return.
   * @param limit the maximum number of entries to return.
   * @return the requested storage entries as a map of key hash to entry.
   */
  NavigableMap<Bytes32, AccountStorageEntry> storageEntriesFrom(Bytes32 startKeyHash, int limit);
}
