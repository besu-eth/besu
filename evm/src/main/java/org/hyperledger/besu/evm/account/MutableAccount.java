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

import org.hyperledger.besu.datatypes.Wei;

import java.util.Map;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/** A mutable world state account. */
public interface MutableAccount extends Account {

  /**
   * Increments (by 1) the nonce of this account.
   *
   * @return the previous value of the nonce.
   */
  default long incrementNonce() {
    final long current = getNonce();
    setNonce(current + 1);
    return current;
  }

  /**
   * Sets the nonce of this account to the provide value.
   *
   * @param value the value to set the nonce to.
   */
  void setNonce(long value);

  /**
   * Increments the account balance by the provided amount.
   *
   * @param value The amount to increment
   * @return the previous balance (before increment).
   */
  default Wei incrementBalance(final Wei value) {
    final Wei current = getBalance();
    setBalance(current.add(value));
    return current;
  }

  /**
   * Decrements the account balance by the provided amount.
   *
   * @param value The amount to decrement
   * @return the previous balance (before decrement). The account must have enough funds or an
   *     exception is thrown.
   * @throws IllegalStateException if the account balance is strictly less than {@code value}.
   */
  default Wei decrementBalance(final Wei value) {
    final Wei current = getBalance();
    if (current.compareTo(value) < 0) {
      throw new IllegalStateException(
          String.format("Cannot remove %s wei from account, balance is only %s", value, current));
    }
    setBalance(current.subtract(value));
    return current;
  }

  /**
   * Sets the balance of the account to the provided amount.
   *
   * @param value the amount to set.
   */
  void setBalance(Wei value);

  /**
   * Sets the code for the account.
   *
   * @param code the code to set for the account.
   */
  void setCode(Bytes code);

  /**
   * Sets a particular key-value pair in the account storage.
   *
   * <p>Note that setting the value of an entry to 0 is basically equivalent to deleting that entry.
   *
   * @param key the key to set.
   * @param value the value to set {@code key} to.
   */
  void setStorageValue(UInt256 key, UInt256 value);

  /** Clears out an account's storage. */
  void clearStorage();

  /**
   * Returns the storage entries that have been set through the updater this instance came from.
   *
   * @return a map of storage that has been modified.
   */
  Map<UInt256, UInt256> getUpdatedStorage();

  /**
   * Make this instance immutable. Used for private world state interactions with public contracts.
   */
  void becomeImmutable();
}
