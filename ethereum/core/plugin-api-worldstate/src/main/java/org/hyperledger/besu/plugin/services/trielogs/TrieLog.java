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
package org.hyperledger.besu.plugin.services.trielogs;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * An interface for interacting with TrieLog objects, which represent changes to accounts, code, and
 * storage in a blockchain state trie.
 */
public interface TrieLog {

  /**
   * Gets the block hash associated with this TrieLog.
   *
   * @return the block hash
   */
  Hash getBlockHash();

  /**
   * Gets the block number associated with this TrieLog, if available.
   *
   * @return an Optional containing the block number if available, otherwise an empty Optional
   */
  Optional<Long> getBlockNumber();

  /** Freezes the TrieLog to prevent further modifications. */
  void freeze();

  /**
   * Gets a map of addresses to their account value changes.
   *
   * @param <U> the type of LogTuple representing the account value changes
   * @return a map of addresses to their account value changes
   */
  <U extends LogTuple<AccountValue>> Map<Address, U> getAccountChanges();

  /**
   * Gets a map of addresses to their code changes.
   *
   * @param <U> the type of LogTuple representing the code changes
   * @return a map of addresses to their code changes
   */
  <U extends LogTuple<Bytes>> Map<Address, U> getCodeChanges();

  /**
   * Gets a map of addresses to their storage changes.
   *
   * @param <U> the type of LogTuple representing the storage changes
   * @return a map of addresses to their storage changes
   */
  <U extends LogTuple<UInt256>> Map<Address, Map<StorageSlotKey, U>> getStorageChanges();

  /**
   * Gets the storage changes for a specific address.
   *
   * @param address the address to get the storage changes for
   * @param <U> the type of LogTuple representing the storage changes
   * @return a map of storage slot keys to their changes
   */
  <U extends LogTuple<UInt256>> Map<StorageSlotKey, U> getStorageChanges(final Address address);

  /**
   * Gets the prior code for a specific address, if available.
   *
   * @param address the address to get the prior code for
   * @return an Optional containing the prior code if available, otherwise an empty Optional
   */
  Optional<Bytes> getPriorCode(final Address address);

  /**
   * Gets the code for a specific address, if available.
   *
   * @param address the address to get the code for
   * @return an Optional containing the code if available, otherwise an empty Optional
   */
  Optional<Bytes> getCode(final Address address);

  /**
   * Gets the prior storage value for a specific address and storage slot key, if available.
   *
   * @param address the address to get the prior storage value for
   * @param storageSlotKey the storage slot key to get the prior storage value for
   * @return an Optional containing the prior storage value if available, otherwise an empty
   *     Optional
   */
  Optional<UInt256> getPriorStorageByStorageSlotKey(
      final Address address, final StorageSlotKey storageSlotKey);

  /**
   * Gets the storage value for a specific address and storage slot key, if available.
   *
   * @param address the address to get the storage value for
   * @param storageSlotKey the storage slot key to get the storage value for
   * @return an Optional containing the storage value if available, otherwise an empty Optional
   */
  Optional<UInt256> getStorageByStorageSlotKey(
      final Address address, final StorageSlotKey storageSlotKey);

  /**
   * Gets the prior account value for a specific address, if available.
   *
   * @param address the address to get the prior account value for
   * @return an Optional containing the prior account value if available, otherwise an empty
   *     Optional
   */
  Optional<? extends AccountValue> getPriorAccount(final Address address);

  /**
   * Gets the account value for a specific address, if available.
   *
   * @param address the address to get the account value for
   * @return an Optional containing the account value if available, otherwise an empty Optional
   */
  Optional<? extends AccountValue> getAccount(final Address address);

  /**
   * An interface representing a tuple of prior and updated values for a specific type T in a log.
   * The interface also provides methods to check if the values are unchanged or cleared.
   *
   * @param <T> the type of values stored in the log tuple
   */
  public interface LogTuple<T> {

    /**
     * Gets the prior value of the tuple.
     *
     * @return the prior value of type T
     */
    T getPrior();

    /**
     * Gets the updated value of the tuple.
     *
     * @return the updated value of type T
     */
    T getUpdated();

    /**
     * Checks if the prior and updated values are equal.
     *
     * @return true if the prior and updated values are equal, false otherwise
     */
    default boolean isUnchanged() {
      return Objects.equals(getUpdated(), getPrior());
    }

    /**
     * Checks if the last step performed a 'clear'.
     *
     * @return true if the last step performed a 'clear', false otherwise.
     */
    boolean isLastStepCleared();

    /**
     * Checks if a 'clear' has been performed at least once.
     *
     * @return true if a 'clear' has been performed at least once, false otherwise.
     */
    boolean isClearedAtLeastOnce();
  }
}
