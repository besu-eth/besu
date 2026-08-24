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
import org.hyperledger.besu.datatypes.StorageSlotKey;

import java.util.Map;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/** Accumulator interface for providing trie updates for creating TrieLogs. */
public interface TrieLogAccumulator {

  /**
   * Returns the state trie accounts which have been updated.
   *
   * @return the accounts to update
   */
  Map<Address, ? extends TrieLog.LogTuple<? extends AccountValue>> getAccountsToUpdate();

  /**
   * Returns code which has been updated.
   *
   * @return the code to update
   */
  Map<Address, ? extends TrieLog.LogTuple<Bytes>> getCodeToUpdate();

  /**
   * Returns storage which has been updated.
   *
   * @return the storage to update
   */
  Map<Address, ? extends Map<StorageSlotKey, ? extends TrieLog.LogTuple<UInt256>>>
      getStorageToUpdate();
}
