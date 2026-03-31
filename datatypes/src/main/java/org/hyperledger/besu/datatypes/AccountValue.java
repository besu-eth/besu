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

import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Optional;

/** The values of an account in the world state trie. */
public interface AccountValue {
  /**
   * The nonce of the account.
   *
   * @return the nonce of the account.
   */
  long getNonce();

  /**
   * The available balance of that account.
   *
   * @return the balance, in Wei, of the account.
   */
  Wei getBalance();

  /**
   * The hash of the root of the storage trie associated with this account.
   *
   * @return the hash of the root node of the storage trie.
   */
  Hash getStorageRoot();

  /**
   * The hash of the EVM bytecode associated with this account.
   *
   * @return the hash of the account code (which may be {@link Hash#EMPTY}).
   */
  Hash getCodeHash();

  /**
   * The size of the EVM bytecode associated with this account.
   *
   * @return the size of the account code (which may be {@link Optional#empty()}).
   */
  Optional<Long> getCodeSize();

  /**
   * Writes the account value to the provided {@link RLPOutput}.
   *
   * @param out the {@link RLPOutput} to write to.
   */
  void writeTo(final RLPOutput out);
}
