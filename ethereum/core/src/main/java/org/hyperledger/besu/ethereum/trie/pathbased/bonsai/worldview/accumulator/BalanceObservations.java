/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** The balances a speculatively executed transaction depends on. */
public final class BalanceObservations {

  private final Set<Address> observed = new HashSet<>();
  // how far the balances it only spends from may be lower
  private final Map<Address, Wei> margins = new HashMap<>();

  void observe(final Address address) {
    observed.add(address);
  }

  void observeSufficient(final Address address, final Wei margin) {
    margins.merge(address, margin, (a, b) -> a.compareTo(b) <= 0 ? a : b);
  }

  /**
   * Whether the transaction depends on the balance of an account.
   *
   * @param address the account
   * @return true if the transaction read the balance or tried to spend more than it holds
   */
  public boolean isObserved(final Address address) {
    return observed.contains(address);
  }

  /**
   * How far below its starting value the balance of an account the transaction only spends from may
   * be without changing the transaction's outcome.
   *
   * @param address the account
   * @return the smallest margin of its spendings, or empty if the transaction did not spend from it
   */
  public Optional<Wei> margin(final Address address) {
    return Optional.ofNullable(margins.get(address));
  }
}
