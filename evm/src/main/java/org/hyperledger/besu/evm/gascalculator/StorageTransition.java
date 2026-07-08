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
package org.hyperledger.besu.evm.gascalculator;

import java.util.function.Supplier;

import org.apache.tuweni.units.bigints.UInt256;

/**
 * The three booleans that determine SSTORE state-gas treatment under EIP-8037: the slot's value at
 * tx-entry (original), at the SSTORE site (current), and the new value being written.
 *
 * @param originalIsZero whether the slot was zero at transaction entry
 * @param currentIsZero whether the slot is zero at the SSTORE site
 * @param newIsZero whether the new value being written is zero
 */
public record StorageTransition(boolean originalIsZero, boolean currentIsZero, boolean newIsZero) {

  /**
   * Materialises the three booleans, invoking the suppliers lazily so the caller can pass memoised
   * accessors for {@code originalValue} / {@code currentValue}.
   *
   * @param newValue the new value being written
   * @param currentValue supplier for the slot's current value
   * @param originalValue supplier for the slot's transaction-entry value
   * @return a transition descriptor
   */
  public static StorageTransition of(
      final UInt256 newValue,
      final Supplier<UInt256> currentValue,
      final Supplier<UInt256> originalValue) {
    return new StorageTransition(
        originalValue.get().isZero(), currentValue.get().isZero(), newValue.isZero());
  }

  /**
   * SSTORE_SET (0 → 0 → X): the only transition that consumes storage-set state gas.
   *
   * @return true if this transition matches the storage-set pattern
   */
  public boolean isStorageSet() {
    return originalIsZero && currentIsZero && !newIsZero;
  }

  /**
   * In-tx unwind (0 → X → 0): the only transition that refunds storage-set state gas.
   *
   * @return true if this transition matches the in-tx unwind pattern
   */
  public boolean isUnwoundSet() {
    return originalIsZero && !currentIsZero && newIsZero;
  }
}
