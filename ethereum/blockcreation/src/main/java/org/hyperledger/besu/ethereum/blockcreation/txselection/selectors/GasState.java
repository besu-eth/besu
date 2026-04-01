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
package org.hyperledger.besu.ethereum.blockcreation.txselection.selectors;

/**
 * Tracks cumulative regular gas and state gas used during block building. Used by {@link
 * BlockSizeTransactionSelector} for multidimensional gas metering (EIP-8037).
 *
 * <p>For pre-EIP-8037, stateGas is always 0.
 *
 * @param regularGas cumulative regular (non-state) gas used
 * @param stateGas cumulative state gas used
 */
record GasState(long regularGas, long stateGas) {

  static final GasState ZERO = new GasState(0, 0);

  /**
   * Duplicator function for {@link
   * org.hyperledger.besu.plugin.services.txselection.SelectorsStateManager}. Since records are
   * immutable value types, returning the same instance is safe — no caller can mutate it.
   *
   * @param state the state to duplicate
   * @return the same instance
   */
  static GasState duplicate(final GasState state) {
    return state;
  }
}
