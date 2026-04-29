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
package org.hyperledger.besu.evm.frame;

import org.hyperledger.besu.datatypes.Address;

import org.apache.tuweni.bytes.Bytes32;

/**
 * EIP-8037 state-change event recorded at the opcode site for later frame-end aggregation. Three
 * variants: {@link Storage}, {@link AccountCreated}, {@link CodeDeposit}.
 */
public sealed interface StateChange
    permits StateChange.Storage, StateChange.AccountCreated, StateChange.CodeDeposit {

  /**
   * A storage cell write. The booleans are taken at the SSTORE site from values the opcode already
   * has on hand: tx-entry value, current value before the write, and the new value being written.
   *
   * @param address the account address
   * @param key the storage key
   * @param txEntryIsZero whether the slot's value at tx-entry was zero
   * @param beforeIsZero whether the slot's value before this SSTORE was zero
   * @param afterIsZero whether the new value is zero
   */
  record Storage(
      Address address,
      Bytes32 key,
      boolean txEntryIsZero,
      boolean beforeIsZero,
      boolean afterIsZero)
      implements StateChange {}

  /**
   * A new account was materialised at this address.
   *
   * @param address the account address
   */
  record AccountCreated(Address address) implements StateChange {}

  /**
   * Code was deposited at this address.
   *
   * @param address the contract address
   * @param codeLength the deposited code length
   */
  record CodeDeposit(Address address, int codeLength) implements StateChange {}
}
