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

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.units.bigints.UInt256;

/**
 * Manages and records all accesses to the witness for stateless access to the storage and it might
 * return back the corresponding gas charges for such access.
 */
public interface AccessWitness {

  /**
   * Touch witness for all required accesses during a CALL when value is transferred.
   *
   * @param caller address of whom is doing the CALL operation
   * @param target address of the account being called
   * @param isAccountCreation will there be any account creation as a result of the operation
   * @param warmReadCost cost to deduct in case the witness was touched before with {@code target}
   *     address
   * @param remainingGas gas remaining for touching witness
   * @return stateless costs associated with accesses
   */
  long touchAndChargeValueTransfer(
      Address caller,
      Address target,
      boolean isAccountCreation,
      long warmReadCost,
      long remainingGas);

  /**
   * Touch witness for all required accesses during a SELFDESTRUCT when value is transferred.
   *
   * @param caller address of whom is doing the SELFDESTRUCT operation
   * @param target address of the account beneficiary
   * @param isAccountCreation will there be any account creation as a result of the operation
   * @param warmReadCost cost to deduct in case the witness was touched before with {@code target}
   *     address
   * @param remainingGas gas remaining for touching witness
   * @return stateless costs associated with accesses
   */
  long touchAndChargeValueTransferSelfDestruct(
      Address caller,
      Address target,
      boolean isAccountCreation,
      long warmReadCost,
      long remainingGas);

  /**
   * Touch witness as if accessing the {@code leafKey} index on the 0x0 branch index in the trie of
   * an account.
   *
   * @param address address of the account to record the read access
   * @param leafKey leaf index of the tree to record the read access
   * @param remainingGas gas remaining for touching witness
   * @return stateless costs associated with accesses
   */
  long touchAddressAndChargeRead(Address address, UInt256 leafKey, long remainingGas);

  /**
   * Touch witness for all required accesses during a base transaction from an EOA. This does not
   * return any cost because there are no charges.
   *
   * @param origin EOA from which transaction originates
   * @param target address of the account being called
   * @param value value being transferred as part of the transaction
   */
  void touchBaseTx(Address origin, Optional<Address> target, Wei value);

  /**
   * Touch witness for checking if a contract address already exists upon contract creation.
   *
   * @param address contract address being created
   * @param remainingGas gas remaining for touching witness
   * @return stateless costs associated with access
   */
  long touchAndChargeProofOfAbsence(Address address, long remainingGas);

  /**
   * Touch witness for all required accesses after a contract has been deposited in the account.
   * This implies all accesses required to finish contract creation after code has been added stored
   * in the account.
   *
   * @param address where the contract is being deployed
   * @param codeLength total size of the contract's code
   * @param remainingGas gas remaining for touching witness
   * @return stateless costs associated with accesses
   */
  long touchCodeChunksUponContractCreation(Address address, long codeLength, long remainingGas);

  /**
   * Touch witness for all required accesses for completing contract creation. This does not include
   * accesses related to the contract's code being deposited in the account.
   *
   * @param address where the contract is deployed
   * @param remainingGas gas remaining for touching witness
   * @return stateless costs associated with accesses
   */
  long touchAndChargeContractCreateCompleted(final Address address, long remainingGas);

  /**
   * Touch witness for read access to a given account's storage location during an SLOAD operation.
   *
   * @param address account address to read the storage for
   * @param storageKey key to the account storage location in the trie
   * @param remainingGas gas remaining for touching witness
   * @return stateless costs associated with storage access for the given key
   */
  long touchAndChargeStorageLoad(Address address, UInt256 storageKey, long remainingGas);

  /**
   * Touch witness for write access to a given account's storage location during an SSTORE
   * operation. Depending on whether a storage location already holds a value, write access will be
   * different for either setting or resetting the trie leaf holding the value.
   *
   * @param address account address to write the storage for
   * @param storageKey key to the account storage location in the trie
   * @param hasPreviousValue true if account previously stored a value at the given location, false
   *     otherwise
   * @param remainingGas gas remaining for touching witness
   * @return stateless costs associated with storage access for the given key
   */
  long touchAndChargeStorageStore(
      Address address, UInt256 storageKey, boolean hasPreviousValue, long remainingGas);

  /**
   * Touch witness for reading code chunks of an account.
   *
   * @param address contract address to read the code for
   * @param isContractInDeployment whether contract, which code is being read, is being deployed in
   *     the current transaction
   * @param offset index of the code that the read event starting from
   * @param readSize how many bytes are being read from the contract's code
   * @param codeLength total size of the contract's code
   * @param remainingGas gas remaining for touching witness
   * @return stateless costs associated with accesses
   */
  long touchCodeChunks(
      Address address,
      boolean isContractInDeployment,
      long offset,
      long readSize,
      long codeLength,
      long remainingGas);

  /**
   * Get all of the access events that constitute accesses to leaves.
   *
   * @return list of leaf access events.
   */
  List<AccessEvent<?>> getLeafAccesses();
}
