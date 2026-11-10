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
package org.hyperledger.besu.datatypes;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.tuweni.bytes.Bytes;

/**
 * Interface defining parameters for Ethereum transaction calls and simulations.
 *
 * <p>This interface provides access to all the parameters that can be specified when making calls
 * to the Ethereum network, including both legacy and EIP-1559 transaction parameters, as well as
 * EIP-4844 blob transaction parameters.
 *
 * <p>All parameters are optional to support various call scenarios where only a subset of
 * parameters may be specified.
 */
public interface CallParameter {

  /**
   * Returns the chain ID for the transaction.
   *
   * @return an {@link Optional} containing the chain ID, or empty if not specified
   */
  Optional<BigInteger> getChainId();

  /**
   * Returns the sender address of the transaction.
   *
   * @return an {@link Optional} containing the sender address, or empty if not specified
   */
  Optional<Address> getSender();

  /**
   * Returns the recipient address of the transaction.
   *
   * @return an {@link Optional} containing the recipient address, or empty if not specified
   */
  Optional<Address> getTo();

  /**
   * Returns the gas limit for the transaction.
   *
   * @return an {@link OptionalLong} containing the gas limit, or empty if not specified
   */
  OptionalLong getGas();

  /**
   * Returns the maximum priority fee per gas (EIP-1559).
   *
   * <p>This represents the maximum fee per gas that the sender is willing to pay as a tip to the
   * block producer.
   *
   * @return an {@link Optional} containing the maximum priority fee per gas, or empty if not
   *     specified
   */
  Optional<Wei> getMaxPriorityFeePerGas();

  /**
   * Returns the maximum fee per gas (EIP-1559).
   *
   * <p>This represents the maximum total fee per gas that the sender is willing to pay, including
   * both the base fee and the priority fee.
   *
   * @return an {@link Optional} containing the maximum fee per gas, or empty if not specified
   */
  Optional<Wei> getMaxFeePerGas();

  /**
   * Returns the maximum fee per blob gas (EIP-4844).
   *
   * <p>This parameter is used for blob transactions to specify the maximum fee the sender is
   * willing to pay per unit of blob gas.
   *
   * @return an {@link Optional} containing the maximum fee per blob gas, or empty if not specified
   */
  Optional<Wei> getMaxFeePerBlobGas();

  /**
   * Returns the gas price for legacy transactions.
   *
   * <p>This parameter is used in pre-EIP-1559 transactions to specify the price per unit of gas.
   *
   * @return an {@link Optional} containing the gas price, or empty if not specified
   */
  Optional<Wei> getGasPrice();

  /**
   * Returns the value (amount of Ether) to be transferred with the transaction.
   *
   * @return an {@link Optional} containing the transaction value, or empty if not specified
   */
  Optional<Wei> getValue();

  /**
   * Returns the access list for the transaction (EIP-2930).
   *
   * <p>The access list specifies addresses and storage keys that the transaction plans to access,
   * potentially reducing gas costs.
   *
   * @return an {@link Optional} containing the access list, or empty if not specified
   */
  Optional<List<AccessListEntry>> getAccessList();

  /**
   * Returns the blob versioned hashes for blob transactions (EIP-4844).
   *
   * <p>These hashes represent the blob data associated with the transaction and are used for blob
   * transaction validation.
   *
   * @return an {@link Optional} containing the list of blob versioned hashes, or empty if not
   *     specified
   */
  Optional<List<VersionedHash>> getBlobVersionedHashes();

  /**
   * Returns the nonce of the transaction.
   *
   * <p>The nonce is a sequence number that prevents replay attacks and ensures transaction ordering
   * for a given sender.
   *
   * @return an {@link OptionalLong} containing the nonce, or empty if not specified
   */
  OptionalLong getNonce();

  /**
   * Returns whether strict validation should be applied.
   *
   * <p>When strict mode is enabled, additional validation rules may be applied during transaction
   * processing or simulation.
   *
   * @return an {@link Optional} containing the strict flag, or empty if not specified
   */
  Optional<Boolean> getStrict();

  /**
   * Returns the transaction payload (input data).
   *
   * <p>This contains the data sent along with the transaction, which may include contract method
   * calls, constructor parameters, or arbitrary data.
   *
   * @return an {@link Optional} containing the payload bytes, or empty if not specified
   */
  Optional<Bytes> getPayload();

  /**
   * Returns the list of code delegation authorizations.
   *
   * <p>Each authorization represents a signed statement from an externally owned account (EOA)
   * permitting a specific contract's code to be used as the EOA's code during transaction
   * execution. The EOA remains the transaction sender, but its code is temporarily substituted with
   * that of the authorized contract, enabling dynamic behavior similar to smart contract wallets.
   *
   * @return a {@link List} of {@link CodeDelegation} entries
   */
  List<CodeDelegation> getCodeDelegationAuthorizations();
}
