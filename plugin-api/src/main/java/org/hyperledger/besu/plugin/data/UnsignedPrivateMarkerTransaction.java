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
package org.hyperledger.besu.plugin.data;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Quantity;
import org.hyperledger.besu.datatypes.TransactionType;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * The interface Unsigned private marker transaction.
 *
 * @deprecated This type is scheduled for removal in a future release, with no replacement. It
 *     belonged to the private transaction API removed in Besu 25.6.0 and is no longer reachable
 *     from any plugin contract.
 */
@Deprecated(forRemoval = true)
public interface UnsignedPrivateMarkerTransaction {

  /**
   * Gets type.
   *
   * @return the type
   */
  TransactionType getType();

  /**
   * Gets nonce.
   *
   * @return the nonce
   */
  long getNonce();

  /**
   * Gets gas price.
   *
   * @return the gas price
   */
  Optional<? extends Quantity> getGasPrice();

  /**
   * Gets gas limit.
   *
   * @return the gas limit
   */
  long getGasLimit();

  /**
   * Gets to address.
   *
   * @return the to address
   */
  Optional<? extends Address> getTo();

  /**
   * Gets value.
   *
   * @return the value
   */
  Quantity getValue();

  /**
   * Gets payload.
   *
   * @return the payload
   */
  Bytes getPayload();
}
