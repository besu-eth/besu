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

import org.hyperledger.besu.crypto.SECPSignature;

import java.math.BigInteger;
import java.util.Optional;

/**
 * CodeDelegation is a data structure that represents the authorization to delegate code of an EOA
 * account to another account.
 */
public interface CodeDelegation {
  /** The cost of delegating code on an existing account. */
  long PER_AUTH_BASE_COST = 12_500L;

  /**
   * Return the chain id.
   *
   * @return chain id
   */
  BigInteger chainId();

  /**
   * Return the address of the account which code will be used.
   *
   * @return address
   */
  Address address();

  /**
   * Return the signature.
   *
   * @return signature
   */
  SECPSignature signature();

  /**
   * Return the authorizer address.
   *
   * @return authorizer address of the EOA which will load the code into its account
   */
  Optional<Address> authorizer();

  /**
   * Return the nonce
   *
   * @return the nonce
   */
  long nonce();

  /**
   * Return the recovery id.
   *
   * @return byte
   */
  byte v();

  /**
   * Return the r value of the signature.
   *
   * @return r value
   */
  BigInteger r();

  /**
   * Return the s value of the signature.
   *
   * @return s value
   */
  BigInteger s();
}
