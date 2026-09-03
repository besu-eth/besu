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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.frame.CodeReadTracker;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Block-scoped collector of EIP-8025 code reads. Lives for the lifetime of a single block execution
 * and is independent of the EIP-7928 block-access-list pipeline.
 *
 * <p>Instantiated once per block and passed through the processing stack; all transaction
 * processors append into the same instance so that the final sets cover the entire block.
 */
public class WitnessCodeTracker implements CodeReadTracker {

  private final Set<Address> codeReads = ConcurrentHashMap.newKeySet();
  private final Set<Address> authorizationCodeReads = ConcurrentHashMap.newKeySet();

  /**
   * Records that the bytecode at {@code address} was read during EVM execution (e.g. a CALL,
   * DELEGATECALL, EXTCODESIZE, or EXTCODECOPY that accessed the account's code).
   *
   * @param address the account whose code was read
   */
  @Override
  public void addCodeRead(final Address address) {
    codeReads.add(address);
  }

  /**
   * Records that the bytecode at {@code address} was read during EIP-7702 SET_CODE authorization
   * processing, before EVM execution begins for the transaction.
   *
   * @param address the authority address whose code was read during authorization
   */
  @Override
  public void addAuthorizationCodeRead(final Address address) {
    authorizationCodeReads.add(address);
  }

  /**
   * Returns an unmodifiable snapshot of all addresses whose code was read during EVM execution.
   *
   * @return the set of addresses with EVM code reads
   */
  public Set<Address> getCodeReads() {
    return Collections.unmodifiableSet(codeReads);
  }

  /**
   * Returns an unmodifiable snapshot of all authority addresses whose code was read during EIP-7702
   * authorization processing.
   *
   * @return the set of authority addresses with authorization code reads
   */
  public Set<Address> getAuthorizationCodeReads() {
    return Collections.unmodifiableSet(authorizationCodeReads);
  }
}
