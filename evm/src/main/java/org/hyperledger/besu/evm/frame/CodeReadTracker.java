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

/**
 * Records contract code reads during EVM execution for EIP-8025 execution witness generation.
 * Separated from {@link Eip7928AccessList} so that witness concerns remain distinct from BAL
 * tracking.
 */
public interface CodeReadTracker {

  /**
   * Records that the given account's contract code was read during execution.
   *
   * @param address the address whose code was read
   */
  void addCodeRead(Address address);

  /**
   * Records that the given account's contract code was read during EIP-7702 SET_CODE authorization
   * processing, before EVM execution begins.
   *
   * @param address the address whose code was read during authorization
   */
  void addAuthorizationCodeRead(Address address);
}
