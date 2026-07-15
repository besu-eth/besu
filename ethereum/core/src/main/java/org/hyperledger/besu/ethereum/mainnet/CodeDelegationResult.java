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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.datatypes.Address;

import java.util.HashSet;
import java.util.Set;

public class CodeDelegationResult {
  private final Set<Address> accessedDelegatorAddresses = new HashSet<>(Address.SIZE);
  private long alreadyExistingDelegators = 0L;
  private long authBaseRefundCount = 0L;

  public void addAccessedDelegatorAddress(final Address address) {
    accessedDelegatorAddresses.add(address);
  }

  public void incrementAlreadyExistingDelegators() {
    alreadyExistingDelegators += 1;
  }

  public void incrementAuthBaseRefundCount() {
    authBaseRefundCount += 1;
  }

  public Set<Address> accessedDelegatorAddresses() {
    return accessedDelegatorAddresses;
  }

  public long alreadyExistingDelegators() {
    return alreadyExistingDelegators;
  }

  /**
   * Returns the count of authorizations that don't write new delegation-indicator bytes — either
   * the authority already had a delegation designator (overwritten in place) or {@code
   * auth.address} is zero (no indicator written). The AUTH_BASE state gas is refunded for these.
   */
  public long authBaseRefundCount() {
    return authBaseRefundCount;
  }
}
