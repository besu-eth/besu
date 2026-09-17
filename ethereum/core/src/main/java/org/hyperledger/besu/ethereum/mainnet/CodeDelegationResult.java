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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * What a transaction's EIP-7702 authorizations accessed and owe. Under EIP-2780 each authorization
 * is charged at the top frame against the authority's pre-transaction state, with no refund.
 */
public class CodeDelegationResult {

  /**
   * One authorization's top-frame access, in transaction order. Recorded for every authorization
   * whose signature recovered, so the authority reaches the EIP-7928 block access list whether or
   * not it goes on to be charged; one that failed the nonce/code check carries all-false flags. The
   * charges are applied in the order the flags are declared.
   *
   * @param authority the recovered authority address
   * @param newAccount whether the authority's account leaf had to be created
   * @param accountWrite whether this is the transaction's first write to the authority's leaf
   * @param authBase whether a net-new delegation indicator is written for the authority
   */
  public record AuthorityAccess(
      Address authority, boolean newAccount, boolean accountWrite, boolean authBase) {

    /** An authorization that was touched during validation but failed it, so is never charged. */
    public static AuthorityAccess touchOnly(final Address authority) {
      return new AuthorityAccess(authority, false, false, false);
    }
  }

  private final List<AuthorityAccess> authorityAccesses = new ArrayList<>();
  // Pre-Amsterdam only: feeds the EIP-7702 PER_EMPTY_ACCOUNT - PER_AUTH_BASE refund.
  private long alreadyExistingDelegators = 0L;

  /** Records one authorization's top-frame access. */
  public void addAuthorityAccess(final AuthorityAccess access) {
    authorityAccesses.add(access);
  }

  /** Authorization accesses in the order the runtime charge has to replay them. */
  public List<AuthorityAccess> authorityAccesses() {
    return authorityAccesses;
  }

  public void incrementAlreadyExistingDelegators() {
    alreadyExistingDelegators += 1;
  }

  /** Pre-Amsterdam refund model: count of authorities whose account leaf already existed. */
  public long alreadyExistingDelegators() {
    return alreadyExistingDelegators;
  }

  /** The authorities EIP-2929 warms: every one whose signature recovered, applied or not. */
  public Set<Address> accessedDelegatorAddresses() {
    return authorityAccesses.stream()
        .map(AuthorityAccess::authority)
        .collect(Collectors.toCollection(() -> new HashSet<>(Address.SIZE)));
  }
}
