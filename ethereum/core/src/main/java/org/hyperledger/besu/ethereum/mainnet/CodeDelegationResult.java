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
import java.util.function.Predicate;

/**
 * The state-dependent charges accrued while applying a transaction's EIP-7702 authorizations, under
 * the EIP-2780 devnet-7 rework. Each valid authorization is charged at the top frame, keyed on the
 * authority's pre-transaction state, with no refund:
 *
 * <ul>
 *   <li>{@code newAccountCount}: authorities whose account leaf did not exist and had to be created
 *       — each charges NEW_ACCOUNT state gas.
 *   <li>{@code accountWriteCount}: the transaction's first write to a given authority's leaf (the
 *       sender is exempt, having been written at inclusion; repeats on one authority pay once) —
 *       each charges ACCOUNT_WRITE regular gas.
 *   <li>{@code authBaseCount}: authorities receiving a net-new delegation indicator (not delegated
 *       before the transaction, not already set earlier in it) — each charges AUTH_BASE state gas,
 *       never credited back.
 * </ul>
 */
public class CodeDelegationResult {

  /**
   * One authorization's top-frame access, in transaction order. Recorded for every authorization
   * that passes signature recovery — the point at which EELS {@code validate_authorization} adds
   * the authority to the accessed set — so the authority is touched (for the EIP-7928 block access
   * list) whether or not it goes on to be charged. The three flags mirror EELS {@code
   * set_delegation}'s per-authority charges, applied in this order: {@code newAccount} (NEW_ACCOUNT
   * state gas), {@code accountWrite} (ACCOUNT_WRITE regular gas), {@code authBase} (AUTH_BASE state
   * gas). An authorization that failed the nonce/code check carries all-false flags (touched, not
   * charged).
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
  private final Set<Address> accessedDelegatorAddresses = new HashSet<>(Address.SIZE);
  // Pre-Amsterdam (Prague/Osaka) refund model: authorities whose account leaf already existed, used
  // for the EIP-7702 PER_EMPTY_ACCOUNT - PER_AUTH_BASE regular refund. Not used by the Amsterdam
  // (devnet-7) per-authority runtime-charge model.
  private long alreadyExistingDelegators = 0L;

  public void addAccessedDelegatorAddress(final Address address) {
    accessedDelegatorAddresses.add(address);
  }

  /** Records one authorization's ordered top-frame access (touch, then per-authority charges). */
  public void addAuthorityAccess(final AuthorityAccess access) {
    authorityAccesses.add(access);
  }

  /**
   * Authorization accesses in transaction order. The Amsterdam runtime charge iterates these,
   * touching each authority before charging it and stopping at the first out-of-gas, so a partial
   * out-of-gas records only the authorities reached (up to and including the one being charged).
   */
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

  public Set<Address> accessedDelegatorAddresses() {
    return accessedDelegatorAddresses;
  }

  private long countAccesses(final Predicate<AuthorityAccess> charged) {
    return authorityAccesses.stream().filter(charged).count();
  }

  /** Authorities that had to be created — each charges NEW_ACCOUNT state gas. */
  public long newAccountCount() {
    return countAccesses(AuthorityAccess::newAccount);
  }

  /** First-write-to-authority events (sender exempt) — each charges ACCOUNT_WRITE regular gas. */
  public long accountWriteCount() {
    return countAccesses(AuthorityAccess::accountWrite);
  }

  /** Net-new delegation indicators written — each charges AUTH_BASE state gas, never refunded. */
  public long authBaseCount() {
    return countAccesses(AuthorityAccess::authBase);
  }
}
