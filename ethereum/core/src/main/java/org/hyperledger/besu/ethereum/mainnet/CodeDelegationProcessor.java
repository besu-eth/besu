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

import static org.hyperledger.besu.evm.account.Account.MAX_NONCE;
import static org.hyperledger.besu.evm.worldstate.CodeDelegationHelper.hasCodeDelegation;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.CodeDelegationService;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CodeDelegationProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(CodeDelegationProcessor.class);

  private final Optional<BigInteger> maybeChainId;
  private final BigInteger halfCurveOrder;
  private final CodeDelegationService codeDelegationService;

  public CodeDelegationProcessor(
      final Optional<BigInteger> maybeChainId,
      final BigInteger halfCurveOrder,
      final CodeDelegationService codeDelegationService) {
    this.maybeChainId = maybeChainId;
    this.halfCurveOrder = halfCurveOrder;
    this.codeDelegationService = codeDelegationService;
  }

  /**
   * At the start of executing the transaction, after incrementing the sender’s nonce, for each
   * authorization we do the following:
   *
   * <ol>
   *   <li>Verify the chain id is either 0 or the chain's current ID.
   *   <li>`authority = ecrecover(keccak(MAGIC || rlp([chain_id, address, nonce])), y_parity, r, s]`
   *   <li>Add `authority` to `accessed_addresses` (as defined in [EIP-2929](./eip-2929.md).)
   *   <li>Verify the code of `authority` is either empty or already delegated.
   *   <li>Verify the nonce of `authority` is equal to `nonce`.
   *   <li>Add `PER_EMPTY_ACCOUNT_COST - PER_AUTH_BASE_COST` gas to the global refund counter if
   *       `authority` exists in the trie.
   *   <li>Set the code of `authority` to be `0xef0100 || address`. This is a delegation
   *       designation.
   *   <li>Increase the nonce of `authority` by one.
   * </ol>
   *
   * @param worldUpdater The world state updater which is aware of code delegation.
   * @param transaction The transaction being processed.
   * @return The result of the code delegation processing.
   */
  public CodeDelegationResult process(
      final WorldUpdater worldUpdater,
      final Transaction transaction,
      final Optional<AccessLocationTracker> eip7928AccessList) {
    final CodeDelegationResult result = new CodeDelegationResult();

    // EIP-8037: tracks, per authority, whether it already held a delegation in the pre-transaction
    // state. Captured on the first authorization seen for an authority (before any prior auth in
    // this transaction could have changed its code), so later authorizations can distinguish a
    // delegation that existed before the transaction from one created earlier within it.
    final Map<Address, Boolean> delegatedBeforeTx = new HashMap<>();

    transaction
        .getCodeDelegationList()
        .get()
        .forEach(
            codeDelegation ->
                processCodeDelegation(
                    worldUpdater, codeDelegation, result, eip7928AccessList, delegatedBeforeTx));

    return result;
  }

  private void processCodeDelegation(
      final WorldUpdater worldUpdater,
      final CodeDelegation codeDelegation,
      final CodeDelegationResult result,
      final Optional<AccessLocationTracker> eip7928AccessList,
      final Map<Address, Boolean> delegatedBeforeTx) {
    LOG.trace("Processing code delegation: {}", codeDelegation);

    if (maybeChainId.isPresent()
        && !codeDelegation.chainId().equals(BigInteger.ZERO)
        && !maybeChainId.get().equals(codeDelegation.chainId())) {
      LOG.trace(
          "Invalid chain id for code delegation. Expected: {}, Actual: {}",
          maybeChainId.get(),
          codeDelegation.chainId());
      result.incrementInvalidAuthorization();
      return;
    }

    if (codeDelegation.nonce() == MAX_NONCE) {
      LOG.trace("Nonce of code delegation must be less than 2^64-1");
      result.incrementInvalidAuthorization();
      return;
    }

    if (codeDelegation.signature().getS().compareTo(halfCurveOrder) > 0) {
      LOG.trace(
          "Invalid signature for code delegation. S value must be less or equal than the half curve order.");
      result.incrementInvalidAuthorization();
      return;
    }

    final Optional<Address> authorizer = codeDelegation.authorizer();
    if (authorizer.isEmpty()) {
      LOG.trace("Invalid signature for code delegation");
      result.incrementInvalidAuthorization();
      return;
    }

    LOG.trace("Set code delegation for authority: {}", authorizer.get());

    // Use read-only get() to avoid marking the account as touched during validation.
    // getAccount() would mark it as touched, causing empty accounts to be incorrectly
    // deleted by clearAccountsThatAreEmpty() even when authorization is invalid/skipped.
    final Optional<Account> maybeExistingAccount =
        Optional.ofNullable(worldUpdater.get(authorizer.get()));
    eip7928AccessList.ifPresent(t -> t.addTouchedAccount(authorizer.get()));
    result.addAccessedDelegatorAddress(authorizer.get());

    MutableAccount authority;
    boolean authorityDoesAlreadyExist = false;
    boolean authorityHasExistingDelegation = false;
    if (maybeExistingAccount.isEmpty()) {
      // only create an account if nonce is valid
      if (codeDelegation.nonce() != 0) {
        result.incrementInvalidAuthorization();
        return;
      }
      authority = worldUpdater.createAccount(authorizer.get());
      eip7928AccessList.ifPresent(t -> t.addTouchedAccount(authority.getAddress()));
    } else {
      if (!codeDelegationService.canSetCodeDelegation(maybeExistingAccount.get())) {
        result.incrementInvalidAuthorization();
        return;
      }

      if (codeDelegation.nonce() != maybeExistingAccount.get().getNonce()) {
        LOG.trace(
            "Invalid nonce for code delegation. Expected: {}, Actual: {}",
            maybeExistingAccount.get().getNonce(),
            codeDelegation.nonce());
        result.incrementInvalidAuthorization();
        return;
      }

      authorityHasExistingDelegation = hasCodeDelegation(maybeExistingAccount.get().getCode());

      // Validation passed — now get the mutable account for mutation
      authority = worldUpdater.getAccount(authorizer.get());
      eip7928AccessList.ifPresent(t -> t.addTouchedAccount(authority.getAddress()));
      authorityDoesAlreadyExist = true;
    }

    if (authorityDoesAlreadyExist) {
      result.incrementAlreadyExistingDelegators();
    }

    // EIP-8037 AUTH_BASE state-gas refills, mirroring EELS set_delegation:
    //   - delegatedNow: the authority currently holds a delegation indicator (possibly written by
    //     an earlier authorization in this same transaction).
    //   - delegatedBefore: the authority already held a delegation in the pre-transaction state,
    //     captured the first time this authority is processed.
    final boolean delegatedNow = authorityHasExistingDelegation;
    final boolean delegatedBefore =
        delegatedBeforeTx.computeIfAbsent(authorizer.get(), a -> delegatedNow);
    if (codeDelegation.address().equals(Address.ZERO)) {
      // Clearing a delegation writes no indicator bytes, so AUTH_BASE is always refilled once. When
      // the delegation being cleared was created earlier in this same transaction (delegated now
      // but not before the transaction), its AUTH_BASE is refilled a second time.
      result.incrementAuthBaseRefundCount();
      if (delegatedNow && !delegatedBefore) {
        result.incrementAuthBaseRefundCount();
      }
    } else if (delegatedNow || delegatedBefore) {
      // Overwriting an existing delegation designator (current or pre-transaction) in place writes
      // no new indicator bytes, so AUTH_BASE is refilled.
      result.incrementAuthBaseRefundCount();
    }

    codeDelegationService.processCodeDelegation(authority, codeDelegation.address());
    authority.incrementNonce();
  }
}
