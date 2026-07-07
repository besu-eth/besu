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

    transaction
        .getCodeDelegationList()
        .get()
        .forEach(
            codeDelegation ->
                processCodeDelegation(worldUpdater, codeDelegation, result, eip7928AccessList));

    return result;
  }

  private void processCodeDelegation(
      final WorldUpdater worldUpdater,
      final CodeDelegation codeDelegation,
      final CodeDelegationResult result,
      final Optional<AccessLocationTracker> eip7928AccessList) {
    LOG.trace("Processing code delegation: {}", codeDelegation);

    // EIP-7702 (#11715): an invalid authorization grows no state, so its full worst-case intrinsic
    // charge (NEW_ACCOUNT + AUTH_BASE state gas, plus the regular ACCOUNT_WRITE) is refunded — the
    // wiring in MainnetTransactionProcessor folds invalidAuthorizations into both the
    // already-existing (ACCOUNT_WRITE / NEW_ACCOUNT) and AUTH_BASE refund counters.
    if (!isCodeDelegationValid(codeDelegation)) {
      result.incrementInvalidAuthorization();
      return;
    }

    final Optional<Address> maybeAuthorizer = codeDelegation.authorizer();
    if (maybeAuthorizer.isEmpty()) {
      LOG.trace("Invalid signature for code delegation");
      result.incrementInvalidAuthorization();
      return;
    }

    final Address authorizer = maybeAuthorizer.get();
    LOG.trace("Set code delegation for authority: {}", authorizer);

    // Use read-only get() to avoid marking the account as touched during validation.
    // getAccount() would mark it as touched, causing empty accounts to be incorrectly
    // deleted by clearAccountsThatAreEmpty() even when authorization is invalid/skipped.
    final Optional<Account> maybeExistingAccount =
        Optional.ofNullable(worldUpdater.get(authorizer));
    eip7928AccessList.ifPresent(t -> t.addTouchedAccount(authorizer));
    result.addAccessedDelegatorAddress(authorizer);

    if (!canSetCodeDelegation(codeDelegation, maybeExistingAccount)) {
      result.incrementInvalidAuthorization();
      return;
    }

    // the worst-case STATE_BYTES_PER_NEW_ACCOUNT and ACCOUNT_WRITE charged at intrinsic time
    // are only refilled/refunded if the authority already exists AND is non-empty
    // (non-zero nonce, non-zero balance, or non-empty code). A present-but-empty account is
    // treated like a brand-new leaf for gas purposes.
    final boolean authorityAlreadyExists =
        maybeExistingAccount.isPresent() && !maybeExistingAccount.get().isEmpty();

    final MutableAccount authority =
        maybeExistingAccount.isEmpty()
            ? worldUpdater.createAccount(authorizer)
            : worldUpdater.getAccount(authorizer);

    if (authorityAlreadyExists) {
      result.incrementAlreadyExistingDelegators();
    }

    // AUTH_BASE state gas is refunded when no new delegation-indicator bytes are written: either
    // the authority already carries a delegation designator (overwritten in place) or the
    // authorization clears the delegation (auth.address == 0).
    final boolean hasExistingDelegation =
        maybeExistingAccount.isPresent() && hasCodeDelegation(maybeExistingAccount.get().getCode());
    if (hasExistingDelegation || codeDelegation.address().equals(Address.ZERO)) {
      result.incrementAuthBaseRefundCount();
    }

    eip7928AccessList.ifPresent(t -> t.addTouchedAccount(authority.getAddress()));
    codeDelegationService.processCodeDelegation(authority, codeDelegation.address());
    authority.incrementNonce();
  }

  private boolean isCodeDelegationValid(final CodeDelegation codeDelegation) {
    if (maybeChainId.isPresent()
        && !codeDelegation.chainId().equals(BigInteger.ZERO)
        && !maybeChainId.get().equals(codeDelegation.chainId())) {
      LOG.trace(
          "Invalid chain id for code delegation. Expected: {}, Actual: {}",
          maybeChainId.get(),
          codeDelegation.chainId());
      return false;
    }

    if (codeDelegation.nonce() == MAX_NONCE) {
      LOG.trace("Nonce of code delegation must be less than 2^64-1");
      return false;
    }

    if (codeDelegation.signature().getS().compareTo(halfCurveOrder) > 0) {
      LOG.trace(
          "Invalid signature for code delegation. S value must be less or equal than the half curve order.");
      return false;
    }

    return true;
  }

  private boolean canSetCodeDelegation(
      final CodeDelegation codeDelegation, final Optional<Account> maybeExistingAccount) {
    if (maybeExistingAccount.isEmpty()) {
      // only create an account if nonce is valid
      return codeDelegation.nonce() == 0;
    }

    final Account existingAccount = maybeExistingAccount.get();

    if (!codeDelegationService.canSetCodeDelegation(existingAccount)) {
      return false;
    }

    if (codeDelegation.nonce() != existingAccount.getNonce()) {
      LOG.trace(
          "Invalid nonce for code delegation. Expected: {}, Actual: {}",
          existingAccount.getNonce(),
          codeDelegation.nonce());
      return false;
    }

    return true;
  }
}
