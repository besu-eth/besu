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
import org.hyperledger.besu.ethereum.mainnet.CodeDelegationResult.AuthorityAccess;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.CodeDelegationService;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
      final WorldUpdater worldUpdater, final Transaction transaction) {
    final CodeDelegationResult result = new CodeDelegationResult();

    // EIP-2780 (devnet-7): tracks, per authority, whether it already held a delegation in the
    // pre-transaction state. Captured on the first authorization seen for an authority (before any
    // prior auth in this transaction could have changed its code), so a later authorization can
    // tell a pre-transaction delegation from one created earlier within the transaction.
    final Map<Address, Boolean> delegatedBeforeTx = new HashMap<>();
    // Accounts already written by this transaction. The sender's leaf was written at inclusion
    // (nonce bump + fee deduction, priced into TX_BASE), so a self-sponsored authority pays no
    // ACCOUNT_WRITE; repeated authorizations on one authority pay it once. When the transaction
    // carries value, the recipient's leaf is also written by the value transfer, so an authority
    // equal to the recipient is likewise exempt (EELS set_delegation).
    final Set<Address> writtenAccounts = new HashSet<>();
    writtenAccounts.add(transaction.getSender());
    if (!transaction.getValue().isZero()) {
      transaction.getTo().ifPresent(writtenAccounts::add);
    }
    // Authorities a net-new delegation indicator has already been set for earlier in this
    // transaction; AUTH_BASE is charged at most once per authority.
    final Set<Address> delegationSetFor = new HashSet<>();

    transaction
        .getCodeDelegationList()
        .get()
        .forEach(
            codeDelegation ->
                processCodeDelegation(
                    worldUpdater,
                    codeDelegation,
                    result,
                    delegatedBeforeTx,
                    writtenAccounts,
                    delegationSetFor));

    return result;
  }

  private void processCodeDelegation(
      final WorldUpdater worldUpdater,
      final CodeDelegation codeDelegation,
      final CodeDelegationResult result,
      final Map<Address, Boolean> delegatedBeforeTx,
      final Set<Address> writtenAccounts,
      final Set<Address> delegationSetFor) {
    LOG.trace("Processing code delegation: {}", codeDelegation);

    if (maybeChainId.isPresent()
        && !codeDelegation.chainId().equals(BigInteger.ZERO)
        && !maybeChainId.get().equals(codeDelegation.chainId())) {
      LOG.trace(
          "Invalid chain id for code delegation. Expected: {}, Actual: {}",
          maybeChainId.get(),
          codeDelegation.chainId());
      return;
    }

    if (codeDelegation.nonce() == MAX_NONCE) {
      LOG.trace("Nonce of code delegation must be less than 2^64-1");
      return;
    }

    if (codeDelegation.signature().getS().compareTo(halfCurveOrder) > 0) {
      LOG.trace(
          "Invalid signature for code delegation. S value must be less or equal than the half curve order.");
      return;
    }

    final Optional<Address> authorizer = codeDelegation.authorizer();
    if (authorizer.isEmpty()) {
      LOG.trace("Invalid signature for code delegation");
      return;
    }

    LOG.trace("Set code delegation for authority: {}", authorizer.get());

    // Use read-only get() to avoid marking the account as touched during validation.
    // getAccount() would mark it as touched, causing empty accounts to be incorrectly
    // deleted by clearAccountsThatAreEmpty() even when authorization is invalid/skipped.
    final Optional<Account> maybeExistingAccount =
        Optional.ofNullable(worldUpdater.get(authorizer.get()));
    // Signature recovery succeeded: EELS validate_authorization adds the authority to the accessed
    // set here, before the nonce/code checks and the per-authority charge. The block-access-list
    // touch is deferred to the Amsterdam runtime charge, which replays these accesses in order and
    // stops at the first out-of-gas; recording the access now (even for the touch-only cases below)
    // keeps the touched set to exactly the authorities reached before an out-of-gas.
    result.addAccessedDelegatorAddress(authorizer.get());

    final boolean newAccount = maybeExistingAccount.isEmpty();
    final MutableAccount authority;
    final boolean authorityHasExistingDelegation;
    if (newAccount) {
      // only create an account if nonce is valid
      if (codeDelegation.nonce() != 0) {
        result.addAuthorityAccess(AuthorityAccess.touchOnly(authorizer.get()));
        return;
      }
      // EIP-2780: the authority's account leaf did not exist, so charge NEW_ACCOUNT state gas.
      authority = worldUpdater.createAccount(authorizer.get());
      authorityHasExistingDelegation = false;
    } else {
      if (!codeDelegationService.canSetCodeDelegation(maybeExistingAccount.get())) {
        result.addAuthorityAccess(AuthorityAccess.touchOnly(authorizer.get()));
        return;
      }

      if (codeDelegation.nonce() != maybeExistingAccount.get().getNonce()) {
        LOG.trace(
            "Invalid nonce for code delegation. Expected: {}, Actual: {}",
            maybeExistingAccount.get().getNonce(),
            codeDelegation.nonce());
        result.addAuthorityAccess(AuthorityAccess.touchOnly(authorizer.get()));
        return;
      }

      authorityHasExistingDelegation = hasCodeDelegation(maybeExistingAccount.get().getCode());

      // Validation passed — now get the mutable account for mutation
      authority = worldUpdater.getAccount(authorizer.get());
      // Pre-Amsterdam (Prague/Osaka) refund model uses this count.
      result.incrementAlreadyExistingDelegators();
    }

    // EIP-2780: ACCOUNT_WRITE (regular) on the transaction's first write to this authority's leaf.
    // The sender's leaf was pre-seeded into writtenAccounts, so a self-sponsored authority and
    // repeated authorizations on one authority pay ACCOUNT_WRITE at most once.
    final boolean accountWrite = writtenAccounts.add(authorizer.get());

    // EIP-2780 AUTH_BASE (state), mirroring EELS set_delegation. delegatedBefore records whether
    // the
    // authority already held a delegation in the pre-transaction state, captured the first time the
    // authority is seen (so a delegation set by an earlier authorization in this transaction does
    // not
    // masquerade as a pre-existing one). Charge AUTH_BASE only for a net-new delegation indicator:
    // the authority was not delegated before the transaction and has not already been set in it. It
    // is never credited back — a delegation set then cleared in the same transaction keeps its
    // charge.
    final boolean delegatedBefore =
        delegatedBeforeTx.computeIfAbsent(authorizer.get(), a -> authorityHasExistingDelegation);
    final boolean authBase =
        !codeDelegation.address().equals(Address.ZERO)
            && !delegatedBefore
            && delegationSetFor.add(authorizer.get());

    result.addAuthorityAccess(
        new AuthorityAccess(authorizer.get(), newAccount, accountWrite, authBase));

    codeDelegationService.processCodeDelegation(authority, codeDelegation.address());
    authority.incrementNonce();
  }
}
