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

import static org.hyperledger.besu.evm.worldstate.CodeDelegationHelper.hasCodeDelegation;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.feemarket.CoinbaseFeePriceCalculator;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.processor.MessageCallProcessor;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;

/**
 * Transaction-level steps shared by {@link MainnetTransactionProcessor} and {@link
 * FrameTransactionProcessor}: resolving a top-level call's code, the EIP-2780 charges on entering a
 * top-level call, crediting the coinbase, and settling self-destructs.
 */
final class TopLevelTransactionSupport {

  private final GasCalculator gasCalculator;
  private final MessageCallProcessor messageCallProcessor;
  private final CoinbaseFeePriceCalculator coinbaseFeePriceCalculator;

  TopLevelTransactionSupport(
      final GasCalculator gasCalculator,
      final MessageCallProcessor messageCallProcessor,
      final CoinbaseFeePriceCalculator coinbaseFeePriceCalculator) {
    this.gasCalculator = gasCalculator;
    this.messageCallProcessor = messageCallProcessor;
    this.coinbaseFeePriceCalculator = coinbaseFeePriceCalculator;
  }

  /**
   * Resolves the code a top-level call to {@code account} runs.
   *
   * @param account the called account, possibly null
   * @param delegatedCode resolves the code of an EIP-7702 delegated account; callers differ in how
   *     they warm and record the delegation target
   * @return the code to execute
   */
  Code accountCode(final Account account, final Function<Account, Code> delegatedCode) {
    if (account == null) {
      return Code.EMPTY_CODE;
    }

    final Hash codeHash = account.getCodeHash();
    if (codeHash == null || codeHash.equals(Hash.EMPTY)) {
      return Code.EMPTY_CODE;
    }

    if (hasCodeDelegation(account.getCode())) {
      return delegatedCode.apply(account);
    }

    // Bonsai accounts may have a fully cached code, so we use that one
    if (account.getCodeCache() != null) {
      return account.getOrCreateCachedCode();
    }

    // Any other account can only use the cached jump dest analysis if available
    return messageCallProcessor.getOrCreateCachedJumpDest(codeHash, account.getCode());
  }

  /**
   * Charges the warm/cold-aware access to {@code address} against the frame's remaining gas,
   * warming it as a side effect. Precompiles are warm by construction, but warmUpAddress still has
   * to run: its side effect must stand for any later access.
   *
   * @return true if the charge was afforded, false on out-of-gas
   */
  boolean chargeAccountAccess(final MessageFrame frame, final Address address) {
    final boolean wasWarm = frame.warmUpAddress(address) || gasCalculator.isPrecompile(address);
    final long cost =
        wasWarm ? gasCalculator.getWarmStorageReadCost() : gasCalculator.getColdAccountAccessCost();
    if (frame.getRemainingGas() < cost) {
      return false;
    }
    frame.decrementRemainingGas(cost);
    return true;
  }

  /**
   * Charges the EIP-2780 dispatch-entry costs on a top-level call frame: NEW_ACCOUNT when value
   * materialises an empty recipient leaf, then the access to a delegated recipient's target. {@code
   * worldState} should already have the recipient cached from code resolution, so reading it
   * pre-value-transfer costs no extra lookup.
   *
   * @return true if both charges were afforded, false on out-of-gas
   */
  boolean chargeTransactionEntry(
      final MessageFrame frame, final WorldUpdater worldState, final Address to) {
    final Account recipient = worldState.get(to);
    // Positive value to a non-alive recipient. Precompiles are deliberately not excluded, since
    // a zero-balance precompile is not "alive" under EIP-161 either.
    if (!frame.getValue().isZero()
        && (recipient == null || recipient.isEmpty())
        && !frame.consumeStateGas(gasCalculator.stateGasCostCalculator().newAccountStateGas())) {
      return false;
    }
    // EIP-2780: the top-level access to a delegated recipient's target is warm/cold aware.
    if (recipient != null && hasCodeDelegation(recipient.getCode())) {
      final Address target = CodeDelegationHelper.getTargetAddress(recipient.getCode());
      if (!chargeAccountAccess(frame, target)) {
        return false;
      }
      // EIP-7928: the target is loaded only once its access is paid for, so an access charge that
      // runs out of gas has to leave it out of the list entirely.
      frame.getEip7928AccessList().ifPresent(bal -> bal.addTouchedAccount(target));
    }
    return true;
  }

  /**
   * Selects how the coinbase is paid for a transaction priced at {@code gasPrice}.
   *
   * @return the calculator, or empty when the price is below the base fee and the validation
   *     parameters do not allow it
   */
  Optional<CoinbaseFeePriceCalculator> coinbaseCalculator(
      final ProcessableBlockHeader blockHeader,
      final Wei gasPrice,
      final TransactionValidationParams transactionValidationParams) {
    if (blockHeader.getBaseFee().isEmpty()) {
      return Optional.of(CoinbaseFeePriceCalculator.frontier());
    }
    final boolean gasPriceBelowBaseFee = gasPrice.compareTo(blockHeader.getBaseFee().get()) < 0;
    if (transactionValidationParams.allowUnderpricedGas()
        || transactionValidationParams.isPreserveCallerGasPricing()) {
      return Optional.of(gasPriceBelowBaseFee ? (a, b, c) -> Wei.ZERO : coinbaseFeePriceCalculator);
    }
    return gasPriceBelowBaseFee ? Optional.empty() : Optional.of(coinbaseFeePriceCalculator);
  }

  /**
   * Credits the coinbase with its share of the transaction fee. The coinbase is touched even when
   * the fee is zero (EIP-158 & EIP-7928), so an <em>empty</em> coinbase can be deleted during state
   * clearing.
   */
  static void creditCoinbase(
      final WorldUpdater worldUpdater,
      final Address miningBeneficiary,
      final Wei coinbaseWeiDelta,
      final Optional<AccessLocationTracker> accessLocationTracker) {
    final MutableAccount coinbase = worldUpdater.getOrCreate(miningBeneficiary);
    accessLocationTracker.ifPresent(t -> t.addTouchedAccount(miningBeneficiary));
    if (!coinbaseWeiDelta.isZero()) {
      coinbase.incrementBalance(coinbaseWeiDelta);
    }
  }

  /**
   * Settles accounts marked for self-destruction at transaction finalization. Under EIP-8246 each
   * account is cleared (nonce reset, code and storage removed) but keeps its balance — EIP-161
   * state clearing (via {@code clearAccountsThatAreEmpty}) then removes any account left with a
   * zero balance. Pre-EIP-8246 the accounts are deleted outright.
   *
   * @param worldState the world state updater
   * @param selfDestructs the addresses marked for self-destruction
   */
  void settleSelfDestructs(final WorldUpdater worldState, final Set<Address> selfDestructs) {
    if (gasCalculator.isSelfDestructBalancePreserved()) {
      selfDestructs.forEach(
          address -> {
            final MutableAccount account = worldState.getAccount(address);
            if (account != null) {
              account.setNonce(0L);
              account.setCode(Bytes.EMPTY);
              account.clearStorage();
            }
          });
    } else {
      selfDestructs.forEach(worldState::deleteAccount);
    }
  }
}
