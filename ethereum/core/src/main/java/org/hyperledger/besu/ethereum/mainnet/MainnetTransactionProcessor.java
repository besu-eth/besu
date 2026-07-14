/*
 * Copyright ConsenSys AG.
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

import static org.hyperledger.besu.evm.internal.Words.clampedAdd;
import static org.hyperledger.besu.evm.worldstate.CodeDelegationHelper.getTarget;
import static org.hyperledger.besu.evm.worldstate.CodeDelegationHelper.hasCodeDelegation;

import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.feemarket.CoinbaseFeePriceCalculator;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.frame.Eip8037Trace;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.StateGasCostCalculator;
import org.hyperledger.besu.evm.log.TransferLogEmitter;
import org.hyperledger.besu.evm.processor.AbstractMessageProcessor;
import org.hyperledger.besu.evm.processor.ContractCreationProcessor;
import org.hyperledger.besu.evm.processor.MessageCallProcessor;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainnetTransactionProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(MainnetTransactionProcessor.class);

  private static final Set<Address> EMPTY_ADDRESS_SET = Set.of();

  protected final GasCalculator gasCalculator;

  protected final TransactionValidatorFactory transactionValidatorFactory;

  private final ContractCreationProcessor contractCreationProcessor;

  private final MessageCallProcessor messageCallProcessor;

  private final int maxStackSize;

  private final boolean clearEmptyAccounts;

  protected final boolean warmCoinbase;

  protected final FeeMarket feeMarket;
  private final CoinbaseFeePriceCalculator coinbaseFeePriceCalculator;

  private final Optional<CodeDelegationProcessor> maybeCodeDelegationProcessor;

  private final TransferLogEmitter transferLogEmitter;

  private MainnetTransactionProcessor(
      final GasCalculator gasCalculator,
      final TransactionValidatorFactory transactionValidatorFactory,
      final ContractCreationProcessor contractCreationProcessor,
      final MessageCallProcessor messageCallProcessor,
      final boolean clearEmptyAccounts,
      final boolean warmCoinbase,
      final int maxStackSize,
      final FeeMarket feeMarket,
      final CoinbaseFeePriceCalculator coinbaseFeePriceCalculator,
      final CodeDelegationProcessor maybeCodeDelegationProcessor,
      final TransferLogEmitter transferLogEmitter) {
    this.gasCalculator = gasCalculator;
    this.transactionValidatorFactory = transactionValidatorFactory;
    this.contractCreationProcessor = contractCreationProcessor;
    this.messageCallProcessor = messageCallProcessor;
    this.clearEmptyAccounts = clearEmptyAccounts;
    this.warmCoinbase = warmCoinbase;
    this.maxStackSize = maxStackSize;
    this.feeMarket = feeMarket;
    this.coinbaseFeePriceCalculator = coinbaseFeePriceCalculator;
    this.maybeCodeDelegationProcessor = Optional.ofNullable(maybeCodeDelegationProcessor);
    this.transferLogEmitter = transferLogEmitter;
  }

  /**
   * Applies a transaction to the current system state.
   *
   * @param worldState The current world state
   * @param blockHeader The current block header
   * @param transaction The transaction to process
   * @param miningBeneficiary The address which is to receive the transaction fee
   * @param blockHashLookup The {@link BlockHashLookup} to use for BLOCKHASH operations
   * @param transactionValidationParams Validation parameters that will be used by the {@link
   *     MainnetTransactionValidator}
   * @return the transaction result
   * @see MainnetTransactionValidator
   * @see TransactionValidationParams
   */
  public TransactionProcessingResult processTransaction(
      final WorldUpdater worldState,
      final ProcessableBlockHeader blockHeader,
      final Transaction transaction,
      final Address miningBeneficiary,
      final BlockHashLookup blockHashLookup,
      final TransactionValidationParams transactionValidationParams,
      final Wei blobGasPrice) {
    return processTransaction(
        worldState,
        blockHeader,
        transaction,
        miningBeneficiary,
        OperationTracer.NO_TRACING,
        blockHashLookup,
        transactionValidationParams,
        blobGasPrice);
  }

  /**
   * Applies a transaction to the current system state.
   *
   * @param worldState The current world state
   * @param blockHeader The current block header
   * @param transaction The transaction to process
   * @param miningBeneficiary The address which is to receive the transaction fee
   * @param operationTracer The tracer to record results of each EVM operation
   * @param blockHashLookup The {@link BlockHashLookup} to use for BLOCKHASH operations
   * @return the transaction result
   */
  public TransactionProcessingResult processTransaction(
      final WorldUpdater worldState,
      final ProcessableBlockHeader blockHeader,
      final Transaction transaction,
      final Address miningBeneficiary,
      final OperationTracer operationTracer,
      final BlockHashLookup blockHashLookup,
      final Wei blobGasPrice) {
    return processTransaction(
        worldState,
        blockHeader,
        transaction,
        miningBeneficiary,
        operationTracer,
        blockHashLookup,
        ImmutableTransactionValidationParams.builder().build(),
        blobGasPrice);
  }

  public TransactionProcessingResult processTransaction(
      final WorldUpdater worldState,
      final ProcessableBlockHeader blockHeader,
      final Transaction transaction,
      final Address miningBeneficiary,
      final OperationTracer operationTracer,
      final BlockHashLookup blockHashLookup,
      final TransactionValidationParams transactionValidationParams,
      final Wei blobGasPrice) {
    return processTransaction(
        worldState,
        blockHeader,
        transaction,
        miningBeneficiary,
        operationTracer,
        blockHashLookup,
        transactionValidationParams,
        blobGasPrice,
        Optional.empty());
  }

  public TransactionProcessingResult processTransaction(
      final WorldUpdater worldState,
      final ProcessableBlockHeader blockHeader,
      final Transaction transaction,
      final Address miningBeneficiary,
      final OperationTracer operationTracer,
      final BlockHashLookup blockHashLookup,
      final TransactionValidationParams transactionValidationParams,
      final Wei blobGasPrice,
      final Optional<AccessLocationTracker> accessLocationTracker) {
    try {
      final var transactionValidator = transactionValidatorFactory.get();
      LOG.trace("Starting execution of {}", transaction);
      ValidationResult<TransactionInvalidReason> validationResult =
          transactionValidator.validate(
              transaction,
              blockHeader.getBaseFee(),
              Optional.ofNullable(blobGasPrice),
              transactionValidationParams);
      // Make sure the transaction is intrinsically valid before trying to
      // compare against a sender account (because the transaction may not
      // be signed correctly to extract the sender).
      if (!validationResult.isValid()) {
        LOG.debug("Invalid transaction: {}", validationResult.getErrorMessage());
        return TransactionProcessingResult.invalid(validationResult);
      }

      final Address senderAddress = transaction.getSender();
      final MutableAccount sender = worldState.getOrCreateSenderAccount(senderAddress);
      accessLocationTracker.ifPresent(t -> t.addTouchedAccount(senderAddress));

      validationResult =
          transactionValidator.validateForSender(transaction, sender, transactionValidationParams);
      if (!validationResult.isValid()) {
        LOG.debug("Invalid transaction: {}", validationResult.getErrorMessage());
        return TransactionProcessingResult.invalid(validationResult);
      }

      operationTracer.tracePrepareTransaction(worldState, transaction);

      final Set<Address> eip2930WarmAddressList = new HashSet<>(Address.SIZE);

      final long previousNonce = sender.incrementNonce();
      LOG.trace(
          "Incremented sender {} nonce ({} -> {})",
          senderAddress,
          previousNonce,
          sender.getNonce());

      final Wei transactionGasPrice =
          feeMarket.getTransactionPriceCalculator().price(transaction, blockHeader.getBaseFee());

      final long blobGas = gasCalculator.blobGasCost(transaction.getBlobCount());

      final Wei upfrontGasCost =
          transaction.getUpfrontGasCost(transactionGasPrice, blobGasPrice, blobGas);
      try {
        final Wei previousBalance = sender.decrementBalance(upfrontGasCost);
        LOG.trace(
            "Deducted sender {} upfront gas cost {} ({} -> {})",
            senderAddress,
            upfrontGasCost,
            previousBalance,
            sender.getBalance());
      } catch (final IllegalStateException ise) {
        if (transactionValidationParams.allowUnderpriced()) {
          LOG.trace("Allowing account balance underflow as requested");
        } else {
          throw ise;
        }
      }

      final var stateGasCalc = gasCalculator.stateGasCostCalculator();

      // Amsterdam (devnet-7): authorizations are charged at the top frame on their pre-state with
      // no
      // refund. Pre-Amsterdam (Prague/Osaka): the worst-case per-auth cost is charged in the
      // intrinsic and the PER_EMPTY_ACCOUNT - PER_AUTH_BASE portion is refunded for existing
      // authorities.
      long codeDelegationRefund = 0L;
      // Per-authority runtime accesses (Amsterdam), replayed against the initial frame below: each
      // authority is touched (for the block access list) then charged, stopping at the first
      // out-of-gas. Empty for Prague/Osaka and non-delegation transactions.
      List<CodeDelegationResult.AuthorityAccess> delegationAccesses = List.of();
      // Amsterdam (devnet-7): the applied delegations are held in this uncommitted updater until
      // the
      // top-frame authorization/dispatch prep charges clear. A prep out-of-gas leaves it
      // uncommitted
      // (rolling the delegations back, per EELS set_delegation); once prep clears it is committed
      // so
      // the delegations persist even through a later dispatch revert. Null for Prague/Osaka (which
      // commit immediately, no runtime prep charge) and for non-delegation transactions.
      WorldUpdater deferredDelegationUpdater = null;
      if (transaction.getType().equals(TransactionType.DELEGATE_CODE)) {
        if (maybeCodeDelegationProcessor.isEmpty()) {
          throw new RuntimeException("Code delegation processor is required for 7702 transactions");
        }

        final WorldUpdater delegationUpdater = worldState.updater();
        final CodeDelegationResult codeDelegationResult =
            maybeCodeDelegationProcessor.get().process(delegationUpdater, transaction);
        eip2930WarmAddressList.addAll(codeDelegationResult.accessedDelegatorAddresses());
        if (stateGasCalc.isActive()) {
          // Amsterdam per-authority runtime-charge model; defer the commit for prep-OOG rollback.
          delegationAccesses = codeDelegationResult.authorityAccesses();
          deferredDelegationUpdater = delegationUpdater;
        } else {
          // Prague/Osaka refund model; the intrinsic already validated the charge, so commit now.
          codeDelegationRefund =
              gasCalculator.calculateDelegateCodeGasRefund(
                  codeDelegationResult.alreadyExistingDelegators());
          delegationUpdater.commit();
        }
      }

      // The frame reads the applied delegations from the deferred (uncommitted) updater for an
      // Amsterdam delegation tx, otherwise directly from the transaction-level world state.
      final WorldUpdater frameWorldState =
          deferredDelegationUpdater != null ? deferredDelegationUpdater : worldState;

      final List<AccessListEntry> eip2930AccessListEntries =
          transaction.getAccessList().orElse(List.of());
      // we need to keep a separate hash set of addresses in case they specify no storage.
      // No-storage is a common pattern, especially for Externally Owned Accounts
      final Multimap<Address, Bytes32> eip2930StorageList = HashMultimap.create();
      int accessListStorageCount = 0;
      for (final var entry : eip2930AccessListEntries) {
        final Address address = entry.address();
        eip2930WarmAddressList.add(address);
        final List<Bytes32> storageKeys = entry.storageKeys();
        eip2930StorageList.putAll(address, storageKeys);
        accessListStorageCount += storageKeys.size();
      }
      if (warmCoinbase) {
        eip2930WarmAddressList.add(miningBeneficiary);
      }

      final long accessListGas =
          gasCalculator.accessListGasCost(eip2930AccessListEntries.size(), accessListStorageCount);
      final long codeDelegationGas =
          gasCalculator.delegateCodeGasCost(transaction.codeDelegationListSize());
      final long intrinsicRegularGas =
          gasCalculator.transactionIntrinsicGasCost(
              transaction, clampedAdd(accessListGas, codeDelegationGas));

      // EIP-8037: Validate that gas limit covers both regular AND state intrinsic gas.
      // This must be checked before frame construction to reject the tx at the intrinsic level.
      final long intrinsicStateGas =
          stateGasCalc.transactionIntrinsicStateGas(
              transaction.isContractCreation(), transaction.codeDelegationListSize());
      if (transaction.getGasLimit() < intrinsicRegularGas + intrinsicStateGas) {
        LOG.trace(
            "Insufficient gas for intrinsic cost: gasLimit={}, regularIntrinsic={}, stateIntrinsic={}",
            transaction.getGasLimit(),
            intrinsicRegularGas,
            intrinsicStateGas);
        return TransactionProcessingResult.invalid(
            ValidationResult.invalid(
                TransactionInvalidReason.INTRINSIC_GAS_EXCEEDS_GAS_LIMIT,
                String.format(
                    "intrinsic gas cost %d (regular %d + state %d) exceeds gas limit %d",
                    intrinsicRegularGas + intrinsicStateGas,
                    intrinsicRegularGas,
                    intrinsicStateGas,
                    transaction.getGasLimit())));
      }

      final long gasAvailable = transaction.getGasLimit() - intrinsicRegularGas;
      LOG.trace(
          "Gas available for execution {} = {} - {} (limit - intrinsic)",
          gasAvailable,
          transaction.getGasLimit(),
          intrinsicRegularGas);

      final WorldUpdater worldUpdater = frameWorldState.updater();

      operationTracer.traceStartTransaction(worldUpdater, transaction);

      final MessageFrame.Builder commonMessageFrameBuilder =
          MessageFrame.builder()
              .maxStackSize(maxStackSize)
              .worldUpdater(worldUpdater.updater())
              .initialGas(gasAvailable)
              .originator(senderAddress)
              .gasPrice(transactionGasPrice)
              .blobGasPrice(blobGasPrice)
              .sender(senderAddress)
              .value(transaction.getValue())
              .apparentValue(transaction.getValue())
              .blockValues(blockHeader)
              .completer(__ -> {})
              .miningBeneficiary(miningBeneficiary)
              .blockHashLookup(blockHashLookup)
              .eip2930AccessListWarmStorage(eip2930StorageList);

      accessLocationTracker.ifPresent(commonMessageFrameBuilder::eip7928AccessList);

      if (transaction.getVersionedHashes().isPresent()) {
        commonMessageFrameBuilder.versionedHashes(
            Optional.of(transaction.getVersionedHashes().get().stream().toList()));
      } else {
        commonMessageFrameBuilder.versionedHashes(Optional.empty());
      }

      final MessageFrame initialFrame;
      // EIP-8037: whether a contract-creation transaction's target address was already alive
      // (existed and non-empty, e.g. pre-funded) before execution. On a successful creation to an
      // already-alive target no new account leaf is added, so the intrinsic NEW_ACCOUNT state gas
      // is refunded — matching EELS process_transaction's created_target_alive path.
      boolean createTargetAlreadyAlive = false;
      if (transaction.isContractCreation()) {
        final Address contractAddress =
            Address.contractAddress(senderAddress, sender.getNonce() - 1L);
        final Account existingTarget = worldState.get(contractAddress);
        createTargetAlreadyAlive = existingTarget != null && !existingTarget.isEmpty();
        accessLocationTracker.ifPresent(t -> t.addTouchedAccount(contractAddress));

        final Bytes initCodeBytes = transaction.getPayload();
        Code code = new Code(initCodeBytes);
        initialFrame =
            commonMessageFrameBuilder
                .type(MessageFrame.Type.CONTRACT_CREATION)
                .address(contractAddress)
                .contract(contractAddress)
                .inputData(initCodeBytes.slice(code.getSize()))
                .code(code)
                .eip2930AccessListWarmAddresses(eip2930WarmAddressList)
                .build();
      } else {
        @SuppressWarnings("OptionalGetWithoutIsPresent") // isContractCall tests isPresent
        final Address to = transaction.getTo().get();
        // EIP-7928 (devnet-7 v7.1.0): the recipient is loaded during dispatch, which for a
        // delegation
        // transaction runs AFTER the top-frame authorization charges. If those charges run out of
        // gas the recipient is never loaded, so it must not appear in the block access list. Defer
        // the recipient touch to after the auth charge for delegation txs; touch it eagerly
        // otherwise (there is no pre-dispatch charge that can halt for a non-delegation tx).
        if (!transaction.getType().equals(TransactionType.DELEGATE_CODE)) {
          accessLocationTracker.ifPresent(t -> t.addTouchedAccount(to));
        }
        final Code code =
            processCodeFromAccount(
                frameWorldState,
                eip2930WarmAddressList,
                frameWorldState.get(to),
                accessLocationTracker);

        initialFrame =
            commonMessageFrameBuilder
                .type(MessageFrame.Type.MESSAGE_CALL)
                .address(to)
                .contract(to)
                .inputData(transaction.getPayload())
                .code(code)
                .eip2930AccessListWarmAddresses(eip2930WarmAddressList)
                .build();
      }
      initializeStateGasReservoir(initialFrame, gasAvailable, intrinsicRegularGas, stateGasCalc);
      final TopFrameCharges topFrameCharges =
          chargeIntrinsicStateGas(
              initialFrame,
              transaction,
              gasCalculator,
              stateGasCalc,
              createTargetAlreadyAlive,
              delegationAccesses,
              accessLocationTracker);
      // Whether the authorization charges ran out of gas. EELS runs set_delegation before
      // prepare_dispatch, so an authorization out-of-gas means dispatch prep never starts and the
      // recipient is never loaded at all.
      final boolean authChargeHalted =
          initialFrame.getState() == MessageFrame.State.EXCEPTIONAL_HALT;
      // EIP-7928 (v7.1.0): once the authorization charges clear, dispatch prep loads the delegation
      // transaction's recipient — record it in the block access list. EELS prepare_dispatch reads
      // the
      // recipient's account *before* charging it, so the recipient stays in the list even when its
      // own entry charge below then runs out of gas; only an authorization out-of-gas (which
      // precedes the load) leaves it out.
      if (transaction.getType().equals(TransactionType.DELEGATE_CODE) && !authChargeHalted) {
        accessLocationTracker.ifPresent(
            t -> t.addTouchedAccount(transaction.getTo().orElseThrow()));
      }
      // Skip the entry charge if the intrinsic state-gas charge already halted the frame. The
      // recipient's NEW_ACCOUNT (value materialising an empty leaf) is measured here because the
      // leaf rolls back if the transaction fails, so the charge is refunded below — unlike an
      // authorization's state gas, whose delegation survives a dispatch failure.
      StateCharge recipientCharge = StateCharge.NONE;
      if (!authChargeHalted) {
        final StateGasMark mark = StateGasMark.of(initialFrame);
        stateGasCalc.chargeTransactionEntry(initialFrame, gasCalculator, frameWorldState);
        recipientCharge = mark.chargeSince(initialFrame);
      }
      // Whether any top-frame prep charge (authorization or dispatch entry) ran out of gas. The two
      // share one snapshot in EELS, so either one leaves the Amsterdam delegations uncommitted
      // (rolled back); a later dispatch failure keeps them.
      final boolean prepChargeHalted =
          initialFrame.getState() == MessageFrame.State.EXCEPTIONAL_HALT;
      // EIP-8037: Advance the undo mark so intrinsic state gas charges (auth delegation,
      // contract creation) are not rolled back if the initial frame's execution reverts.
      // These are transaction-level costs that persist regardless of execution outcome.
      initialFrame.advanceUndoMark();
      // EIP-8037: the intrinsic/top-frame state-gas charges above drew from gasRemaining when the
      // reservoir was empty, which would otherwise count as frame spill. In EELS these intrinsic
      // costs are pre-deducted (the frame's state_gas_spilled starts at zero), and their refund on
      // a failed contract-creation tx is credited solely to the reservoir via
      // refundTxCreateIntrinsicStateGas. Clearing the spill here prevents the frame-failure handler
      // from also returning that intrinsic spill to gasRemaining on revert (a double refund); only
      // genuine execution-time spill is tracked from this point.
      initialFrame.resetStateGasSpilled();

      Deque<MessageFrame> messageFrameStack = initialFrame.getMessageFrameStack();
      while (!messageFrameStack.isEmpty()) {
        process(messageFrameStack.peekFirst(), operationTracer);
      }

      // EIP-8037: Runtime TX_MAX_GAS_LIMIT enforcement on regular gas only.
      // With multidimensional gas, tx.gasLimit can exceed TX_MAX_GAS_LIMIT to accommodate
      // state gas, but regular gas consumption is still bounded at runtime.
      // For pre-Amsterdam forks, transactionRegularGasLimit() returns Long.MAX_VALUE (always
      // passes).
      // We also need to include leftover reservoir in remaining gas for correct consumption
      // calculation
      final long totalRemaining =
          initialFrame.getRemainingGas() + initialFrame.getStateGasReservoir();
      final long totalConsumed = transaction.getGasLimit() - totalRemaining;
      final long regularConsumed = totalConsumed - initialFrame.getStateGasUsed();
      final boolean regularGasLimitExceeded =
          regularConsumed > stateGasCalc.transactionRegularGasLimit();
      if (regularGasLimitExceeded) {
        LOG.debug(
            "Transaction {} regular gas {} exceeds TX_MAX_GAS_LIMIT {}, reverting",
            transaction.getHash(),
            regularConsumed,
            stateGasCalc.transactionRegularGasLimit());
      }

      final boolean txSucceeded =
          initialFrame.getState() == MessageFrame.State.COMPLETED_SUCCESS
              && !regularGasLimitExceeded;

      if (txSucceeded) {
        worldUpdater.commit();
        // Amsterdam: fold the frame's execution changes down into the world state (the deferred
        // delegation updater is the frame's base, so it must be committed after worldUpdater).
        if (deferredDelegationUpdater != null) {
          deferredDelegationUpdater.commit();
        }
        // EIP-8037 (#3116): the creation NEW_ACCOUNT was charged only when the target was not
        // alive, and a successful creation adds the account, so the charge stands — no refund.
      } else {
        // Amsterdam: the dispatch failed but preparation succeeded, so the applied delegations
        // persist (EIP-7702) even though the frame's execution changes (in worldUpdater) are
        // discarded. A prep out-of-gas instead leaves the delegations uncommitted (rolled back).
        if (deferredDelegationUpdater != null && !prepChargeHalted) {
          deferredDelegationUpdater.commit();
        }
        if (initialFrame.getExceptionalHaltReason().isPresent()) {
          validationResult =
              ValidationResult.invalid(
                  TransactionInvalidReason.EXECUTION_HALTED,
                  initialFrame.getExceptionalHaltReason().get().getDescription());
        }
        if (regularGasLimitExceeded) {
          validationResult =
              ValidationResult.invalid(
                  TransactionInvalidReason.EXECUTION_HALTED,
                  "Regular gas consumption exceeds TX_MAX_GAS_LIMIT");
        }
        // EIP-8037 (#3116): neither the created contract's leaf nor a recipient leaf materialised
        // by
        // value survives a failed transaction, so their top-frame NEW_ACCOUNT charges are refilled.
        // Only what was actually charged is refunded (a charge that ran out of gas consumed
        // nothing,
        // and refilling it would inflate the reservoir and drive state gas negative). An
        // authorization's state gas is deliberately NOT refunded here: its delegation persists
        // through a dispatch failure.
        //
        // Where the credit lands decides who pays. Mirroring EELS refill_frame_state_gas, the
        // reservoir-drawn part is restored while the spilled part rides gasRemaining: a revert
        // preserves it (returned to the sender), an exceptional halt burns it along with the rest
        // of
        // gasRemaining (so the sender pays the full gas limit). Crediting the spill back on a halt
        // would hide it in the reservoir, which is never burned.
        if (stateGasCalc.isActive()) {
          final boolean burnsAllGas =
              initialFrame.getExceptionalHaltReason().isPresent() || regularGasLimitExceeded;
          refundRolledBackStateGas(
              initialFrame, stateGasCalc, topFrameCharges.create(), burnsAllGas);
          refundRolledBackStateGas(initialFrame, stateGasCalc, recipientCharge, burnsAllGas);
          // The authorizations' state gas is refunded only when a preparation charge ran out of
          // gas:
          // the whole preparation shares one snapshot, so the delegations roll back with it —
          // whether
          // the shortfall hit an authorization or the recipient charge that follows them. It is NOT
          // refunded on a dispatch failure, where the applied delegations persist (EIP-7702).
          if (prepChargeHalted) {
            refundRolledBackStateGas(
                initialFrame, stateGasCalc, topFrameCharges.authorizations(), burnsAllGas);
          }
        }
      }

      // TODO SLD are the log correct following EIP-7623?
      if (LOG.isTraceEnabled()) {
        LOG.trace(
            "Gas used by transaction: {}, by message call/contract creation: {}",
            transaction.getGasLimit() - initialFrame.getRemainingGas(),
            gasAvailable - initialFrame.getRemainingGas());
      }

      // Refund the sender by what we should and pay the miner fee (note that we're doing them one
      // after the other so that if it is the same account somehow, we end up with the right result)
      // EIP-8037: No refund when regular gas limit is exceeded (all gas consumed)
      final long refundedGas =
          regularGasLimitExceeded
              ? 0L
              : gasCalculator.calculateGasRefund(transaction, initialFrame, codeDelegationRefund);
      final Wei refundedWei = transactionGasPrice.multiply(refundedGas);
      final Wei balancePriorToRefund = sender.getBalance();
      sender.incrementBalance(refundedWei);
      LOG.atTrace()
          .setMessage("refunded sender {}  {} wei ({} -> {})")
          .addArgument(senderAddress)
          .addArgument(refundedWei)
          .addArgument(balancePriorToRefund)
          .addArgument(sender.getBalance())
          .log();
      // Calculate gas used: max of execution gas and transaction floor cost (EIP-7623)
      // For pre-Prague forks, floor cost is 0, so this returns just execution gas
      // For Prague+ forks with EIP-7778, this ensures block gas accounts for data floor
      // EIP-8037: Gas accounting with multidimensional gas support
      final long floorCost = gasCalculator.transactionFloorCost(transaction);
      final TransactionGasAccounting.GasResult gasResult =
          TransactionGasAccounting.builder()
              .txGasLimit(transaction.getGasLimit())
              .remainingGas(initialFrame.getRemainingGas())
              .stateGasReservoir(initialFrame.getStateGasReservoir())
              .stateGasUsed(initialFrame.getStateGasUsed())
              .refundedGas(refundedGas)
              .floorCost(floorCost)
              .regularGasLimitExceeded(regularGasLimitExceeded)
              .build()
              .calculate();
      final long effectiveStateGas = gasResult.effectiveStateGas();
      final long gasUsedByTransaction = gasResult.gasUsedByTransaction();
      final long usedGas = gasResult.usedGas();
      if (Eip8037Trace.ENABLED) {
        Eip8037Trace.txEnd(
            gasUsedByTransaction, effectiveStateGas, initialFrame.getStateGasReservoir());
      }
      final CoinbaseFeePriceCalculator coinbaseCalculator;
      if (blockHeader.getBaseFee().isPresent()) {
        final Wei baseFee = blockHeader.getBaseFee().get();
        final boolean gasPriceBelowBaseFee = transactionGasPrice.compareTo(baseFee) < 0;
        if (transactionValidationParams.allowUnderpriced()
            || transactionValidationParams.isPreserveCallerGasPricing()) {
          coinbaseCalculator =
              gasPriceBelowBaseFee ? (a, b, c) -> Wei.ZERO : coinbaseFeePriceCalculator;
        } else {
          if (gasPriceBelowBaseFee) {
            final Optional<PartialBlockAccessView> partialBlockAccessView =
                accessLocationTracker.map(
                    tracker -> tracker.createPartialBlockAccessView(worldState));
            return TransactionProcessingResult.failed(
                gasUsedByTransaction,
                refundedGas,
                usedGas,
                effectiveStateGas,
                ValidationResult.invalid(
                    TransactionInvalidReason.TRANSACTION_PRICE_TOO_LOW,
                    "transaction price must be greater than base fee"),
                Optional.empty(),
                Optional.empty(),
                partialBlockAccessView);
          }
          coinbaseCalculator = coinbaseFeePriceCalculator;
        }
      } else {
        coinbaseCalculator = CoinbaseFeePriceCalculator.frontier();
      }

      final Wei coinbaseWeiDelta =
          coinbaseCalculator.price(usedGas, transactionGasPrice, blockHeader.getBaseFee());

      operationTracer.traceBeforeRewardTransaction(worldUpdater, transaction, coinbaseWeiDelta);

      // EIP-158 & EIP-7928: coinbase is considered "touched" even when fees are zero.
      // Touching ensures an *empty* coinbase can be deleted during state clearing.
      final MutableAccount coinbase = worldState.getOrCreate(miningBeneficiary);
      accessLocationTracker.ifPresent(t -> t.addTouchedAccount(miningBeneficiary));
      if (!coinbaseWeiDelta.isZero()) {
        coinbase.incrementBalance(coinbaseWeiDelta);
      }

      // EIP-7708: Emit closure (burn) logs for self-destructed accounts whose balance is burned.
      // EIP-8246 preserves the balance instead of burning it, so no closure log is emitted then.
      if (!gasCalculator.isSelfDestructBalancePreserved()) {
        transferLogEmitter.emitClosureLogs(
            worldState, initialFrame.getSelfDestructs(), initialFrame::addLog);
      }

      operationTracer.traceEndTransaction(
          worldState.updater(),
          transaction,
          txSucceeded,
          initialFrame.getOutputData(),
          initialFrame.getLogs(),
          gasUsedByTransaction,
          initialFrame.getSelfDestructs(),
          0L);

      settleSelfDestructs(worldState, initialFrame.getSelfDestructs());

      if (clearEmptyAccounts) {
        worldState.clearAccountsThatAreEmpty();
      }

      final Optional<PartialBlockAccessView> partialBlockAccessView =
          accessLocationTracker.map(tracker -> tracker.createPartialBlockAccessView(worldState));

      if (txSucceeded) {
        final TransactionProcessingResult successResult =
            TransactionProcessingResult.successful(
                initialFrame.getLogs(),
                gasUsedByTransaction,
                refundedGas,
                usedGas,
                effectiveStateGas,
                initialFrame.getOutputData(),
                partialBlockAccessView,
                validationResult);
        successResult.setRegularGasUsedForBlock(gasResult.regularGas());
        return successResult;
      } else {
        if (initialFrame.getExceptionalHaltReason().isPresent()) {
          LOG.debug(
              "Transaction {} processing halted: {}",
              transaction.getHash(),
              initialFrame.getExceptionalHaltReason().get());
        }
        if (initialFrame.getRevertReason().isPresent()) {
          LOG.debug(
              "Transaction {} reverted: {}",
              transaction.getHash(),
              initialFrame.getRevertReason().get());
        }
        final TransactionProcessingResult failedResult =
            TransactionProcessingResult.failed(
                gasUsedByTransaction,
                refundedGas,
                usedGas,
                effectiveStateGas,
                validationResult,
                initialFrame.getRevertReason(),
                initialFrame.getExceptionalHaltReason(),
                partialBlockAccessView);
        failedResult.setRegularGasUsedForBlock(gasResult.regularGas());
        return failedResult;
      }
    } catch (final MerkleTrieException re) {
      operationTracer.traceEndTransaction(
          worldState.updater(),
          transaction,
          false,
          Bytes.EMPTY,
          List.of(),
          0,
          EMPTY_ADDRESS_SET,
          0L);

      // need to throw to trigger the heal
      throw re;
    } catch (final RuntimeException re) {
      final var cause = re.getCause();
      // in case of an interruption then just return without calling any other tracing method
      if (cause != null && cause instanceof InterruptedException) {
        LOG.atDebug()
            .setMessage("Interrupted while processing the transaction with hash {}")
            .addArgument(transaction::getHash)
            .log();
        return TransactionProcessingResult.invalid(
            ValidationResult.invalid(TransactionInvalidReason.EXECUTION_INTERRUPTED));
      }

      operationTracer.traceEndTransaction(
          worldState.updater(),
          transaction,
          false,
          Bytes.EMPTY,
          List.of(),
          0,
          EMPTY_ADDRESS_SET,
          0L);

      LOG.error("Critical Exception Processing Transaction", re);
      return TransactionProcessingResult.invalid(
          ValidationResult.invalid(
              TransactionInvalidReason.INTERNAL_ERROR,
              "Internal Error in Besu - " + re + "\n" + printableStackTraceFromThrowable(re)));
    }
  }

  public void process(final MessageFrame frame, final OperationTracer operationTracer) {
    final AbstractMessageProcessor executor = getMessageProcessor(frame.getType());

    executor.process(frame, operationTracer);
  }

  public AbstractMessageProcessor getMessageProcessor(final MessageFrame.Type type) {
    return switch (type) {
      case MESSAGE_CALL -> messageCallProcessor;
      case CONTRACT_CREATION -> contractCreationProcessor;
    };
  }

  public MessageCallProcessor getMessageCallProcessor() {
    return messageCallProcessor;
  }

  public boolean getClearEmptyAccounts() {
    return clearEmptyAccounts;
  }

  public GasCalculator getGasCalculator() {
    return gasCalculator;
  }

  /**
   * EIP-8037: Initializes the state gas reservoir for Amsterdam+ forks. When multidimensional gas
   * is active, regular gas is capped at {@code TX_MAX_GAS_LIMIT - intrinsic}; gas beyond that cap
   * goes into the state gas reservoir. No-op for pre-Amsterdam forks.
   */
  private static void initializeStateGasReservoir(
      final MessageFrame initialFrame,
      final long gasAvailable,
      final long intrinsicRegularGas,
      final StateGasCostCalculator stateGasCalc) {
    if (!stateGasCalc.isActive()) {
      return;
    }
    final long regularBudget =
        Math.max(0L, stateGasCalc.transactionRegularGasLimit() - intrinsicRegularGas);
    final long gasLeft = Math.min(regularBudget, gasAvailable);
    final long reservoir = gasAvailable - gasLeft;
    initialFrame.setGasRemaining(gasLeft);
    initialFrame.setStateGasReservoir(reservoir);
  }

  /**
   * Settles accounts marked for self-destruction at transaction finalization. Under EIP-8246 each
   * account is cleared (nonce reset, code and storage removed) but keeps its balance — EIP-161
   * state clearing then removes any account left with a zero balance. Pre-EIP-8246 the accounts are
   * deleted outright.
   */
  private void settleSelfDestructs(
      final WorldUpdater worldState, final Set<Address> selfDestructs) {
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

  /**
   * A top-frame state-gas charge, split into the total consumed and the part that spilled out of
   * gasRemaining rather than being drawn from the reservoir. The split matters when the charge is
   * refunded: the reservoir-drawn part is credited back, while the spilled part follows
   * gasRemaining and is burned if the frame exceptionally halts.
   */
  private record StateCharge(long amount, long spilled) {
    private static final StateCharge NONE = new StateCharge(0L, 0L);
  }

  /**
   * A snapshot of the frame's state-gas counters, taken before a top-frame charge so that {@link
   * #chargeSince} can report what that charge consumed. Both figures are read straight off the
   * counters {@link MessageFrame} already maintains as it drains the reservoir and spills into
   * gasRemaining, so the reservoir-versus-spill routing rule lives in one place.
   */
  private record StateGasMark(long used, long spilled) {

    static StateGasMark of(final MessageFrame frame) {
      return new StateGasMark(frame.getStateGasUsed(), frame.getStateGasSpilled());
    }

    StateCharge chargeSince(final MessageFrame frame) {
      return new StateCharge(frame.getStateGasUsed() - used, frame.getStateGasSpilled() - spilled);
    }
  }

  /** The top-frame state-gas charges of a creation / delegation transaction. */
  private record TopFrameCharges(StateCharge create, StateCharge authorizations) {}

  /**
   * Refunds a top-frame charge whose state effect rolled back with the failed transaction. A charge
   * that ran out of gas consumed nothing, so it is a no-op.
   */
  private static void refundRolledBackStateGas(
      final MessageFrame initialFrame,
      final StateGasCostCalculator stateGasCalc,
      final StateCharge charge,
      final boolean burnsAllGas) {
    if (charge.amount() > 0L) {
      stateGasCalc.refundFailedTopFrameStateGas(
          initialFrame, charge.amount(), burnsAllGas ? charge.spilled() : 0L);
    }
  }

  /**
   * Charges the top-frame state gas of a creation / delegation transaction, halting the frame on
   * out-of-gas, and reports what each charge consumed so the failure path can refund it.
   */
  private static TopFrameCharges chargeIntrinsicStateGas(
      final MessageFrame initialFrame,
      final Transaction transaction,
      final GasCalculator gasCalculator,
      final StateGasCostCalculator stateGasCalc,
      final boolean createTargetAlreadyAlive,
      final List<CodeDelegationResult.AuthorityAccess> delegationAccesses,
      final Optional<AccessLocationTracker> accessLocationTracker) {
    boolean outOfGas = false;
    StateCharge create = StateCharge.NONE;
    StateCharge authorizations = StateCharge.NONE;
    if (transaction.isContractCreation() && !createTargetAlreadyAlive) {
      // EIP-8037 (#3116): the created account's NEW_ACCOUNT state gas is charged at the top frame
      // only when the deployment target is not already alive; refilled on a failed create. A charge
      // that runs out of gas consumes nothing, so it measures as StateCharge.NONE and is not
      // refunded.
      final StateGasMark mark = StateGasMark.of(initialFrame);
      outOfGas = !stateGasCalc.chargeCreateStateGas(initialFrame);
      create = mark.chargeSince(initialFrame);
    }
    if (!outOfGas && transaction.getType().equals(TransactionType.DELEGATE_CODE)) {
      // A partial out-of-gas still leaves the earlier authorizations' state gas consumed; the whole
      // preparation shares one snapshot, so the failure path refunds it whenever any prep charge
      // halts — including the recipient's, charged after these.
      final StateGasMark mark = StateGasMark.of(initialFrame);
      outOfGas =
          !chargeCodeDelegationAccesses(
              initialFrame,
              stateGasCalc,
              gasCalculator.getAccountWriteGasCost(),
              delegationAccesses,
              accessLocationTracker);
      authorizations = mark.chargeSince(initialFrame);
    }
    if (outOfGas) {
      initialFrame.setExceptionalHaltReason(Optional.of(ExceptionalHaltReason.INSUFFICIENT_GAS));
      initialFrame.setState(MessageFrame.State.EXCEPTIONAL_HALT);
    }
    return new TopFrameCharges(create, authorizations);
  }

  /**
   * EIP-2780 (devnet-7): replays each authorization's top-frame access in transaction order,
   * mirroring EELS {@code set_delegation}. For every authorization it first touches the authority
   * (recording it in the EIP-7928 block access list — EELS adds the authority to the accessed set
   * in {@code validate_authorization}, before the charge), then charges its state-dependent costs
   * in order: NEW_ACCOUNT (state) when the authority's leaf was created, ACCOUNT_WRITE (regular) on
   * the transaction's first write to it, and AUTH_BASE (state) for a net-new delegation. The first
   * charge that cannot be afforded stops the replay and returns false, so a partial out-of-gas
   * leaves exactly the authorities reached (up to and including the one being charged) in the block
   * access list — the later authorities are never touched. Charges never partially consume gas, so
   * the frame state is consistent on the stopping authority. Whatever the earlier authorizations
   * did consume is refunded by the caller's failure path, which rolls back the whole preparation.
   *
   * @return true if every authorization was charged, false on the first out-of-gas
   */
  private static boolean chargeCodeDelegationAccesses(
      final MessageFrame initialFrame,
      final StateGasCostCalculator stateGasCalc,
      final long accountWriteCost,
      final List<CodeDelegationResult.AuthorityAccess> delegationAccesses,
      final Optional<AccessLocationTracker> accessLocationTracker) {
    final AccessLocationTracker tracker = accessLocationTracker.orElse(null);
    for (final CodeDelegationResult.AuthorityAccess access : delegationAccesses) {
      if (tracker != null) {
        tracker.addTouchedAccount(access.authority());
      }
      if (access.newAccount() && !initialFrame.consumeStateGas(stateGasCalc.newAccountStateGas())) {
        return false;
      }
      if (access.accountWrite()) {
        if (initialFrame.getRemainingGas() < accountWriteCost) {
          return false;
        }
        initialFrame.decrementRemainingGas(accountWriteCost);
      }
      if (access.authBase() && !initialFrame.consumeStateGas(stateGasCalc.authBaseStateGas())) {
        return false;
      }
    }
    return true;
  }

  private String printableStackTraceFromThrowable(final RuntimeException re) {
    final StringBuilder builder = new StringBuilder();

    for (final StackTraceElement stackTraceElement : re.getStackTrace()) {
      builder.append("\tat ").append(stackTraceElement.toString()).append("\n");
    }

    return builder.toString();
  }

  private Code processCodeFromAccount(
      final WorldUpdater worldUpdater,
      final Set<Address> warmAddressList,
      final Account contract,
      final Optional<AccessLocationTracker> accessLocationTracker) {
    if (contract == null) {
      return Code.EMPTY_CODE;
    }

    final Hash codeHash = contract.getCodeHash();
    if (codeHash == null || codeHash.equals(Hash.EMPTY)) {
      return Code.EMPTY_CODE;
    }

    if (hasCodeDelegation(contract.getCode())) {
      return delegationTargetCode(worldUpdater, warmAddressList, contract, accessLocationTracker);
    }

    // Bonsai accounts may have a fully cached code, so we use that one
    if (contract.getCodeCache() != null) {
      return contract.getOrCreateCachedCode();
    }

    // Any other account can only use the cached jump dest analysis if available
    return messageCallProcessor.getOrCreateCachedJumpDest(
        contract.getCodeHash(), contract.getCode());
  }

  private Code delegationTargetCode(
      final WorldUpdater worldUpdater,
      final Set<Address> warmAddressList,
      final Account contract,
      final Optional<AccessLocationTracker> accessLocationTracker) {
    final boolean stateGasActive = gasCalculator.stateGasCostCalculator().isActive();
    // EIP-7928: under state-gas metering EELS loads the delegation target only after the top-frame
    // delegation access charge is paid, so a charge that runs out of gas must leave the target out
    // of
    // the block access list. Besu resolves the code eagerly to build the frame, so the target is
    // not
    // recorded here — chargeTransactionEntry records it once the charge succeeds. Pre-Amsterdam
    // forks
    // charge no top-frame delegation access, so they record it here as before.
    //
    // we need to look up the target account and its code, but do NOT charge gas for it
    final CodeDelegationHelper.Target target =
        getTarget(
            worldUpdater,
            gasCalculator::isPrecompile,
            contract,
            stateGasActive ? Optional.empty() : accessLocationTracker);
    // EIP-2780 (devnet-7, #3045): under state-gas metering the top-frame delegation access is
    // warm/cold-aware and charged in chargeTransactionEntry, which also warms the target. Pre-
    // warming it here would make that access always appear warm. Pre-Amsterdam forks charge no
    // top-frame delegation access, so the target is warmed here to match their accessed-address
    // set.
    if (!stateGasActive) {
      warmAddressList.add(target.address());
    }

    return target.code();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private GasCalculator gasCalculator;
    private TransactionValidatorFactory transactionValidatorFactory;
    private ContractCreationProcessor contractCreationProcessor;
    private MessageCallProcessor messageCallProcessor;
    private boolean clearEmptyAccounts;
    private boolean warmCoinbase;
    private int maxStackSize;
    private FeeMarket feeMarket;
    private CoinbaseFeePriceCalculator coinbaseFeePriceCalculator;
    private CodeDelegationProcessor codeDelegationProcessor;
    private TransferLogEmitter transferLogEmitter = TransferLogEmitter.NOOP;

    public Builder gasCalculator(final GasCalculator gasCalculator) {
      this.gasCalculator = gasCalculator;
      return this;
    }

    public Builder transactionValidatorFactory(
        final TransactionValidatorFactory transactionValidatorFactory) {
      this.transactionValidatorFactory = transactionValidatorFactory;
      return this;
    }

    public Builder contractCreationProcessor(
        final ContractCreationProcessor contractCreationProcessor) {
      this.contractCreationProcessor = contractCreationProcessor;
      return this;
    }

    public Builder messageCallProcessor(final MessageCallProcessor messageCallProcessor) {
      this.messageCallProcessor = messageCallProcessor;
      return this;
    }

    public Builder clearEmptyAccounts(final boolean clearEmptyAccounts) {
      this.clearEmptyAccounts = clearEmptyAccounts;
      return this;
    }

    public Builder warmCoinbase(final boolean warmCoinbase) {
      this.warmCoinbase = warmCoinbase;
      return this;
    }

    public Builder maxStackSize(final int maxStackSize) {
      this.maxStackSize = maxStackSize;
      return this;
    }

    public Builder feeMarket(final FeeMarket feeMarket) {
      this.feeMarket = feeMarket;
      return this;
    }

    public Builder coinbaseFeePriceCalculator(
        final CoinbaseFeePriceCalculator coinbaseFeePriceCalculator) {
      this.coinbaseFeePriceCalculator = coinbaseFeePriceCalculator;
      return this;
    }

    public Builder codeDelegationProcessor(
        final CodeDelegationProcessor maybeCodeDelegationProcessor) {
      this.codeDelegationProcessor = maybeCodeDelegationProcessor;
      return this;
    }

    public Builder transferLogEmitter(final TransferLogEmitter transferLogEmitter) {
      this.transferLogEmitter = transferLogEmitter;
      return this;
    }

    public Builder populateFrom(final MainnetTransactionProcessor processor) {
      this.gasCalculator = processor.gasCalculator;
      this.transactionValidatorFactory = processor.transactionValidatorFactory;
      this.contractCreationProcessor = processor.contractCreationProcessor;
      this.messageCallProcessor = processor.messageCallProcessor;
      this.clearEmptyAccounts = processor.clearEmptyAccounts;
      this.warmCoinbase = processor.warmCoinbase;
      this.maxStackSize = processor.maxStackSize;
      this.feeMarket = processor.feeMarket;
      this.coinbaseFeePriceCalculator = processor.coinbaseFeePriceCalculator;
      this.codeDelegationProcessor = processor.maybeCodeDelegationProcessor.orElse(null);
      this.transferLogEmitter = processor.transferLogEmitter;
      return this;
    }

    public MainnetTransactionProcessor build() {
      return new MainnetTransactionProcessor(
          gasCalculator,
          transactionValidatorFactory,
          contractCreationProcessor,
          messageCallProcessor,
          clearEmptyAccounts,
          warmCoinbase,
          maxStackSize,
          feeMarket,
          coinbaseFeePriceCalculator,
          codeDelegationProcessor,
          transferLogEmitter);
    }
  }
}
