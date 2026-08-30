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

      final List<AccessListEntry> eip2930AccessListEntries =
          transaction.getAccessList().orElse(List.of());
      // we need to keep a separate hash set of addresses in case they specify no storage.
      // No-storage is a common pattern, especially for Externally Owned Accounts
      final Multimap<Address, Bytes32> eip2930StorageList = HashMultimap.create();
      for (final var entry : eip2930AccessListEntries) {
        final Address address = entry.address();
        eip2930WarmAddressList.add(address);
        final List<Bytes32> storageKeys = entry.storageKeys();
        eip2930StorageList.putAll(address, storageKeys);
      }
      if (warmCoinbase) {
        eip2930WarmAddressList.add(miningBeneficiary);
      }

      final long intrinsicRegularGas = gasCalculator.transactionIntrinsicRegularGas(transaction);
      final var stateGasCalc = gasCalculator.stateGasCostCalculator();

      // EIP-2780 charges every state-dependent cost at the top frame, so the intrinsic is
      // entirely regular gas and an unaffordable charge halts the frame rather than invalidating
      // the transaction. Checked before frame construction to reject at the intrinsic level.
      if (transaction.getGasLimit() < intrinsicRegularGas) {
        LOG.trace(
            "Insufficient gas for intrinsic cost: gasLimit={}, intrinsic={}",
            transaction.getGasLimit(),
            intrinsicRegularGas);
        return TransactionProcessingResult.invalid(
            ValidationResult.invalid(
                TransactionInvalidReason.INTRINSIC_GAS_EXCEEDS_GAS_LIMIT,
                String.format(
                    "intrinsic gas cost %d exceeds gas limit %d",
                    intrinsicRegularGas, transaction.getGasLimit())));
      }

      // Amsterdam charges authorizations at the top frame with no refund; pre-Amsterdam reserves
      // the worst case in the intrinsic and refunds it here.
      long codeDelegationRefund = 0L;
      // Empty pre-Amsterdam and for non-delegation transactions.
      List<CodeDelegationResult.AuthorityAccess> delegationAccesses = List.of();
      // Holds the applied delegations uncommitted until the top-frame preparation charges clear,
      // so a preparation out-of-gas rolls them back while a later dispatch failure keeps them.
      // Null pre-Amsterdam, which has no preparation charge to fail, and when there are no
      // authorizations.
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
          // Defer the commit so a preparation out-of-gas can roll the delegations back.
          delegationAccesses = codeDelegationResult.authorityAccesses();
          deferredDelegationUpdater = delegationUpdater;
        } else {
          // The intrinsic already validated the charge, so there is nothing left to fail.
          codeDelegationRefund =
              gasCalculator.calculateDelegateCodeGasRefund(
                  codeDelegationResult.alreadyExistingDelegators());
          delegationUpdater.commit();
        }
      }

      // The frame has to see the applied delegations, which may still be uncommitted.
      final WorldUpdater frameWorldState =
          deferredDelegationUpdater != null ? deferredDelegationUpdater : worldState;

      final long gasAvailable = transaction.getGasLimit() - intrinsicRegularGas;
      LOG.trace(
          "Gas available for execution {} = {} - {} (limit - intrinsic)",
          gasAvailable,
          transaction.getGasLimit(),
          intrinsicRegularGas);

      // EIP-8037: regular gas is capped at TX_MAX_GAS_LIMIT, so anything bought above that cap can
      // only ever be spent as state gas and starts in the reservoir. Pre-Amsterdam the cap is
      // Long.MAX_VALUE, leaving the whole budget regular.
      final long regularBudget =
          Math.max(0L, stateGasCalc.transactionRegularGasLimit() - intrinsicRegularGas);
      final long initialGas = Math.min(regularBudget, gasAvailable);
      final long initialStateGasReservoir = gasAvailable - initialGas;

      final WorldUpdater worldUpdater = frameWorldState.updater();

      operationTracer.traceStartTransaction(worldUpdater, transaction);

      final MessageFrame.Builder commonMessageFrameBuilder =
          MessageFrame.builder()
              .maxStackSize(maxStackSize)
              .worldUpdater(worldUpdater.updater())
              .initialGas(initialGas)
              .initialStateGasReservoir(initialStateGasReservoir)
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
      // EIP-8037: an already-alive (e.g. pre-funded) target adds no leaf, so it owes no
      // NEW_ACCOUNT.
      boolean createTargetAlreadyAlive = false;
      if (transaction.isContractCreation()) {
        final Address contractAddress =
            Address.contractAddress(senderAddress, sender.getNonce() - 1L);
        // Nothing reads it pre-Amsterdam, so skip the lookup.
        if (stateGasCalc.isActive()) {
          final Account existingTarget = frameWorldState.get(contractAddress);
          createTargetAlreadyAlive = existingTarget != null && !existingTarget.isEmpty();
        }
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

      // EIP-2780: the state-dependent costs are charged against the frame, on the transaction's
      // pre-state, before any opcode runs.
      final PrepCharges prepCharges =
          chargeTopFrame(
              initialFrame,
              transaction,
              frameWorldState,
              stateGasCalc,
              createTargetAlreadyAlive,
              delegationAccesses);

      // Transaction-level state-gas charges persist regardless of the execution outcome, so put
      // them out of reach of a rollback.
      initialFrame.advanceUndoMark();
      // Those charges may have drawn from gasRemaining, which the failure handler would read as
      // frame spill and refund a second time. prepCharges already holds what it needs.
      initialFrame.resetStateGasSpilled();

      Deque<MessageFrame> messageFrameStack = initialFrame.getMessageFrameStack();
      while (!messageFrameStack.isEmpty()) {
        process(messageFrameStack.peekFirst(), operationTracer);
      }

      // Under two-dimensional gas, tx.gasLimit may exceed TX_MAX_GAS_LIMIT to accommodate state
      // gas, so the cap on regular gas has to be enforced separately here.
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
        // The deferred updater is the frame's base, so it commits after worldUpdater.
        if (deferredDelegationUpdater != null) {
          deferredDelegationUpdater.commit();
        }
        // EIP-8037: a successful creation adds the leaf it was charged for, so the charge stands.
      } else {
        // EIP-7702: delegations applied by a successful preparation outlive a dispatch failure,
        // unlike the frame's execution changes. A preparation out-of-gas leaves them rolled back.
        if (deferredDelegationUpdater != null && !prepCharges.halted()) {
          deferredDelegationUpdater.commit();
        }
        // A real halt reason is more specific, so it wins when both apply.
        if (initialFrame.getExceptionalHaltReason().isPresent()) {
          validationResult =
              ValidationResult.invalid(
                  TransactionInvalidReason.EXECUTION_HALTED,
                  initialFrame.getExceptionalHaltReason().get().getDescription());
        } else if (regularGasLimitExceeded) {
          validationResult =
              ValidationResult.invalid(
                  TransactionInvalidReason.EXECUTION_HALTED,
                  "Regular gas consumption exceeds TX_MAX_GAS_LIMIT");
        }
        // EIP-8037: no leaf the creation or the value transfer would have added survives a failed
        // transaction, so their charges come back. Only what was actually charged: refilling a
        // charge that ran out of gas would inflate the reservoir and drive state gas negative.
        if (stateGasCalc.isActive()) {
          final boolean burnsAllGas =
              initialFrame.getExceptionalHaltReason().isPresent() || regularGasLimitExceeded;
          refundRolledBackStateGas(initialFrame, prepCharges.create(), burnsAllGas);
          refundRolledBackStateGas(initialFrame, prepCharges.recipient(), burnsAllGas);
          // The whole preparation shares one snapshot, so any charge running out of gas rolls the
          // delegations back with it — and only then is their state gas owed back.
          if (prepCharges.halted()) {
            refundRolledBackStateGas(initialFrame, prepCharges.authorizations(), burnsAllGas);
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
      final long stateGasUsed = gasResult.effectiveStateGas();
      final long gasUsedByTransaction = gasResult.gasUsedByTransaction();
      final long usedGas = gasResult.usedGas();
      LOG.trace(
          "EIP-8037 TX_END gasUsed={} stateGasUsed={} reservoir={}",
          gasUsedByTransaction,
          stateGasUsed,
          initialFrame.getStateGasReservoir());
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
                stateGasUsed,
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

      // For a failed transaction all selfDestructs must have been rolled back by the frame.
      // Guard here as defense-in-depth: if any leak path (e.g. regularGasLimitExceeded) leaves
      // stale markers, we must not permanently delete accounts from the world state.
      final Set<Address> effectiveSelfDestructs =
          txSucceeded ? initialFrame.getSelfDestructs() : Set.of();

      // EIP-7708: Emit closure (burn) logs for self-destructed accounts whose balance is burned.
      // Noop before Amsterdam. EIP-8246 preserves the balance instead of burning it, so no
      // closure log is emitted then.
      if (!gasCalculator.isSelfDestructBalancePreserved()) {
        transferLogEmitter.emitClosureLogs(
            worldState, effectiveSelfDestructs, initialFrame::addLog);
      }

      operationTracer.traceEndTransaction(
          worldState.updater(),
          transaction,
          txSucceeded,
          initialFrame.getOutputData(),
          initialFrame.getLogs(),
          gasUsedByTransaction,
          effectiveSelfDestructs,
          0L);

      settleSelfDestructs(worldState, effectiveSelfDestructs);

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
                stateGasUsed,
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
                stateGasUsed,
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
   * A top-frame state-gas charge, split into the total consumed and the part that spilled out of
   * gasRemaining. The split decides where a refund lands, and so who ends up paying it.
   */
  private record StateCharge(long amount, long spilled) {
    private static final StateCharge NONE = new StateCharge(0L, 0L);
  }

  /**
   * A snapshot of the frame's state-gas counters, taken before a top-frame charge so {@link
   * #chargeSince} can report what it consumed. Reading the counters {@link MessageFrame} already
   * maintains keeps the reservoir-versus-spill routing rule in one place.
   */
  private record StateGasMark(long used, long spilled) {

    static StateGasMark of(final MessageFrame frame) {
      return new StateGasMark(frame.getStateGasUsed(), frame.getStateGasSpilled());
    }

    StateCharge chargeSince(final MessageFrame frame) {
      return new StateCharge(frame.getStateGasUsed() - used, frame.getStateGasSpilled() - spilled);
    }
  }

  /**
   * What the top frame's preparation phase consumed, per charge, so the failure path can refund the
   * charges whose state effect rolled back with the transaction.
   *
   * @param create the contract-creation NEW_ACCOUNT charge
   * @param authorizations the EIP-7702 per-authority charges, taken as a whole
   * @param recipient the dispatch-entry charge on the recipient
   * @param halted whether any of them ran out of gas, leaving the frame exceptionally halted
   */
  private record PrepCharges(
      StateCharge create, StateCharge authorizations, StateCharge recipient, boolean halted) {}

  /**
   * Refunds a top-frame charge whose state effect rolled back with the failed transaction. A charge
   * that ran out of gas consumed nothing, so it is a no-op.
   *
   * <p>Where the credit lands decides who pays. The spilled part rides gasRemaining, which a revert
   * returns to the sender and an exceptional halt burns; crediting it back to the reservoir instead
   * would hide it from that burn, since the reservoir never is.
   */
  private static void refundRolledBackStateGas(
      final MessageFrame initialFrame, final StateCharge charge, final boolean burnsAllGas) {
    if (charge.amount() <= 0L) {
      return;
    }
    final long burnedSpill = burnsAllGas ? charge.spilled() : 0L;
    final long credited = Math.max(0L, charge.amount() - burnedSpill);
    if (credited > 0L) {
      initialFrame.incrementStateGasReservoir(credited);
    }
    // stateGasUsed drops by the full charge either way: the leaf did not persist.
    initialFrame.decrementStateGasUsed(charge.amount());
  }

  /**
   * Runs the top frame's preparation phase before any opcode executes, in spec order: the
   * contract-creation charge, the per-authority delegation charges, then the dispatch-entry
   * recipient load and charge. The first unaffordable charge halts the frame and skips the rest,
   * which is what keeps a recipient the transaction never got to load out of the block access list.
   */
  private PrepCharges chargeTopFrame(
      final MessageFrame initialFrame,
      final Transaction transaction,
      final WorldUpdater frameWorldState,
      final StateGasCostCalculator stateGasCalc,
      final boolean createTargetAlreadyAlive,
      final List<CodeDelegationResult.AuthorityAccess> delegationAccesses) {
    // Pre-Amsterdam forks pay none of these charges, but still record the recipient load below.
    final boolean stateGasActive = stateGasCalc.isActive();
    boolean outOfGas = false;
    StateCharge create = StateCharge.NONE;
    StateCharge authorizations = StateCharge.NONE;
    StateCharge recipient = StateCharge.NONE;

    if (transaction.isContractCreation()) {
      // The created address was already recorded when the frame was built.
      if (stateGasActive && !createTargetAlreadyAlive) {
        // EIP-8037: the created account's NEW_ACCOUNT state gas is charged only when the
        // deployment target is not already alive; refilled on a failed create. A charge that runs
        // out of gas consumes nothing, so it measures as StateCharge.NONE and is not refunded.
        final StateGasMark mark = StateGasMark.of(initialFrame);
        outOfGas = !initialFrame.consumeStateGas(stateGasCalc.newContractStateGas());
        create = mark.chargeSince(initialFrame);
      }
    } else {
      if (stateGasActive && transaction.getType().equals(TransactionType.DELEGATE_CODE)) {
        // A partial out-of-gas still leaves the earlier authorizations' state gas consumed; the
        // whole preparation shares one snapshot, so the failure path refunds it whenever any prep
        // charge halts — including the recipient's, charged after these.
        final StateGasMark mark = StateGasMark.of(initialFrame);
        outOfGas = !chargeCodeDelegationAccesses(initialFrame, stateGasCalc, delegationAccesses);
        authorizations = mark.chargeSince(initialFrame);
      }
      if (!outOfGas) {
        final Address to = transaction.getTo().orElseThrow();
        // EIP-7928: the load precedes the charge, so the recipient stays listed even when its own
        // entry charge runs out of gas — but an authorization out-of-gas, which comes first,
        // leaves it out.
        initialFrame.getEip7928AccessList().ifPresent(bal -> bal.addTouchedAccount(to));
        if (stateGasActive) {
          // Measured because the leaf it pays for rolls back with a failed transaction, unlike a
          // delegation, which survives one.
          final StateGasMark mark = StateGasMark.of(initialFrame);
          outOfGas = !chargeTransactionEntry(initialFrame, frameWorldState, to, stateGasCalc);
          recipient = mark.chargeSince(initialFrame);
        }
      }
    }

    if (outOfGas) {
      initialFrame.setExceptionalHaltReason(Optional.of(ExceptionalHaltReason.INSUFFICIENT_GAS));
      initialFrame.setState(MessageFrame.State.EXCEPTIONAL_HALT);
    }
    return new PrepCharges(create, authorizations, recipient, outOfGas);
  }

  /**
   * EIP-2780: replays each authorization's top-frame access in transaction order, touching the
   * authority before charging it so an out-of-gas leaves the authorities after it out of the
   * EIP-7928 block access list. Charges never partially consume gas, so the frame stays consistent
   * on the authorization that stopped the replay; what the earlier ones consumed is refunded by the
   * caller's failure path.
   *
   * @return true if every authorization was charged, false on the first out-of-gas
   */
  private boolean chargeCodeDelegationAccesses(
      final MessageFrame initialFrame,
      final StateGasCostCalculator stateGasCalc,
      final List<CodeDelegationResult.AuthorityAccess> delegationAccesses) {
    final long accountWriteCost = gasCalculator.getAccountWriteGasCost();
    for (final CodeDelegationResult.AuthorityAccess access : delegationAccesses) {
      initialFrame
          .getEip7928AccessList()
          .ifPresent(bal -> bal.addTouchedAccount(access.authority()));
      if (access.newAccount()
          && !initialFrame.consumeStateGas(stateGasCalc.emptyAccountDelegationStateGas())) {
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

  /**
   * Charges the EIP-2780 dispatch-entry costs on the depth-0 frame of a non-create transaction:
   * NEW_ACCOUNT when value materialises an empty recipient leaf, then the access to a delegated
   * recipient's target. {@code worldState} is the transaction-level updater, which already has the
   * recipient cached from code resolution, so reading it pre-value-transfer costs no extra lookup.
   *
   * @return true if both charges were afforded, false on out-of-gas
   */
  private boolean chargeTransactionEntry(
      final MessageFrame initialFrame,
      final WorldUpdater worldState,
      final Address to,
      final StateGasCostCalculator stateGasCalc) {
    final Account recipient = worldState.get(to);
    // Positive value to a non-alive recipient. Precompiles are deliberately not excluded, since
    // a zero-balance precompile is not "alive" under EIP-161 either.
    if (!initialFrame.getValue().isZero()
        && (recipient == null || recipient.isEmpty())
        && !initialFrame.consumeStateGas(stateGasCalc.newAccountStateGas())) {
      return false;
    }
    // EIP-2780: the top-level access to a delegated recipient's target is warm/cold aware.
    if (recipient != null && hasCodeDelegation(recipient.getCode())) {
      final Address target = CodeDelegationHelper.getTargetAddress(recipient.getCode());
      // Precompiles are warm by construction, but warmUpAddress still has to run: its side effect
      // must stand for any later access.
      final boolean targetWasWarm =
          initialFrame.warmUpAddress(target) || gasCalculator.isPrecompile(target);
      final long delegationAccessCost =
          targetWasWarm
              ? gasCalculator.getWarmStorageReadCost()
              : gasCalculator.getColdAccountAccessCost();
      if (initialFrame.getRemainingGas() < delegationAccessCost) {
        return false;
      }
      initialFrame.decrementRemainingGas(delegationAccessCost);
      // EIP-7928: the target is loaded only once its access is paid for, so an access charge that
      // runs out of gas has to leave it out of the list entirely.
      initialFrame.getEip7928AccessList().ifPresent(bal -> bal.addTouchedAccount(target));
    }
    return true;
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
    // Besu resolves the delegation target eagerly to build the frame, but EIP-7928 only lists it
    // once its access charge is paid, so recording is left to chargeTransactionEntry. Pre-Amsterdam
    // forks charge no such access and so still record it here.
    //
    // we need to look up the target account and its code, but do NOT charge gas for it
    final boolean stateGasActive = gasCalculator.stateGasCostCalculator().isActive();
    final CodeDelegationHelper.Target target =
        getTarget(
            worldUpdater,
            gasCalculator::isPrecompile,
            contract,
            stateGasActive ? Optional.empty() : accessLocationTracker);
    // EIP-2780: pre-warming the target here would make the warm/cold-aware access charged in
    // chargeTransactionEntry always read warm. Pre-Amsterdam forks have no such charge, so they
    // still warm it here.
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
