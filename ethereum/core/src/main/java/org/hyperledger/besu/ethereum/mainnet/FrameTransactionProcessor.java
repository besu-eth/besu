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

import org.hyperledger.besu.collections.undo.UndoSet;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.FrameReceipt;
import org.hyperledger.besu.ethereum.core.FrameSignature;
import org.hyperledger.besu.ethereum.core.FrameTransactionGas;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.feemarket.CoinbaseFeePriceCalculator;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.frame.TxValues;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.ApproveOperation;
import org.hyperledger.besu.evm.processor.MessageCallProcessor;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes EIP-8141 frame transactions: validates the declared signatures, runs each frame as an
 * isolated top-level call with explicit execution and state gas budgets, applies the APPROVE-based
 * payment model, and settles gas per the EIP's two-dimensional accounting.
 */
public class FrameTransactionProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(FrameTransactionProcessor.class);

  private final GasCalculator gasCalculator;
  private final TransactionValidatorFactory transactionValidatorFactory;
  private final MessageCallProcessor messageCallProcessor;
  private final TopLevelTransactionSupport topLevelSupport;
  private final boolean clearEmptyAccounts;
  private final int maxStackSize;
  private final FeeMarket feeMarket;

  FrameTransactionProcessor(
      final GasCalculator gasCalculator,
      final TransactionValidatorFactory transactionValidatorFactory,
      final MessageCallProcessor messageCallProcessor,
      final TopLevelTransactionSupport topLevelSupport,
      final boolean clearEmptyAccounts,
      final int maxStackSize,
      final FeeMarket feeMarket) {
    this.gasCalculator = gasCalculator;
    this.transactionValidatorFactory = transactionValidatorFactory;
    this.messageCallProcessor = messageCallProcessor;
    this.topLevelSupport = topLevelSupport;
    this.clearEmptyAccounts = clearEmptyAccounts;
    this.maxStackSize = maxStackSize;
    this.feeMarket = feeMarket;
  }

  /** The mutable execution state of one frame while it is being processed. */
  private static final class FrameExecution {
    int status = FrameReceipt.STATUS_FAILURE;
    long executionGasUsed = 0L;
    List<Log> logs = List.of();
    boolean skipped = false;
  }

  /**
   * Applies a frame transaction to the current world state.
   *
   * @param worldState the current world state
   * @param blockHeader the current block header
   * @param transaction the frame transaction
   * @param miningBeneficiary the coinbase
   * @param operationTracer the tracer
   * @param blockHashLookup the BLOCKHASH lookup
   * @param transactionValidationParams the validation parameters
   * @param blobGasPrice the current blob gas price
   * @param accessLocationTracker the EIP-7928 access location tracker for this transaction, when
   *     the fork builds a block access list
   * @return the processing result
   */
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
    final var transactionValidator = transactionValidatorFactory.get();
    ValidationResult<TransactionInvalidReason> validationResult =
        transactionValidator.validate(
            transaction,
            blockHeader.getBaseFee(),
            Optional.ofNullable(blobGasPrice),
            transactionValidationParams);
    if (!validationResult.isValid()) {
      LOG.debug("Invalid frame transaction: {}", validationResult.getErrorMessage());
      return TransactionProcessingResult.invalid(validationResult);
    }

    final Address senderAddress = transaction.getSender();
    // EIP-7928: the declared sender is touched by the transaction regardless of which frames run.
    accessLocationTracker.ifPresent(t -> t.addTouchedAccount(senderAddress));
    final Account senderAtStart = worldState.get(senderAddress);

    // EIP-8141: tx.nonce must equal the sender's current nonce; the nonce is incremented by
    // APPROVE, not up front. EIP-3607 is deliberately not applied.
    final long senderNonce = senderAtStart == null ? 0L : senderAtStart.getNonce();
    if (transaction.getNonce() != senderNonce) {
      return TransactionProcessingResult.invalid(
          ValidationResult.invalid(
              transaction.getNonce() < senderNonce
                  ? TransactionInvalidReason.NONCE_TOO_LOW
                  : TransactionInvalidReason.NONCE_TOO_HIGH,
              String.format(
                  "transaction nonce %d does not match sender nonce %d",
                  transaction.getNonce(), senderNonce)));
    }

    final List<Frame> frames = transaction.getFrames().orElseThrow();
    final List<FrameSignature> signatures = transaction.getFrameSignatures().orElse(List.of());
    final Bytes32 signatureHash = Bytes32.wrap(transaction.getFrameSignatureHash().getBytes());

    for (final FrameSignature signature : signatures) {
      final Optional<FrameTransactionSignatureValidator.Invalidity> invalidity =
          FrameTransactionSignatureValidator.validate(signature, senderAddress, signatureHash);
      if (invalidity.isPresent()) {
        return TransactionProcessingResult.invalid(
            ValidationResult.invalid(invalidity.get().reason(), invalidity.get().message()));
      }
    }

    // Gas anchors.
    final long intrinsicGas = FrameTransactionGas.intrinsicGas(frames, signatures, senderAddress);
    final long calldataFloorGas =
        FrameTransactionGas.calldataFloorGas(frames, signatures, senderAddress);
    final long totalExecutionGas = FrameTransactionGas.totalExecutionGasLimit(frames);
    final long totalStateGas = FrameTransactionGas.totalStateGasLimit(frames);
    final long standardGasLimit = intrinsicGas + totalExecutionGas + totalStateGas;
    final long maxGas = transaction.getGasLimit();

    final Wei effectiveGasPrice =
        feeMarket.getTransactionPriceCalculator().price(transaction, blockHeader.getBaseFee());
    // Decided before any frame runs: an underpriced frame transaction has no fee-less failure mode.
    final Optional<CoinbaseFeePriceCalculator> coinbaseCalculator =
        topLevelSupport.coinbaseCalculator(
            blockHeader, effectiveGasPrice, transactionValidationParams);
    if (coinbaseCalculator.isEmpty()) {
      return TransactionProcessingResult.invalid(
          ValidationResult.invalid(
              TransactionInvalidReason.TRANSACTION_PRICE_TOO_LOW,
              "transaction price must be greater than base fee"));
    }
    final long blobGas = gasCalculator.blobGasCost(transaction.getBlobCount());
    final Wei blobFee = blobGasPrice == null ? Wei.ZERO : blobGasPrice.multiply(blobGas);
    final java.math.BigInteger maxCostExact =
        transaction
            .getMaxGasPrice()
            .getAsBigInteger()
            .multiply(java.math.BigInteger.valueOf(maxGas))
            .add(blobFee.getAsBigInteger());
    if (maxCostExact.bitLength() > 256) {
      return TransactionProcessingResult.invalid(
          ValidationResult.invalid(
              TransactionInvalidReason.UPFRONT_COST_EXCEEDS_UINT256,
              "frame transaction max cost exceeds 2^256 wei"));
    }
    final Wei maxCost = Wei.of(maxCostExact);

    // Build the EVM-visible transaction context.
    final List<FrameTransactionContext.FrameInfo> frameInfos = new ArrayList<>(frames.size());
    for (final Frame frame : frames) {
      frameInfos.add(
          new FrameTransactionContext.FrameInfo(
              frame.mode(),
              frame.flags(),
              frame.resolvedTarget(senderAddress),
              frame.executionGasLimit(),
              frame.stateGasLimit(),
              frame.value(),
              frame.data()));
    }
    final List<FrameTransactionContext.SignatureInfo> signatureInfos =
        new ArrayList<>(signatures.size());
    for (final FrameSignature signature : signatures) {
      final boolean arbitrary = signature.scheme() == FrameSignature.SCHEME_ARBITRARY;
      signatureInfos.add(
          new FrameTransactionContext.SignatureInfo(
              signature.scheme(),
              arbitrary ? null : signature.resolvedSigner(senderAddress),
              signature.msg().isEmpty() ? null : Bytes32.wrap(signature.msg()),
              arbitrary ? signature.signature() : null));
    }
    final FrameTransactionContext context =
        new FrameTransactionContext(
            frameInfos,
            signatureInfos,
            senderAddress,
            signatureHash,
            transaction.getNonce(),
            transaction.getMaxPriorityFeePerGas().orElse(Wei.ZERO),
            transaction.getMaxFeePerGas().orElse(Wei.ZERO),
            transaction.getMaxFeePerBlobGas().orElse(Wei.ZERO),
            maxCost);

    final WorldUpdater worldUpdater = worldState.updater();
    operationTracer.traceStartTransaction(worldUpdater, transaction);

    // Warm addresses at transaction start: the sender and the coinbase (EIP-3651); precompiles
    // are warm by construction. Frame targets are not pre-warmed and ENTRY_POINT is never warmed.
    final TreeSet<Address> initialWarmth = new TreeSet<>();
    initialWarmth.add(senderAddress);
    initialWarmth.add(miningBeneficiary);
    final TxValues txValues =
        TxValues.forTransaction(
            blockHashLookup,
            maxStackSize,
            UndoSet.of(initialWarmth),
            senderAddress,
            effectiveGasPrice,
            blobGasPrice == null ? Wei.ZERO : blobGasPrice,
            blockHeader,
            miningBeneficiary,
            transaction.getVersionedHashes().map(List::copyOf),
            0L);
    txValues.setFrameTransactionContext(context);
    final long txStartMark = txValues.transientStorage().mark();

    // Frame loop state.
    final FrameExecution[] executions = new FrameExecution[frames.size()];
    WorldUpdater batchUpdater = null;
    long batchMark = 0L;
    int skipUntil = -1; // inclusive index up to which frames are skipped by a failed batch

    for (int i = 0; i < frames.size(); i++) {
      final Frame frame = frames.get(i);
      final FrameExecution execution = new FrameExecution();
      executions[i] = execution;
      context.setCurrentFrameIndex(i);

      if (i <= skipUntil) {
        execution.skipped = true;
        execution.status = FrameReceipt.STATUS_SKIPPED;
        context.recordFrameSkipped(i);
        continue;
      }

      // Open an atomic batch at its first frame.
      final boolean batchOpen = batchUpdater != null;
      if (!batchOpen && frame.isAtomicBatch()) {
        batchUpdater = worldUpdater.updater();
        batchMark = txValues.transientStorage().mark();
      }
      final WorldUpdater activeUpdater = batchUpdater != null ? batchUpdater : worldUpdater;

      // SENDER frames require prior execution approval; otherwise the transaction is invalid.
      if (frame.mode() == Frame.MODE_SENDER && !context.senderApproved()) {
        return TransactionProcessingResult.invalid(
            ValidationResult.invalid(
                TransactionInvalidReason.FRAME_SENDER_NOT_APPROVED,
                "SENDER frame " + i + " executed before execution approval"));
      }

      // Discard transient storage between frames.
      txValues.transientStorage().undo(txStartMark);

      final Address caller =
          frame.mode() == Frame.MODE_SENDER ? senderAddress : FrameTransactionContext.ENTRY_POINT;
      txValues.setOriginator(caller);

      // Seed the frame's isolated state-gas pool before the frame's undo mark is taken, so a
      // frame revert restores the seeded value rather than unwinding it.
      txValues.stateGasReservoir().set(frame.stateGasLimit());

      final Address resolvedTarget = frame.resolvedTarget(senderAddress);
      final Account targetAccount = activeUpdater.get(resolvedTarget);
      final boolean isPrecompile = gasCalculator.isPrecompile(resolvedTarget);
      final boolean hasEmptyCode =
          targetAccount == null
              || targetAccount.getCodeHash() == null
              || targetAccount.getCodeHash().equals(Hash.EMPTY);

      final MessageFrame.Builder evmFrameBuilder =
          MessageFrame.builder()
              .txValues(txValues)
              .type(MessageFrame.Type.MESSAGE_CALL)
              .worldUpdater(activeUpdater.updater())
              .initialGas(frame.executionGasLimit())
              .address(resolvedTarget)
              .contract(resolvedTarget)
              .inputData(frame.data())
              .sender(caller)
              .value(frame.value())
              .apparentValue(frame.value())
              .code(
                  topLevelSupport.accountCode(
                      targetAccount,
                      delegated ->
                          CodeDelegationHelper.getTarget(
                                  activeUpdater,
                                  gasCalculator::isPrecompile,
                                  delegated,
                                  Optional.empty())
                              .code()))
              .isStatic(frame.mode() == Frame.MODE_VERIFY)
              .maxStackSize(maxStackSize)
              .originator(caller)
              .gasPrice(effectiveGasPrice)
              .blobGasPrice(blobGasPrice == null ? Wei.ZERO : blobGasPrice)
              .blockValues(blockHeader)
              .miningBeneficiary(miningBeneficiary)
              .blockHashLookup(blockHashLookup)
              .completer(__ -> {});
      // Shared across every frame: one tracker per transaction, as for a non-frame transaction.
      accessLocationTracker.ifPresent(evmFrameBuilder::eip7928AccessList);
      final MessageFrame evmFrame = evmFrameBuilder.build();

      boolean prepared = true;
      boolean exceptionalPreparation = false;

      // Charge the resolved target's warm/cold account access from the frame's execution pool.
      if (!topLevelSupport.chargeAccountAccess(evmFrame, resolvedTarget)) {
        prepared = false;
        exceptionalPreparation = true;
      } else {
        // EIP-7928: record the target only once its access has actually been paid for. A frame
        // that halts on the entry charge never accessed it, so it must not appear in the block
        // access list -- including when the halt unrolls an atomic batch.
        accessLocationTracker.ifPresent(t -> t.addTouchedAccount(resolvedTarget));
      }

      boolean nativeDefaultVerify = false;
      if (prepared && frame.mode() == Frame.MODE_VERIFY && !isPrecompile && hasEmptyCode) {
        // Protocol-defined default code: draws no execution gas of its own.
        nativeDefaultVerify = true;
      }

      boolean valueBalanceFailure = false;
      if (prepared && !nativeDefaultVerify) {
        if (!frame.value().isZero() && balanceOf(activeUpdater, caller).lessThan(frame.value())) {
          // Revert-style failure: keeps the unspent execution gas.
          prepared = false;
          valueBalanceFailure = true;
        } else if (!topLevelSupport.chargeTransactionEntry(
            evmFrame, activeUpdater, resolvedTarget)) {
          // EIP-2780 dispatch entry: NEW_ACCOUNT for value to an empty target, then the access to
          // a delegated target's delegation target, as for a top-level call.
          prepared = false;
          exceptionalPreparation = true;
        }
      }

      if (!prepared) {
        // The frame never executed: unwind its journal entries (target warmth, state charges).
        evmFrame.rollback();
        txValues.messageFrameStack().clear();
        execution.status = FrameReceipt.STATUS_FAILURE;
        execution.executionGasUsed =
            exceptionalPreparation
                ? frame.executionGasLimit()
                : frame.executionGasLimit() - evmFrame.getRemainingGas();
        context.recordFrameCompletion(i, execution.status, execution.executionGasUsed, 0L);
        if (valueBalanceFailure) {
          LOG.trace("Frame {} value transfer exceeds balance", i);
        }
      } else if (nativeDefaultVerify) {
        final boolean approved = executeDefaultVerifyCode(evmFrame, context, frame);
        txValues.messageFrameStack().clear();
        if (approved) {
          evmFrame.getWorldUpdater().commit();
          execution.status = FrameReceipt.STATUS_SUCCESS;
          execution.executionGasUsed = frame.executionGasLimit() - evmFrame.getRemainingGas();
          context.recordFrameCompletion(
              i,
              execution.status,
              execution.executionGasUsed,
              frame.stateGasLimit() - evmFrame.getStateGasReservoir());
        } else {
          evmFrame.rollback();
          execution.status = FrameReceipt.STATUS_FAILURE;
          execution.executionGasUsed = frame.executionGasLimit();
          context.recordFrameCompletion(i, execution.status, execution.executionGasUsed, 0L);
        }
      } else {
        // Run the frame in the EVM.
        while (!txValues.messageFrameStack().isEmpty()) {
          messageCallProcessor.process(txValues.messageFrameStack().peekFirst(), operationTracer);
        }
        final boolean success = evmFrame.getState() == MessageFrame.State.COMPLETED_SUCCESS;
        execution.status = success ? FrameReceipt.STATUS_SUCCESS : FrameReceipt.STATUS_FAILURE;
        execution.executionGasUsed = frame.executionGasLimit() - evmFrame.getRemainingGas();
        execution.logs = success ? List.copyOf(evmFrame.getLogs()) : List.of();
        context.recordFrameCompletion(
            i,
            execution.status,
            execution.executionGasUsed,
            success ? frame.stateGasLimit() - evmFrame.getStateGasReservoir() : 0L);
      }

      // A failed VERIFY frame invalidates the whole transaction.
      if (frame.mode() == Frame.MODE_VERIFY && execution.status != FrameReceipt.STATUS_SUCCESS) {
        return TransactionProcessingResult.invalid(
            ValidationResult.invalid(
                TransactionInvalidReason.FRAME_VERIFICATION_FAILED,
                "VERIFY frame " + i + " failed"));
      }

      // Atomic batch bookkeeping.
      if (batchUpdater != null) {
        if (execution.status == FrameReceipt.STATUS_FAILURE) {
          // Unroll: discard the batch's state, roll back the journal (which also zeroes the
          // executed batch frames' state-gas receipts and undoes refills to earlier receipts),
          // discard executed batch frames' logs, and skip the remaining batch frames.
          txValues.undoChanges(batchMark);
          for (int j = 0; j <= i; j++) {
            if (executions[j] != null && !executions[j].skipped) {
              // Frames at or after the batch start lose their logs.
              if (jInCurrentBatch(frames, j, i)) {
                executions[j].logs = List.of();
              }
            }
          }
          int j = i + 1;
          while (j < frames.size() && frames.get(j - 1).isAtomicBatch()) {
            skipUntil = j;
            j++;
          }
          batchUpdater = null;
        } else if (!frame.isAtomicBatch()) {
          // The batch terminator succeeded: commit the whole batch.
          batchUpdater.commit();
          batchUpdater = null;
        }
      }
    }

    context.setCurrentFrameIndex(frames.size());

    final Address payer = context.payer();
    if (payer == null) {
      return TransactionProcessingResult.invalid(
          ValidationResult.invalid(
              TransactionInvalidReason.FRAME_PAYMENT_NOT_APPROVED,
              "no frame approved payment for the transaction"));
    }

    // Settlement (EIP-8141 gas accounting).
    long txUnusedGas = 0L;
    long txStateGas = 0L;
    for (int i = 0; i < frames.size(); i++) {
      final long stateUsed = context.frameStateGasUsed(i);
      txUnusedGas += (frames.get(i).executionGasLimit() - executions[i].executionGasUsed);
      txUnusedGas += (frames.get(i).stateGasLimit() - stateUsed);
      txStateGas += stateUsed;
    }
    final long gasUsedBeforeRefund = standardGasLimit - txUnusedGas;
    final long refundCounter = txValues.gasRefunds().get();
    final long appliedRefund =
        Math.min(refundCounter, gasUsedBeforeRefund / gasCalculator.getMaxRefundQuotient());
    final long gasUsedAfterRefund = gasUsedBeforeRefund - appliedRefund;
    final long txExecutionGas = Math.max(gasUsedAfterRefund - txStateGas, calldataFloorGas);
    final long gasUsed = txExecutionGas + txStateGas;
    // EIP-7778: the block accounts execution gas before the refund.
    final long blockExecutionGas = Math.max(gasUsedBeforeRefund - txStateGas, calldataFloorGas);

    final Wei chargedFee = effectiveGasPrice.multiply(gasUsed).addExact(blobFee);
    final Wei payerRefund = maxCost.subtract(chargedFee);
    // EIP-7928: the payer settles the fee and so is touched, even when it is not the sender.
    accessLocationTracker.ifPresent(t -> t.addTouchedAccount(payer));
    final MutableAccount payerAccount = worldUpdater.getOrCreate(payer);
    payerAccount.incrementBalance(payerRefund);

    final Wei coinbaseWeiDelta =
        coinbaseCalculator.get().price(gasUsed, effectiveGasPrice, blockHeader.getBaseFee());
    operationTracer.traceBeforeRewardTransaction(worldUpdater, transaction, coinbaseWeiDelta);
    TopLevelTransactionSupport.creditCoinbase(
        worldUpdater, miningBeneficiary, coinbaseWeiDelta, accessLocationTracker);

    worldUpdater.commit();

    final Set<Address> selfDestructs = Set.copyOf(txValues.selfDestructs());
    topLevelSupport.settleSelfDestructs(worldState, selfDestructs);
    if (clearEmptyAccounts) {
      worldState.clearAccountsThatAreEmpty();
    }

    // Build the frame receipts and the concatenated transaction logs.
    final List<FrameReceipt> frameReceipts = new ArrayList<>(frames.size());
    final List<Log> allLogs = new ArrayList<>();
    for (int i = 0; i < frames.size(); i++) {
      final FrameExecution execution = executions[i];
      frameReceipts.add(
          new FrameReceipt(
              execution.status,
              execution.executionGasUsed,
              context.frameStateGasUsed(i),
              execution.logs));
      allLogs.addAll(execution.logs);
    }

    operationTracer.traceEndTransaction(
        worldState.updater(), transaction, true, Bytes.EMPTY, allLogs, gasUsed, selfDestructs, 0L);

    // Built after self-destruct settlement and empty-account clearing, so the diff against the
    // transaction updater sees the final state, matching MainnetTransactionProcessor.
    final Optional<PartialBlockAccessView> partialBlockAccessView =
        accessLocationTracker.map(tracker -> tracker.createPartialBlockAccessView(worldState));

    final TransactionProcessingResult result =
        TransactionProcessingResult.successful(
            allLogs,
            gasUsed,
            maxGas - gasUsed,
            gasUsed,
            txStateGas,
            Bytes.EMPTY,
            partialBlockAccessView,
            validationResult);
    result.setExecutionGasUsedForBlock(blockExecutionGas);
    result.setFrameTransactionOutcome(
        new TransactionProcessingResult.FrameTransactionOutcome(payer, frameReceipts));
    return result;
  }

  /** Whether frame {@code j} belongs to the batch that frame {@code failedIndex} failed in. */
  private static boolean jInCurrentBatch(
      final List<Frame> frames, final int j, final int failedIndex) {
    if (j > failedIndex) {
      return false;
    }
    // Walk back from the failed frame to the batch start: every predecessor with the atomic
    // batch flag chains the batch backwards.
    int start = failedIndex;
    while (start > 0 && frames.get(start - 1).isAtomicBatch()) {
      start--;
    }
    return j >= start;
  }

  private static Wei balanceOf(final WorldUpdater worldUpdater, final Address address) {
    final Account account = worldUpdater.get(address);
    return account == null ? Wei.ZERO : account.getBalance();
  }

  /**
   * The protocol-defined default code of a VERIFY frame targeting an account without code: requires
   * a matching SECP256K1 signature entry and applies {@code APPROVE(allowed_scope)}.
   *
   * @return true when the approval succeeded
   */
  private boolean executeDefaultVerifyCode(
      final MessageFrame evmFrame, final FrameTransactionContext context, final Frame frame) {
    final int allowedScope = frame.allowedApprovalScope();
    if (allowedScope == 0) {
      return false;
    }
    final int sigIndex = (allowedScope & Frame.FLAG_APPROVE_EXECUTION) != 0 ? 0 : 1;
    final List<FrameTransactionContext.SignatureInfo> signatures = context.signatures();
    if (sigIndex >= signatures.size()) {
      return false;
    }
    final FrameTransactionContext.SignatureInfo signature = signatures.get(sigIndex);
    if (signature.scheme() != FrameTransactionContext.SCHEME_SECP256K1
        || signature.msg() != null
        || !signature
            .resolvedSigner()
            .getBytes()
            .equals(context.currentFrame().resolvedTarget().getBytes())) {
      return false;
    }
    return ApproveOperation.attemptApproval(
            evmFrame,
            context,
            allowedScope,
            gasCalculator.stateGasCostCalculator()::newAccountStateGas)
        == ApproveOperation.ApprovalResult.SUCCESS;
  }
}
