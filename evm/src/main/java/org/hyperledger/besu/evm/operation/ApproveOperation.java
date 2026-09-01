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
package org.hyperledger.besu.evm.operation;

import static org.hyperledger.besu.evm.internal.Words.clampedToLong;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.bytes.Bytes;

/**
 * The EIP-8141 APPROVE operation (0xaa).
 *
 * <p>Exits the current call frame successfully (like RETURN, with the designated memory region as
 * return data) and updates the transaction-scoped approval context. It is the only operation
 * allowed to modify state inside a VERIFY frame, so it deliberately performs no static check.
 */
public class ApproveOperation extends AbstractOperation {

  /** The APPROVE opcode. */
  public static final int OPCODE = 0xaa;

  /** Result of an approval attempt. */
  public enum ApprovalResult {
    /** The approval was applied. */
    SUCCESS,
    /** The approval was refused; the requesting call frame reverts. */
    REVERT,
    /** The sender-creation state charge was unaffordable; the call frame halts exceptionally. */
    OUT_OF_STATE_GAS
  }

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   */
  public ApproveOperation(final GasCalculator gasCalculator) {
    super(OPCODE, "APPROVE", 3, 0, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final var maybeContext = frame.getFrameTransactionContext();
    if (maybeContext.isEmpty()) {
      // Not a frame transaction.
      return new OperationResult(0, ExceptionalHaltReason.INVALID_OPERATION);
    }
    final FrameTransactionContext context = maybeContext.get();

    final long offset = clampedToLong(frame.popStackItem());
    final long length = clampedToLong(frame.popStackItem());
    final Bytes scopeBytes = frame.popStackItem().trimLeadingZeros();

    // Memory expansion for the return-data region, matching RETURN. No additional base cost.
    final long cost = gasCalculator().memoryExpansionGasCost(frame, offset, length);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    // Any scope bits beyond the approval mask revert the requesting call frame.
    if (scopeBytes.size() > 1
        || (scopeBytes.size() == 1
            && (scopeBytes.get(0) & ~FrameTransactionContext.APPROVE_SCOPE_MASK) != 0)) {
      return revert(frame, cost);
    }
    final int scope = scopeBytes.isEmpty() ? 0 : scopeBytes.get(0);

    // Only the frame's resolved target itself (including delegated code running under its
    // address) may approve.
    final FrameTransactionContext.FrameInfo currentFrame = context.currentFrame();
    if (!frame.getRecipientAddress().getBytes().equals(currentFrame.resolvedTarget().getBytes())) {
      return revert(frame, cost);
    }

    final ApprovalResult result =
        attemptApproval(
            frame, context, scope, gasCalculator().stateGasCostCalculator()::newAccountStateGas);
    return switch (result) {
      case SUCCESS -> {
        frame.setOutputData(frame.readMemory(offset, length));
        frame.setState(MessageFrame.State.CODE_SUCCESS);
        yield new OperationResult(cost, null);
      }
      case REVERT -> revert(frame, cost);
      case OUT_OF_STATE_GAS -> new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    };
  }

  /**
   * Attempts to apply an EIP-8141 approval with the given scope on behalf of the current frame's
   * resolved target. Shared by the APPROVE operation and the protocol-defined default code.
   *
   * @param frame the EVM call frame whose world updater receives the effects
   * @param context the frame transaction context
   * @param scope the requested approval scope
   * @param senderCreationStateGas supplier of the state gas charged when the sender account must be
   *     created by the nonce increment
   * @return the approval outcome
   */
  public static ApprovalResult attemptApproval(
      final MessageFrame frame,
      final FrameTransactionContext context,
      final int scope,
      final java.util.function.LongSupplier senderCreationStateGas) {
    final FrameTransactionContext.FrameInfo currentFrame = context.currentFrame();
    // The scope must be non-empty and within the frame's allowed approval scope.
    if (scope == 0
        || (scope & ~(currentFrame.flags() & FrameTransactionContext.APPROVE_SCOPE_MASK)) != 0) {
      return ApprovalResult.REVERT;
    }

    final boolean approvesExecution = (scope & FrameTransactionContext.APPROVE_EXECUTION) != 0;
    final boolean approvesPayment = (scope & FrameTransactionContext.APPROVE_PAYMENT) != 0;
    final Address resolvedTarget = currentFrame.resolvedTarget();
    final Address sender = context.sender();

    if (approvesExecution) {
      if (context.senderApproved()) {
        return ApprovalResult.REVERT;
      }
      if (!resolvedTarget.getBytes().equals(sender.getBytes())) {
        return ApprovalResult.REVERT;
      }
    }
    if (approvesPayment) {
      if (context.payer() != null) {
        return ApprovalResult.REVERT;
      }
      // Payment can only be approved once execution is approved (either earlier or by this very
      // approval when the scope covers both).
      if (!context.senderApproved() && !approvesExecution) {
        return ApprovalResult.REVERT;
      }
      final Account payerAccount = getTouchedAccount(frame, resolvedTarget);
      final Wei payerBalance = payerAccount == null ? Wei.ZERO : payerAccount.getBalance();
      if (payerBalance.lessThan(context.maxCost())) {
        return ApprovalResult.REVERT;
      }
      // The nonce increment may materialise the sender account: charge its creation from the
      // frame's state-gas pool before any approval effect is applied.
      final Account senderAccount = getTouchedAccount(frame, sender);
      if ((senderAccount == null || senderAccount.isEmpty())
          && !frame.consumeStateGas(senderCreationStateGas.getAsLong())) {
        return ApprovalResult.OUT_OF_STATE_GAS;
      }
      if (approvesExecution) {
        context.approveSender();
      }
      final MutableAccount mutableSender = frame.getWorldUpdater().getOrCreate(sender);
      mutableSender.incrementNonce();
      final MutableAccount mutablePayer = frame.getWorldUpdater().getOrCreate(resolvedTarget);
      mutablePayer.decrementBalance(context.maxCost());
      context.setPayer(resolvedTarget);
    } else {
      context.approveSender();
    }
    return ApprovalResult.SUCCESS;
  }

  private static Account getTouchedAccount(final MessageFrame frame, final Address address) {
    final Account account = frame.getWorldUpdater().get(address);
    frame.getEip7928AccessList().ifPresent(list -> list.addTouchedAccount(address));
    return account;
  }

  private OperationResult revert(final MessageFrame frame, final long cost) {
    frame.setState(MessageFrame.State.REVERT);
    frame.setOutputData(Bytes.EMPTY);
    return new OperationResult(cost, null);
  }
}
