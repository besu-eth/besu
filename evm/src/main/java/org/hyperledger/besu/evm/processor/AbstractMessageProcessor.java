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
package org.hyperledger.besu.evm.processor;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.ModificationNotAllowedException;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.ArrayList;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;

/**
 * A skeletal class for instantiating message processors.
 *
 * <p>The following methods have been created to be invoked when the message state changes via the
 * {@link MessageFrame.State}. Note that some of these methods are abstract while others have
 * default behaviors. There is currently no method for responding to a {@link
 * MessageFrame.State#CODE_SUSPENDED}*.
 *
 * <table>
 * <caption>Method Overview</caption>
 * <tr>
 * <td><b>{@code MessageFrame.State}</b></td>
 * <td><b>Method</b></td>
 * </tr>
 * <tr>
 * <td>{@link MessageFrame.State#NOT_STARTED}</td>
 * <td>{@link AbstractMessageProcessor#start(MessageFrame, OperationTracer)}</td>
 * </tr>
 * <tr>
 * <td>{@link MessageFrame.State#CODE_EXECUTING}</td>
 * <td>{@link AbstractMessageProcessor#codeExecute(MessageFrame, OperationTracer)}</td>
 * </tr>
 * <tr>
 * <td>{@link MessageFrame.State#CODE_SUCCESS}</td>
 * <td>{@link AbstractMessageProcessor#codeSuccess(MessageFrame, OperationTracer)}</td>
 * </tr>
 * <tr>
 * <td>{@link MessageFrame.State#COMPLETED_FAILED}</td>
 * <td>{@link AbstractMessageProcessor#completedFailed(MessageFrame)}</td>
 * <tr>
 * <td>{@link MessageFrame.State#COMPLETED_SUCCESS}</td>
 * <td>{@link AbstractMessageProcessor#completedSuccess(MessageFrame)}</td>
 * </tr>
 * </table>
 */
public abstract class AbstractMessageProcessor {

  // List of addresses to force delete when they are touched but empty
  // when the state changes in the message are were not meant to be committed.
  private final Set<? super Address> forceDeleteAccountsWhenEmpty;
  final EVM evm;

  /**
   * Instantiates a new Abstract message processor.
   *
   * @param evm the evm
   * @param forceDeleteAccountsWhenEmpty the force delete accounts when empty
   */
  AbstractMessageProcessor(final EVM evm, final Set<Address> forceDeleteAccountsWhenEmpty) {
    this.evm = evm;
    this.forceDeleteAccountsWhenEmpty = forceDeleteAccountsWhenEmpty;
  }

  /**
   * Start.
   *
   * @param frame the frame
   * @param operationTracer the operation tracer
   */
  protected abstract void start(MessageFrame frame, final OperationTracer operationTracer);

  /**
   * Gets called when the message frame code executes successfully.
   *
   * @param frame The message frame
   * @param operationTracer The tracer recording execution
   */
  protected abstract void codeSuccess(MessageFrame frame, final OperationTracer operationTracer);

  private void clearAccumulatedStateBesidesGasAndOutput(final MessageFrame frame) {
    final var worldUpdater = frame.getWorldUpdater();
    final var touchedAccounts = worldUpdater.getTouchedAccounts();

    if (touchedAccounts.isEmpty() || forceDeleteAccountsWhenEmpty.isEmpty()) {
      // Fast path: no touched accounts or no force-delete targets.
      // Just revert and commit without the stream pipeline overhead.
      worldUpdater.revert();
      worldUpdater.commit();
    } else {
      // Full path: find empty accounts that need force-deletion
      ArrayList<Address> addresses = new ArrayList<>();
      for (final Account account : touchedAccounts) {
        if (account.isEmpty()) {
          Address address = account.getAddress();
          if (forceDeleteAccountsWhenEmpty.contains(address)) {
            addresses.add(address);
          }
        }
      }

      // Clear any pending changes.
      worldUpdater.revert();

      // Force delete any requested accounts and commit the changes.
      for (final Address address : addresses) {
        worldUpdater.deleteAccount(address);
      }
      worldUpdater.commit();
    }

    frame.clearLogs();
    frame.clearGasRefund();

    frame.rollback();
  }

  /**
   * EIP-8037: Handles state gas spill on revert/halt. When state changes are rolled back, the state
   * gas that was consumed is restored, and any "spill" (state gas that had overflowed from the
   * reservoir into gasRemaining) is routed back to the reservoir.
   *
   * <p>For child frames the spill returns to the parent's reservoir for re-use. For the initial
   * (top-level) frame the same mechanism applies (per the EIP-8037 specification: "all consumed
   * execution state gas (from the reservoir and any spillover from gas_left) is moved back into the
   * state_gas_reservoir, and execution_state_gas_used is reset to zero"). The undo-mark was
   * advanced in the transaction processor to point past the intrinsic state gas charges, so
   * rollback restores stateGasUsed and stateGasReservoir to their post-intrinsic values; the
   * spilled portion is then returned to the reservoir to compensate for the gas_left that was
   * burned during execution.
   *
   * @param frame The message frame
   */
  private void handleStateGasSpill(final MessageFrame frame) {
    final boolean isInitialFrame = frame.getMessageFrameStack().size() == 1;
    final long stateGasUsedBefore = frame.getStateGasUsed();
    final long reservoirBefore = frame.getStateGasReservoir();
    final long noGrowthRefundsBefore = frame.getNoGrowthStateGasRefunds();

    clearAccumulatedStateBesidesGasAndOutput(frame);

    final long stateGasRestored = stateGasUsedBefore - frame.getStateGasUsed();
    final long reservoirRestored = frame.getStateGasReservoir() - reservoirBefore;
    final long noGrowthRefundsInScope = noGrowthRefundsBefore - frame.getNoGrowthStateGasRefunds();
    // EIP-8037 spill restoration on revert/halt:
    //   - Per the general spec, state gas consumed (drained from gas_left) is restored to the
    //     reservoir so the parent (or sender on top-level failure) recovers it.
    //   - For non-initial frames, EELS's incorporate_child_on_error subtracts the child's
    //     state_gas_refund from what reaches the parent — so no-growth refunds (SSTORE 0→X→0,
    //     CREATE silent/child failure) credited within the failed sub-tree do not inflate the
    //     parent's state_gas_left. Mirror this by burning the noGrowthRefundsInScope amount and
    //     deducting it from the (shared) reservoir, so the parent does not get the inflated
    //     credit back. The burned amount is tracked in stateGasSpillBurned so block-level
    //     regular gas excludes it (matches EELS, where state-gas spillover never contributes to
    //     regular_gas_used).
    //   - Besu uses a transaction-wide stateGasReservoir (TxValues), unlike EELS's per-frame
    //     state_gas_left. As a result, when CREATE charges state gas that drains from a parent-
    //     contributed reservoir and the inner frame later refunds, the rollback to frame entry
    //     does not visibly decrease the reservoir (the consume + refund net to zero across the
    //     UndoScalar log). We therefore base the burn on noGrowthRefundsInScope rather than the
    //     net reservoir movement, and explicitly deduct the burned amount from the reservoir to
    //     remove the inflation that the parent would otherwise recover via refundedGas.
    //   - For the initial (top-level) frame, EELS preserves all in-frame refund credits in
    //     state_gas_left at tx end (no parent to absorb them); so we don't subtract
    //     noGrowthRefundsInScope and the spill is fully restored.
    final long grossSpill = stateGasRestored - reservoirRestored;
    final long burned;
    final long restored;
    if (isInitialFrame) {
      burned = 0L;
      restored = Math.max(0L, grossSpill);
    } else {
      burned = Math.max(0L, noGrowthRefundsInScope);
      restored = Math.max(0L, grossSpill - burned);
    }
    if (restored > 0) {
      frame.incrementStateGasReservoir(restored);
    }
    if (burned > 0) {
      frame.accumulateStateGasSpillBurned(burned);
      // The shared reservoir was inflated by the no-growth refund; remove the burned portion
      // so the parent (or sender, via refundedGas) does not recover it.
      final long currentReservoir = frame.getStateGasReservoir();
      final long deduct = Math.min(currentReservoir, burned);
      if (deduct > 0) {
        frame.setStateGasReservoir(currentReservoir - deduct);
      }
    }
  }

  /**
   * Snapshots the initial frame's gasRemaining into {@code initialFrameRegularHaltBurn} when a
   * pre-execution halt fires on the initial frame (e.g. EIP-684 CREATE collision) so that gas paid
   * by the sender but never spent on regular or state work is excluded from block regular gas. When
   * opcode execution has already run on the frame, the halt-burn must remain in block regular gas
   * (no-op here).
   *
   * @param frame the initial (depth-0) message frame
   */
  private static void recordInitialFrameRegularHaltBurn(final MessageFrame frame) {
    if (frame.isCodeExecuted()) {
      return;
    }
    final long haltBurn = frame.getRemainingGas();
    if (haltBurn > 0) {
      frame.accumulateInitialFrameRegularHaltBurn(haltBurn);
    }
  }

  /**
   * Gets called when the message frame encounters an exceptional halt.
   *
   * @param frame The message frame
   */
  private void exceptionalHalt(final MessageFrame frame) {
    final boolean isInitialFrame = frame.getMessageFrameStack().size() == 1;

    handleStateGasSpill(frame);

    if (isInitialFrame) {
      recordInitialFrameRegularHaltBurn(frame);
    }

    frame.clearGasRemaining();
    frame.clearOutputData();
    frame.setState(MessageFrame.State.COMPLETED_FAILED);
  }

  /**
   * Gets called when the message frame requests a revert.
   *
   * @param frame The message frame
   */
  protected void revert(final MessageFrame frame) {
    handleStateGasSpill(frame);

    frame.setState(MessageFrame.State.COMPLETED_FAILED);
  }

  /**
   * Gets called when the message frame completes successfully.
   *
   * @param frame The message frame
   */
  private void completedSuccess(final MessageFrame frame) {
    frame.getWorldUpdater().commit();
    frame.getMessageFrameStack().removeFirst();
    frame.notifyCompletion();
  }

  /**
   * Gets called when the message frame execution fails.
   *
   * @param frame The message frame
   */
  private void completedFailed(final MessageFrame frame) {
    frame.getMessageFrameStack().removeFirst();
    frame.notifyCompletion();
  }

  /**
   * Executes the message frame code until it halts.
   *
   * @param frame The message frame
   * @param operationTracer The tracer recording execution
   */
  private void codeExecute(final MessageFrame frame, final OperationTracer operationTracer) {
    frame.markCodeExecuted();
    try {
      evm.runToHalt(frame, operationTracer);
    } catch (final ModificationNotAllowedException e) {
      frame.setState(MessageFrame.State.REVERT);
    }
  }

  /**
   * Process.
   *
   * @param frame the frame
   * @param operationTracer the operation tracer
   */
  public void process(final MessageFrame frame, final OperationTracer operationTracer) {
    if (operationTracer != null) {
      if (frame.getState() == MessageFrame.State.NOT_STARTED) {
        operationTracer.traceContextEnter(frame);
        start(frame, operationTracer);
      } else {
        operationTracer.traceContextReEnter(frame);
      }
    }

    final boolean wasCodeExecuting = (frame.getState() == MessageFrame.State.CODE_EXECUTING);
    if (wasCodeExecuting) {
      codeExecute(frame, operationTracer);

      if (frame.getState() == MessageFrame.State.CODE_SUSPENDED) {
        return;
      }

      if (frame.getState() == MessageFrame.State.CODE_SUCCESS) {
        codeSuccess(frame, operationTracer);
      }
    }

    if (frame.getState() == MessageFrame.State.EXCEPTIONAL_HALT) {
      exceptionalHalt(frame);
    }

    if (frame.getState() == MessageFrame.State.REVERT) {
      revert(frame);
    }

    if (frame.getState() == MessageFrame.State.COMPLETED_SUCCESS) {
      if (operationTracer != null) {
        operationTracer.traceContextExit(frame);
      }
      completedSuccess(frame);
    }
    if (frame.getState() == MessageFrame.State.COMPLETED_FAILED) {
      if (operationTracer != null) {
        operationTracer.traceContextExit(frame);
      }
      completedFailed(frame);
    }
  }

  /**
   * Gets or creates code instance with a cached jump destination.
   *
   * @param codeHash the code hash
   * @param codeBytes the code bytes
   * @return the code instance with the cached jump destination
   */
  public Code getOrCreateCachedJumpDest(final Hash codeHash, final Bytes codeBytes) {
    return evm.getOrCreateCachedJumpDest(codeHash, codeBytes);
  }
}
