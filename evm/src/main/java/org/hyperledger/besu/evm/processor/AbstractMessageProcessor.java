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
   * gas that was consumed is restored. Any "spill" (state gas that had overflowed from the
   * reservoir into gasRemaining) is routed back: for child frames it returns to the reservoir for
   * parent re-use; for the initial frame it is tracked in stateGasSpillBurned for transaction-level
   * gas accounting.
   *
   * <p>For the initial (top-level) frame, the reservoir must be preserved for transaction-level
   * refund. If child frames had restored state gas to the reservoir during the initial frame's
   * execution (via their own revert/halt), that refund must not be lost when the initial frame
   * subsequently reverts or halts. We therefore restore the reservoir to the higher of the
   * pre-rollback value (which may include child refunds) and the post-rollback value (which
   * reflects any reservoir drain that rollback undid). We also compute the spill contribution only
   * from the positive part of reservoirRestored so that child-refunded gas is never counted as
   * burned spill.
   *
   * @param frame The message frame
   */
  private void handleStateGasSpill(final MessageFrame frame, final boolean isInitialFrame) {
    final long stateGasUsedBefore = frame.getStateGasUsed();
    final long reservoirBefore = frame.getStateGasReservoir();

    clearAccumulatedStateBesidesGasAndOutput(frame);

    final long stateGasRestored = stateGasUsedBefore - frame.getStateGasUsed();
    final long reservoirRestored = frame.getStateGasReservoir() - reservoirBefore;

    if (isInitialFrame) {
      // EIP-8037: For initial-frame halt/revert, state gas consumed by ops is final for block
      // accounting (spec: `tx_state_gas = intrinsic_state_gas + state_gas_used`). The portion
      // that spilled from gasRemaining is already accounted via stateGasSpillBurned below; the
      // portion drained from the reservoir (reservoirRestored) was rolled back by the undo but
      // must still count as consumed state gas, so add it back to stateGasUsed. Then preserve
      // the actual pre-rollback reservoir value so drain is reflected in total gas returned to
      // the sender.
      if (reservoirRestored > 0) {
        frame.incrementStateGasUsed(reservoirRestored);
      }
      frame.setStateGasReservoir(reservoirBefore);
      // Only burn the portion of state gas that actually spilled into gasRemaining (not the
      // portion that was drawn from the reservoir and has already been restored, and not the
      // portion that child frames had refunded to the reservoir).
      final long spill = Math.max(0L, stateGasRestored - Math.max(0L, reservoirRestored));
      if (spill > 0) {
        frame.accumulateStateGasSpillBurned(spill);
      }
    } else {
      final long spill = Math.max(0L, stateGasRestored - reservoirRestored);
      if (spill > 0) {
        // Child frame: return spill to reservoir for parent to re-use
        frame.incrementStateGasReservoir(spill);
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

    handleStateGasSpill(frame, isInitialFrame);

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
    final boolean isInitialFrame = frame.getMessageFrameStack().size() == 1;
    handleStateGasSpill(frame, isInitialFrame);

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
