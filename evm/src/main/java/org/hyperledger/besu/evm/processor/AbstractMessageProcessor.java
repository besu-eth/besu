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
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.ArrayList;
import java.util.Optional;
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
   * Gets called when the message frame encounters an exceptional halt. Under EIP-8037 (PR 11573)
   * frame-end accounting, a halted frame produces no state-gas debits or credits — its recorded
   * events are dropped via {@link MessageFrame#rollback()} and no charges or refunds are applied.
   *
   * @param frame The message frame
   */
  private void exceptionalHalt(final MessageFrame frame) {
    rollbackPreservingDescendantStateGas(frame);
    frame.clearGasRemaining();
    frame.clearOutputData();
    frame.setState(MessageFrame.State.COMPLETED_FAILED);
    traceFrameExit(frame, "HALT");
  }

  /**
   * Roll the frame back via {@link MessageFrame#rollback()}, any state-gas charged by successful
   * descendants (which {@code UndoScalar} would otherwise discard) is preserved as a reservoir
   * credit on the parent. The post-rollback reservoir target is {@code descendantStateGas + Re},
   * where {@code Re} is {@code state_gas_reservoir} at the failing frame's exit and {@code
   * descendantStateGas = state_gas_used - frame_entry_state_gas_used}. No-op for the pre-Amsterdam
   * path where {@code stateGasCostCalculator().isActive()} returns false.
   */
  private void rollbackPreservingDescendantStateGas(final MessageFrame frame) {
    final boolean active =
        evm != null && evm.getGasCalculator().stateGasCostCalculator().isActive();
    final long preserved =
        active
            ? frame.getStateGasReservoir()
                + Math.max(0L, frame.getStateGasUsed() - frame.getFrameEntryStateGasUsed())
            : 0L;
    clearAccumulatedStateBesidesGasAndOutput(frame);
    if (active) {
      frame.setStateGasReservoir(preserved);
    }
  }

  private void traceFrameExit(final MessageFrame frame, final String status) {
    if (!org.hyperledger.besu.evm.frame.Eip8037Trace.ENABLED) {
      return;
    }
    final org.hyperledger.besu.datatypes.Address addr = frame.getContractAddress();
    org.hyperledger.besu.evm.frame.Eip8037Trace.frameExit(
        frame.getMessageStackSize(),
        addr == null ? "" : addr.toHexString(),
        status,
        frame.getRemainingGas(),
        frame.getStateGasReservoir(),
        frame.getStateGasUsed());
  }

  /**
   * Gets called when the message frame requests a revert. Under EIP-8037 (PR 11573) frame-end
   * accounting, a reverted frame produces no state-gas debits or credits — its recorded events are
   * dropped via {@link MessageFrame#rollback()}.
   *
   * @param frame The message frame
   */
  protected void revert(final MessageFrame frame) {
    rollbackPreservingDescendantStateGas(frame);
    frame.setState(MessageFrame.State.COMPLETED_FAILED);
    traceFrameExit(frame, "REVERT");
  }

  /**
   * Gets called when the message frame completes successfully. EIP-8037 frame-end aggregation runs
   * here against the frame's recorded state-change events; an OOG outcome transitions the frame to
   * {@link MessageFrame.State#EXCEPTIONAL_HALT}.
   *
   * @param frame The message frame
   */
  private void completedSuccess(final MessageFrame frame) {
    if (!evm.getGasCalculator().stateGasCostCalculator().applyFrameEndStateGasAccounting(frame)) {
      // EIP-8037: state-gas OOG at frame-end is a "soft" failure. We preserve the unspent
      // gas_left (only state changes are rolled back via restore_tx_state) so the parent can
      // recover it via the call/create completion flow. Skip clearGasRemaining here.
      frame.setExceptionalHaltReason(Optional.of(ExceptionalHaltReason.INSUFFICIENT_GAS));
      rollbackPreservingDescendantStateGas(frame);
      frame.setState(MessageFrame.State.COMPLETED_FAILED);
      traceFrameExit(frame, "STATE_OOG");
      return;
    }
    frame.getWorldUpdater().commit();
    traceFrameExit(frame, "SUCCESS");
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
