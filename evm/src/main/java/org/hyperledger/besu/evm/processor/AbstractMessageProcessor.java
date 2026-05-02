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
import org.hyperledger.besu.evm.frame.Eip8037Trace;
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
   * EIP-8037: Handles state-gas accounting on REVERT.
   *
   * <p>Snapshots the state-gas counters, runs the frame rollback (which automatically undoes all
   * UndoScalar-tracked state-gas mutations within the frame's scope — including no-growth refund
   * credits to the shared reservoir), then credits any gas-left spill back to the reservoir so the
   * parent (or sender, on top-level revert) can recover it via {@code refundedGas}.
   *
   * <p>Per EIP-8037, a reverted frame returns the full state-gas charge to the parent: {@code
   * parent.state_gas_left += child.state_gas_used + child.state_gas_left - child.state_gas_refund}.
   * The {@code state_gas_refund} subtraction prevents in-scope no-growth refunds (SSTORE 0→X→0
   * etc.) from inflating the parent's state gas. Besu mirrors this with {@code
   * noGrowthRefundsInScope}: even though rollback already undoes the reservoir credit, the in-scope
   * refund should also cancel out the matching gas-left spill that the refund would have "made up
   * for" — otherwise the parent would recover spill that the refund had effectively reclaimed.
   *
   * <p>Initial-frame revert is the exception: at top level the {@code state_gas_refund} stays
   * inside {@code state_gas_left} (no parent to absorb it), so the spill is fully restored without
   * subtraction — matching {@code state_gas_left += state_gas_used} in the tx-end error fixup.
   */
  private void handleStateGasRevertSpill(final MessageFrame frame) {
    final boolean isInitialFrame = frame.getMessageFrameStack().size() == 1;
    final long stateGasUsedBefore = frame.getStateGasUsed();
    final long reservoirBefore = frame.getStateGasReservoir();
    final long noGrowthRefundsBefore = frame.getNoGrowthStateGasRefunds();
    clearAccumulatedStateBesidesGasAndOutput(frame);
    final long stateGasRestored = stateGasUsedBefore - frame.getStateGasUsed();
    final long reservoirRestored = frame.getStateGasReservoir() - reservoirBefore;
    final long noGrowthRefundsInScope = noGrowthRefundsBefore - frame.getNoGrowthStateGasRefunds();
    final long grossSpill = stateGasRestored - reservoirRestored;
    final long restored;
    final long gasLeftBurn;
    final long reservoirBurn;
    if (isInitialFrame) {
      // Top-level revert: no parent to absorb refunds. Restore the full spill so the sender
      // recovers it via reservoir leftover (matches the tx-end fixup `state_gas_left +=
      // state_gas_used`).
      restored = Math.max(0L, grossSpill);
      gasLeftBurn = 0L;
      reservoirBurn = 0L;
    } else {
      // Non-initial revert: the matched portion of an in-scope charge ↔ in-scope refund must
      // stay paid by the user (parent's incorporate-on-error subtracts the refund). Split
      // the matched amount into:
      //   - gasLeftBurn: portion that was originally drained from gas_left (spill); gas_left is
      //     already short by this amount, so block accounting just needs to exclude it from
      //     block-regular (revert spill does not add to regular_gas_used).
      //   - reservoirBurn: portion drained from the reservoir; UndoScalar rollback restored
      //     the reservoir, so this loss has to be tracked separately and subtracted from the
      //     "effective" reservoir at tx end (raises gasUsedByTransaction; credits the loss to
      //     effective state gas so block-regular still excludes it).
      // Cap by chargesInScope to avoid burning when the refund came from a deeper successful
      // sub-frame (rollback already removed the inflation; no further action needed).
      final long chargesInScope = stateGasRestored + noGrowthRefundsInScope;
      final long matched = Math.min(noGrowthRefundsInScope, Math.max(0L, chargesInScope));
      gasLeftBurn = Math.min(matched, Math.max(0L, grossSpill));
      reservoirBurn = matched - gasLeftBurn;
      restored = Math.max(0L, grossSpill - gasLeftBurn);
    }
    if (restored > 0) {
      frame.incrementStateGasReservoir(restored);
    }
    if (gasLeftBurn > 0) {
      frame.accumulateStateGasSpillBurned(gasLeftBurn);
    }
    if (reservoirBurn > 0) {
      frame.accumulateStateGasReservoirBurn(reservoirBurn);
    }
    if (Eip8037Trace.ENABLED) {
      Eip8037Trace.spillRestore(
          frame.getDepth(),
          isInitialFrame,
          stateGasRestored,
          reservoirRestored,
          noGrowthRefundsInScope,
          grossSpill,
          gasLeftBurn,
          restored);
    }
  }

  /**
   * EIP-8037: Handles state-gas accounting on exceptional HALT.
   *
   * <p>Per EIP-8037, halt unconditionally resets the frame's {@code state_gas_left} to its entry
   * reservoir, zeros {@code state_gas_used} and {@code state_gas_refund}, and adds any spill
   * (state-gas charges drained from {@code gas_left}) to {@code regular_gas_used}. The spilled
   * portion stays burned alongside {@code gas_left}.
   *
   * <p>In Besu this collapses to "rollback only": {@code clearAccumulatedStateBesidesGasAndOutput}
   * → {@code rollback()} restores the reservoir, {@code stateGasUsed}, and the no-growth refund
   * counter to the frame's entry values. Subsequently {@code clearGasRemaining} (in the caller)
   * zeros {@code gasRemaining}, naturally re-classifying the spilled portion as regular-gas
   * consumption via {@code executionGas = txGas - gasRemaining - reservoir}. Unlike the revert
   * path, no spill is credited back to the reservoir — that's the whole point of the halt rule.
   */
  private void handleStateGasHalt(final MessageFrame frame) {
    clearAccumulatedStateBesidesGasAndOutput(frame);
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

    handleStateGasHalt(frame);

    if (isInitialFrame) {
      recordInitialFrameRegularHaltBurn(frame);
    }

    frame.setState(MessageFrame.State.COMPLETED_FAILED);
    traceFrameExit(frame, "HALT");
    frame.clearGasRemaining();
    frame.clearOutputData();
  }

  /**
   * Gets called when the message frame requests a revert.
   *
   * @param frame The message frame
   */
  protected void revert(final MessageFrame frame) {
    handleStateGasRevertSpill(frame);

    frame.setState(MessageFrame.State.COMPLETED_FAILED);
    traceFrameExit(frame, "REVERT");
  }

  /**
   * Gets called when the message frame completes successfully.
   *
   * @param frame The message frame
   */
  private void completedSuccess(final MessageFrame frame) {
    frame.getWorldUpdater().commit();
    traceFrameExit(frame, "SUCCESS");
    frame.getMessageFrameStack().removeFirst();
    frame.notifyCompletion();
  }

  private static void traceFrameExit(final MessageFrame frame, final String status) {
    if (!Eip8037Trace.ENABLED) {
      return;
    }
    final var addr = frame.getContractAddress();
    Eip8037Trace.frameExit(
        frame.getDepth(),
        addr == null ? "" : addr.toHexString(),
        status,
        frame.getRemainingGas(),
        frame.getStateGasReservoir(),
        frame.getStateGasUsed());
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
    if (Eip8037Trace.ENABLED && frame.getState() == MessageFrame.State.NOT_STARTED) {
      final var addr = frame.getContractAddress();
      Eip8037Trace.frameEnter(
          frame.getDepth(),
          addr == null ? "" : addr.toHexString(),
          frame.getRemainingGas(),
          frame.getStateGasReservoir(),
          frame.getStateGasUsed());
    }
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
