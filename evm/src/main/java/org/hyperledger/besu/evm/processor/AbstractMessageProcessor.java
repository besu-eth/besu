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

import static com.google.common.base.Preconditions.checkState;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(AbstractMessageProcessor.class);

  // List of addresses to force delete when they are touched but empty
  // when the state changes in the message are were not meant to be committed.
  final Set<Address> forceDeleteAccountsWhenEmpty;
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
   * Returns whether EVM v2 runs the frames of this processor. A frame built for it from the start
   * does not allocate the EVM v1 stack.
   *
   * @return true if EVM v2 is enabled
   */
  public boolean runsOnEvmV2() {
    return evm.getEvmConfiguration().enableEvmV2();
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
   * EIP-8037 state-gas accounting on frame failure. The spill goes back to gasRemaining rather than
   * the reservoir, because a revert propagates it to the parent and a halt burns it.
   */
  private void handleStateGasOnFrameFailure(final MessageFrame frame) {
    clearAccumulatedStateBesidesGasAndOutput(frame);
    final long spilled = frame.getStateGasSpilled();
    if (spilled > 0) {
      frame.incrementRemainingGas(spilled);
    }
    frame.resetStateGasSpilled();
  }

  private void exceptionalHalt(final MessageFrame frame) {
    handleStateGasOnFrameFailure(frame);

    frame.setState(MessageFrame.State.COMPLETED_FAILED);
    traceFrameExit(frame, "HALT");
    frame.clearGasRemaining();
    frame.clearOutputData();
  }

  /**
   * Gets called when the message frame reverts.
   *
   * @param frame The message frame
   */
  protected void revert(final MessageFrame frame) {
    handleStateGasOnFrameFailure(frame);

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
    frame.releaseStackV2();
  }

  private static void traceFrameExit(final MessageFrame frame, final String status) {
    if (LOG.isTraceEnabled()) {
      final var contractAddress = frame.getContractAddress();
      LOG.trace(
          "EIP-8037 FRAME_EXIT depth={} contractAddress={} status={} gasLeft={} reservoir={} stateGasUsed={}",
          frame.getDepth(),
          contractAddress == null ? "" : contractAddress.toHexString(),
          status,
          frame.getRemainingGas(),
          frame.getStateGasReservoir(),
          frame.getStateGasUsed());
    }
  }

  /**
   * Gets called when the message frame execution fails.
   *
   * @param frame The message frame
   */
  private void completedFailed(final MessageFrame frame) {
    frame.getMessageFrameStack().removeFirst();
    frame.notifyCompletion();
    frame.releaseStackV2();
  }

  private void exitFailed(final MessageFrame frame, final OperationTracer operationTracer) {
    if (operationTracer != null) {
      operationTracer.traceContextExit(frame);
    }
    completedFailed(frame);
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
    if (LOG.isTraceEnabled() && frame.getState() == MessageFrame.State.NOT_STARTED) {
      traceFrameEnter(frame);
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
      exitFailed(frame, operationTracer);
    }
  }

  /**
   * Fails a frame that ran out of gas before it started, without executing it. It goes through the
   * same halt path as a frame that runs out of gas while executing, and tracers still see it enter
   * and exit like any other failed call.
   *
   * @param frame the frame, not started and with its exceptional halt reason already set
   * @param operationTracer the operation tracer
   * @throws IllegalStateException if the frame has started or has no exceptional halt reason
   */
  public void haltBeforeStart(final MessageFrame frame, final OperationTracer operationTracer) {
    checkState(
        frame.getState() == MessageFrame.State.NOT_STARTED,
        "Frame already started, state %s",
        frame.getState());
    checkState(
        frame.getExceptionalHaltReason().isPresent(), "Frame has no exceptional halt reason");
    if (LOG.isTraceEnabled()) {
      traceFrameEnter(frame);
    }
    if (operationTracer != null) {
      operationTracer.traceContextEnter(frame);
    }
    exceptionalHalt(frame);
    exitFailed(frame, operationTracer);
  }

  private static void traceFrameEnter(final MessageFrame frame) {
    final var contractAddress = frame.getContractAddress();
    LOG.trace(
        "EIP-8037 FRAME_ENTER depth={} contractAddress={} gasLimit={} reservoir={} stateGasUsed={}",
        frame.getDepth(),
        contractAddress == null ? "" : contractAddress.toHexString(),
        frame.getRemainingGas(),
        frame.getStateGasReservoir(),
        frame.getStateGasUsed());
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
