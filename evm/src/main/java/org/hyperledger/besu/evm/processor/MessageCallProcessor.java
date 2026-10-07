/*
 * Copyright contributors to Besu.
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
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.ModificationNotAllowedException;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.log.TransferLogEmitter;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.precompile.PrecompiledContract;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Message call processor. */
public class MessageCallProcessor extends AbstractMessageProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(MessageCallProcessor.class);

  /** The precompiles. */
  protected final PrecompileContractRegistry precompiles;

  /** Strategy for emitting ETH transfer logs (no-op before Amsterdam, EIP-7708 after). */
  protected final TransferLogEmitter transferLogEmitter;

  /**
   * Instantiates a new Message call processor.
   *
   * @param evm the evm
   * @param precompiles the precompiles
   * @param forceCommitAddresses the force commit addresses
   */
  public MessageCallProcessor(
      final EVM evm,
      final PrecompileContractRegistry precompiles,
      final Set<Address> forceCommitAddresses) {
    this(evm, precompiles, forceCommitAddresses, TransferLogEmitter.NOOP);
  }

  /**
   * Instantiates a new Message call processor.
   *
   * @param evm the evm
   * @param precompiles the precompiles
   */
  public MessageCallProcessor(final EVM evm, final PrecompileContractRegistry precompiles) {
    this(evm, precompiles, Collections.emptySet(), TransferLogEmitter.NOOP);
  }

  /**
   * Instantiates a new Message call processor with transfer log emission support.
   *
   * @param evm the evm
   * @param precompiles the precompiles
   * @param forceCommitAddresses the force commit addresses
   * @param transferLogEmitter strategy for emitting transfer logs
   */
  public MessageCallProcessor(
      final EVM evm,
      final PrecompileContractRegistry precompiles,
      final Set<Address> forceCommitAddresses,
      final TransferLogEmitter transferLogEmitter) {
    super(evm, forceCommitAddresses);
    this.precompiles = precompiles;
    this.transferLogEmitter = transferLogEmitter;
  }

  @Override
  public void start(final MessageFrame frame, final OperationTracer operationTracer) {
    LOG.trace("Executing message-call");
    try {
      transferValue(frame);

      // Check first if the message call is to a pre-compile contract
      final PrecompiledContract precompile = precompiles.get(frame.getContractAddress());
      if (precompile != null) {
        executePrecompile(precompile, frame, operationTracer);
      } else {
        frame.setState(MessageFrame.State.CODE_EXECUTING);
      }
    } catch (final ModificationNotAllowedException ex) {
      LOG.trace("Message call error: attempt to mutate an immutable account");
      frame.setExceptionalHaltReason(Optional.of(ExceptionalHaltReason.ILLEGAL_STATE_CHANGE));
      frame.setState(MessageFrame.State.EXCEPTIONAL_HALT);
    }
  }

  @Override
  protected void codeSuccess(final MessageFrame frame, final OperationTracer operationTracer) {
    LOG.trace(
        "Successful message call of {} to {} (Gas remaining: {})",
        frame.getSenderAddress(),
        frame.getRecipientAddress(),
        frame.getRemainingGas());
    frame.setState(MessageFrame.State.COMPLETED_SUCCESS);
  }

  /**
   * Transfers the message call value from the sender to the recipient.
   *
   * <p>Assumes that the transaction has been validated so that the sender has the required fund as
   * of the world state of this executor.
   */
  private void transferValue(final MessageFrame frame) {
    final MutableAccount senderAccount = frame.getWorldUpdater().getSenderAccount(frame);

    // The yellow paper explicitly states that if the recipient account doesn't exist at this
    // point, it is created. Even if the value is zero we are still creating an account with 0x!
    final MutableAccount recipientAccount =
        frame.getWorldUpdater().getOrCreate(frame.getRecipientAddress());

    if (Objects.equals(frame.getValue(), Wei.ZERO)) {
      // This is only here for situations where you are calling a public address from a private
      // address. Without this guard clause we would attempt to get a mutable public address
      // which isn't possible from a private address and an error would be thrown.
      // If you are attempting to transfer value from a private address
      // to public address an error will be thrown.
      LOG.trace(
          "Message call from {} to {} has zero value: no fund transferred",
          frame.getSenderAddress(),
          frame.getRecipientAddress());
      return;
    }

    if (frame.getRecipientAddress().equals(frame.getSenderAddress())) {
      LOG.trace("Message call of {} to itself: no fund transferred", frame.getSenderAddress());
    } else {
      frame.getEip7928AccessList().ifPresent(t -> t.addTouchedAccount(senderAccount.getAddress()));
      frame
          .getEip7928AccessList()
          .ifPresent(t -> t.addTouchedAccount(recipientAccount.getAddress()));

      final Wei prevSenderBalance = senderAccount.decrementBalance(frame.getValue());
      final Wei prevRecipientBalance = recipientAccount.incrementBalance(frame.getValue());

      LOG.trace(
          "Transferred value {} for message call from {} ({} -> {}) to {} ({} -> {})",
          frame.getValue(),
          frame.getSenderAddress(),
          prevSenderBalance,
          senderAccount.getBalance(),
          frame.getRecipientAddress(),
          prevRecipientBalance,
          recipientAccount.getBalance());
    }

    // Emit transfer log for nonzero value transfers (no-op before Amsterdam, EIP-7708 after)
    transferLogEmitter.emitTransferLog(
        frame, frame.getSenderAddress(), frame.getRecipientAddress(), frame.getValue());
  }

  /**
   * Executes this message call knowing that it is a call to the provided pre-compiled contract.
   *
   * @param contract The contract this is a message call to.
   */
  private void executePrecompile(
      final PrecompiledContract contract,
      final MessageFrame frame,
      final OperationTracer operationTracer) {
    final long gasRequirement = contract.gasRequirement(frame.getInputData());
    final Bytes output;
    if (frame.getRemainingGas() < gasRequirement) {
      frame.setExceptionalHaltReason(Optional.of(ExceptionalHaltReason.INSUFFICIENT_GAS));
      frame.setState(MessageFrame.State.EXCEPTIONAL_HALT);
      output = null;
    } else {
      frame.decrementRemainingGas(gasRequirement);
      final PrecompiledContract.PrecompileContractResult result =
          contract.computePrecompile(frame.getInputData(), frame);
      output = result.output();
      if (result.isRefundGas()) {
        frame.incrementRemainingGas(gasRequirement);
      }
      if (frame.getState() == MessageFrame.State.REVERT) {
        frame.setRevertReason(output);
      } else {
        frame.setOutputData(output);
      }
      frame.setState(result.state());
      frame.setExceptionalHaltReason(result.haltReason());
    }
    operationTracer.tracePrecompileCall(frame, gasRequirement, output);
  }

  /**
   * Gets the precompile addresses.
   *
   * @return the precompile addresses
   */
  @VisibleForTesting
  public Set<Address> getPrecompileAddresses() {
    return precompiles.getPrecompileAddresses();
  }
}
