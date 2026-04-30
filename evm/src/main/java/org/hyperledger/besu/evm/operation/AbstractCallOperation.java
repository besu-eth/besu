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

import static org.hyperledger.besu.evm.frame.SoftFailureReason.LEGACY_INSUFFICIENT_BALANCE;
import static org.hyperledger.besu.evm.frame.SoftFailureReason.LEGACY_MAX_CALL_DEPTH;
import static org.hyperledger.besu.evm.internal.Words.clampedAdd;
import static org.hyperledger.besu.evm.internal.Words.clampedToLong;
import static org.hyperledger.besu.evm.worldstate.CodeDelegationHelper.getTarget;
import static org.hyperledger.besu.evm.worldstate.CodeDelegationHelper.hasCodeDelegation;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.frame.MessageFrame.State;
import org.hyperledger.besu.evm.frame.SoftFailureReason;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;

import org.apache.tuweni.bytes.Bytes;

/**
 * A skeleton class for implementing call operations.
 *
 * <p>A call operation creates a child message call from the current message context, allows it to
 * execute, and then updates the current message context based on its execution.
 */
public abstract class AbstractCallOperation extends AbstractOperation {

  /** The constant UNDERFLOW_RESPONSE. */
  protected static final OperationResult UNDERFLOW_RESPONSE =
      new OperationResult(0L, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);

  static final Bytes LEGACY_SUCCESS_STACK_ITEM = BYTES_ONE;
  static final Bytes LEGACY_FAILURE_STACK_ITEM = Bytes.EMPTY;

  /**
   * Instantiates a new Abstract call operation.
   *
   * @param opcode the opcode
   * @param name the name
   * @param stackItemsConsumed the stack items consumed
   * @param stackItemsProduced the stack items produced
   * @param gasCalculator the gas calculator
   */
  AbstractCallOperation(
      final int opcode,
      final String name,
      final int stackItemsConsumed,
      final int stackItemsProduced,
      final GasCalculator gasCalculator) {
    super(opcode, name, stackItemsConsumed, stackItemsProduced, gasCalculator);
  }

  /**
   * Returns the additional gas to provide the call operation.
   *
   * @param frame The current message frame
   * @return the additional gas to provide the call operation
   */
  protected long gas(final MessageFrame frame) {
    return clampedToLong(frame.getStackItem(0));
  }

  /**
   * Returns the account the call is being made to.
   *
   * @param frame The current message frame
   * @return the account the call is being made to
   */
  protected abstract Address to(MessageFrame frame);

  /**
   * Returns the value being transferred in the call
   *
   * @param frame The current message frame
   * @return the value being transferred in the call
   */
  protected abstract Wei value(MessageFrame frame);

  /**
   * Returns the apparent value being transferred in the call
   *
   * @param frame The current message frame
   * @return the apparent value being transferred in the call
   */
  protected abstract Wei apparentValue(MessageFrame frame);

  /**
   * Returns the memory offset the input data starts at.
   *
   * @param frame The current message frame
   * @return the memory offset the input data starts at
   */
  protected abstract long inputDataOffset(MessageFrame frame);

  /**
   * Returns the length of the input data to read from memory.
   *
   * @param frame The current message frame
   * @return the length of the input data to read from memory.
   */
  protected abstract long inputDataLength(MessageFrame frame);

  /**
   * Returns the memory offset the offset data starts at.
   *
   * @param frame The current message frame
   * @return the memory offset the offset data starts at
   */
  protected abstract long outputDataOffset(MessageFrame frame);

  /**
   * Returns the length of the output data to read from memory.
   *
   * @param frame The current message frame
   * @return the length of the output data to read from memory.
   */
  protected abstract long outputDataLength(MessageFrame frame);

  /**
   * Returns the account address the call operation is being performed on
   *
   * @param frame The current message frame
   * @return the account address the call operation is being performed on
   */
  protected abstract Address address(MessageFrame frame);

  /**
   * Returns the account address the call operation is being sent from
   *
   * @param frame The current message frame
   * @return the account address the call operation is being sent from
   */
  protected abstract Address sender(MessageFrame frame);

  /**
   * Returns the gas available to execute the child message call.
   *
   * @param frame The current message frame
   * @return the gas available to execute the child message call
   */
  public abstract long gasAvailableForChildCall(MessageFrame frame);

  /**
   * Returns whether the child message call should be static.
   *
   * @param frame The current message frame
   * @return {@code true} if the child message call should be static; otherwise {@code false}
   */
  protected boolean isStatic(final MessageFrame frame) {
    return frame.isStatic();
  }

  /**
   * Returns whether the child message call is a delegate call.
   *
   * @return {@code true} if the child message call is a delegate call; otherwise {@code false}
   */
  protected boolean isDelegate() {
    return false;
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    // manual check because some reads won't come until the "complete" step.
    if (frame.stackSize() < getStackItemsConsumed()) {
      return UNDERFLOW_RESPONSE;
    }

    final Address to = to(frame);
    final boolean accountIsWarm = frame.warmUpAddress(to) || gasCalculator().isPrecompile(to);
    final long stipend = gas(frame);
    final long inputDataOffset = inputDataOffset(frame);
    final long inputDataLength = inputDataLength(frame);
    final long outputDataOffset = outputDataOffset(frame);
    final long outputDataLength = outputDataLength(frame);
    final Wei transferValue = value(frame);
    final Address recipientAddress = address(frame);

    final long staticCost =
        gasCalculator()
            .callOperationStaticGasCost(
                frame,
                stipend,
                inputDataOffset,
                inputDataLength,
                outputDataOffset,
                outputDataLength,
                transferValue,
                recipientAddress,
                accountIsWarm);

    if (frame.getRemainingGas() < staticCost) {
      return new OperationResult(staticCost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    long cost =
        gasCalculator()
            .callOperationGasCost(
                frame,
                staticCost,
                stipend,
                inputDataOffset,
                inputDataLength,
                outputDataOffset,
                outputDataLength,
                transferValue,
                recipientAddress,
                accountIsWarm);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    final Account contract = getAccount(to, frame);
    cost = clampedAdd(cost, gasCalculator().calculateCodeDelegationResolutionGas(frame, contract));

    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }
    frame.decrementRemainingGas(cost);

    // EIP-8037: Charge state gas for new account creation in CALL
    if (!gasCalculator()
        .stateGasCostCalculator()
        .chargeCallNewAccountStateGas(frame, recipientAddress, transferValue)) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    frame.clearReturnData();

    final Account account = getAccount(frame.getRecipientAddress(), frame);

    final Wei balance = account == null ? Wei.ZERO : account.getBalance();

    // If the call is sending more value than the account has or the message frame is too deep
    // return a failed call
    final boolean insufficientBalance = value(frame).compareTo(balance) > 0;
    final boolean isFrameDepthTooDeep = frame.getDepth() >= 1024;
    if (insufficientBalance || isFrameDepthTooDeep) {
      frame.expandMemory(inputDataOffset(frame), inputDataLength(frame));
      frame.expandMemory(outputDataOffset(frame), outputDataLength(frame));
      // For the following, we either increment the gas or return zero, so we don't get double
      // charged. If we return zero then the traces don't have the right per-opcode cost.
      final long gasAvailableForChildCall = gasAvailableForChildCall(frame);
      frame.incrementRemainingGas(gasAvailableForChildCall + cost);
      frame.popStackItems(getStackItemsConsumed());
      frame.pushStackItem(LEGACY_FAILURE_STACK_ITEM);
      final SoftFailureReason softFailureReason =
          insufficientBalance ? LEGACY_INSUFFICIENT_BALANCE : LEGACY_MAX_CALL_DEPTH;
      return new OperationResult(cost, 1, softFailureReason, gasAvailableForChildCall);
    }

    final Bytes inputData = frame.readMutableMemory(inputDataOffset(frame), inputDataLength(frame));

    final Code code = getCode(evm, frame, contract);

    MessageFrame.Builder builder =
        MessageFrame.builder()
            .parentMessageFrame(frame)
            .type(MessageFrame.Type.MESSAGE_CALL)
            .initialGas(gasAvailableForChildCall(frame))
            .address(address(frame))
            .contract(to)
            .inputData(inputData)
            .sender(sender(frame))
            .value(value(frame))
            .apparentValue(apparentValue(frame))
            .code(code)
            .isStatic(isStatic(frame))
            .completer(child -> complete(frame, child));

    if (frame.getEip7928AccessList().isPresent()) {
      builder.eip7928AccessList(frame.getEip7928AccessList().get());
    }

    builder.build();
    // see note in stack depth check about incrementing cost
    frame.incrementRemainingGas(cost);

    frame.setState(MessageFrame.State.CODE_SUSPENDED);
    return new OperationResult(cost, null, 0);
  }

  /**
   * Complete.
   *
   * @param frame the frame
   * @param childFrame the child frame
   */
  public void complete(final MessageFrame frame, final MessageFrame childFrame) {
    frame.setState(MessageFrame.State.CODE_EXECUTING);

    final long outputOffset = outputDataOffset(frame);
    final long outputSize = outputDataLength(frame);
    final Bytes outputData = childFrame.getOutputData();

    if (outputSize > outputData.size()) {
      frame.expandMemory(outputOffset, outputSize);
      frame.writeMemory(outputOffset, outputData.size(), outputData, true);
    } else if (outputSize > 0) {
      frame.writeMemory(outputOffset, outputSize, outputData, true);
    }

    frame.setReturnData(outputData);
    if (!childFrame.getLogs().isEmpty()) {
      frame.addLogs(childFrame.getLogs());
    }
    if (!childFrame.getSelfDestructs().isEmpty()) {
      frame.addSelfDestructs(childFrame.getSelfDestructs());
    }
    if (!childFrame.getCreates().isEmpty()) {
      frame.addCreates(childFrame.getCreates());
    }

    final long gasRemaining = childFrame.getRemainingGas();
    frame.incrementRemainingGas(gasRemaining);

    frame.popStackItems(getStackItemsConsumed());
    Bytes resultItem;

    resultItem = getCallResultStackItem(childFrame);
    frame.pushStackItem(resultItem);

    final int currentPC = frame.getPC();
    frame.setPC(currentPC + 1);
  }

  Bytes getCallResultStackItem(final MessageFrame childFrame) {
    if (childFrame.getState() == State.COMPLETED_SUCCESS) {
      return LEGACY_SUCCESS_STACK_ITEM;
    } else {
      return LEGACY_FAILURE_STACK_ITEM;
    }
  }

  /**
   * Gets the code from the contract or EOA with delegated code.
   *
   * @param evm the evm
   * @param frame the message frame
   * @param account the account which codes needs to be retrieved
   * @return the code
   */
  protected Code getCode(final EVM evm, final MessageFrame frame, final Account account) {
    if (account == null) {
      return Code.EMPTY_CODE;
    }

    final Hash codeHash = account.getCodeHash();
    frame.getEip7928AccessList().ifPresent(t -> t.addTouchedAccount(account.getAddress()));
    if (codeHash == null || codeHash.equals(Hash.EMPTY)) {
      return Code.EMPTY_CODE;
    }

    final boolean accountHasCodeCache = account.getCodeCache() != null;

    final Code code;
    // Bonsai accounts may have a fully cached code, so we use that one
    if (accountHasCodeCache) {
      code = account.getOrCreateCachedCode();
    }
    // Any other account can only use the cached jump dest analysis if available
    else {
      code = evm.getOrCreateCachedJumpDest(codeHash, account.getCode());
    }

    if (!hasCodeDelegation(code.getBytes())) {
      return code;
    }

    final CodeDelegationHelper.Target target =
        getTarget(
            frame.getWorldUpdater(),
            evm.getGasCalculator()::isPrecompile,
            account,
            frame.getEip7928AccessList());

    if (accountHasCodeCache) {
      // If the account has a code cache, we can return the cached code of the target
      return target.code();
    }

    // otherwise we can only use the cached jump destination analysis
    final Code targetCode = target.code();
    return evm.getOrCreateCachedJumpDest(targetCode.getCodeHash(), targetCode.getBytes());
  }
}
