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
package org.hyperledger.besu.evm.v2.operation;

import static org.hyperledger.besu.evm.frame.SoftFailureReason.LEGACY_INSUFFICIENT_BALANCE;
import static org.hyperledger.besu.evm.frame.SoftFailureReason.LEGACY_MAX_CALL_DEPTH;
import static org.hyperledger.besu.evm.internal.Words.clampedAdd;
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
import org.hyperledger.besu.evm.v2.StackArithmetic;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;

import org.apache.tuweni.bytes.Bytes;

/**
 * Base class for EVM v2 CALL-family operations (CALL, CALLCODE, DELEGATECALL, STATICCALL).
 *
 * <p>Reads arguments from the v2 {@code long[]} stack, creates a child {@link MessageFrame} with
 * {@code enableEvmV2(true)}, suspends the parent, and writes the result back when the child
 * completes.
 */
public abstract class AbstractCallOperationV2 extends AbstractOperationV2 {

  /** Underflow response returned when the stack does not have enough items. */
  protected static final OperationResult UNDERFLOW_RESPONSE =
      new OperationResult(0L, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);

  /**
   * Instantiates a new abstract call operation for EVM v2.
   *
   * @param opcode the opcode
   * @param name the name
   * @param stackItemsConsumed the stack items consumed
   * @param stackItemsProduced the stack items produced
   * @param gasCalculator the gas calculator
   */
  AbstractCallOperationV2(
      final int opcode,
      final String name,
      final int stackItemsConsumed,
      final int stackItemsProduced,
      final GasCalculator gasCalculator) {
    super(opcode, name, stackItemsConsumed, stackItemsProduced, gasCalculator);
  }

  /**
   * Returns the gas stipend from the v2 stack (depth 0).
   *
   * @param s the v2 stack array
   * @param top the current stack top
   * @return the gas stipend, clamped to long
   */
  protected long gas(final long[] s, final int top) {
    return StackArithmetic.clampedToLong(s, top, 0);
  }

  /**
   * Returns the target address from the v2 stack.
   *
   * @param s the v2 stack array
   * @param top the current stack top
   * @return the target address
   */
  protected abstract Address to(long[] s, int top);

  /**
   * Returns the value (Wei) to transfer.
   *
   * @param s the v2 stack array
   * @param top the current stack top
   * @return the transfer value
   */
  protected abstract Wei value(long[] s, int top);

  /**
   * Returns the apparent value (Wei) for the child frame.
   *
   * @param frame the current message frame
   * @param s the v2 stack array
   * @param top the current stack top
   * @return the apparent value
   */
  protected abstract Wei apparentValue(MessageFrame frame, long[] s, int top);

  /**
   * Returns the input data memory offset.
   *
   * @param s the v2 stack array
   * @param top the current stack top
   * @return the input data offset
   */
  protected abstract long inputDataOffset(long[] s, int top);

  /**
   * Returns the input data length.
   *
   * @param s the v2 stack array
   * @param top the current stack top
   * @return the input data length
   */
  protected abstract long inputDataLength(long[] s, int top);

  /**
   * Returns the output data memory offset.
   *
   * @param s the v2 stack array
   * @param top the current stack top
   * @return the output data offset
   */
  protected abstract long outputDataOffset(long[] s, int top);

  /**
   * Returns the output data length.
   *
   * @param s the v2 stack array
   * @param top the current stack top
   * @return the output data length
   */
  protected abstract long outputDataLength(long[] s, int top);

  /**
   * Returns the address used as the recipient in the child frame.
   *
   * @param frame the current message frame
   * @param s the v2 stack array
   * @param top the current stack top
   * @return the recipient address
   */
  protected abstract Address address(MessageFrame frame, long[] s, int top);

  /**
   * Returns the sender address for the child frame.
   *
   * @param frame the current message frame
   * @return the sender address
   */
  protected abstract Address sender(MessageFrame frame);

  /**
   * Returns the gas available for the child call.
   *
   * @param frame the current message frame
   * @param s the v2 stack array
   * @param top the current stack top
   * @return the gas available for the child call
   */
  public abstract long gasAvailableForChildCall(MessageFrame frame, long[] s, int top);

  /**
   * Returns whether the child call should be static.
   *
   * @param frame the current message frame
   * @return {@code true} if the child call should be static
   */
  protected boolean isStatic(final MessageFrame frame) {
    return frame.isStatic();
  }

  /**
   * Returns whether this is a delegate call.
   *
   * @return {@code true} if this is a delegate call
   */
  protected boolean isDelegate() {
    return false;
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    if (!frame.stackHasItems(getStackItemsConsumed())) {
      return UNDERFLOW_RESPONSE;
    }

    final long[] s = frame.stackDataV2();
    final int top = frame.stackTopV2();

    final Address to = to(s, top);
    final boolean accountIsWarm = frame.warmUpAddress(to) || gasCalculator().isPrecompile(to);
    final long stipend = gas(s, top);
    final long inputOffset = inputDataOffset(s, top);
    final long inputLength = inputDataLength(s, top);
    final long outputOffset = outputDataOffset(s, top);
    final long outputLength = outputDataLength(s, top);
    final Wei transferValue = value(s, top);
    final Address recipientAddress = address(frame, s, top);

    final long staticCost =
        gasCalculator()
            .callOperationStaticGasCost(
                frame,
                stipend,
                inputOffset,
                inputLength,
                outputOffset,
                outputLength,
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
                inputOffset,
                inputLength,
                outputOffset,
                outputLength,
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

    final long gasForChild = gasAvailableForChildCall(frame, s, top);

    frame.clearReturnData();

    final Account account = getAccount(frame.getRecipientAddress(), frame);
    final Wei balance = account == null ? Wei.ZERO : account.getBalance();

    final boolean insufficientBalance = transferValue.compareTo(balance) > 0;
    final boolean isFrameDepthTooDeep = frame.getDepth() >= 1024;
    if (insufficientBalance || isFrameDepthTooDeep) {
      frame.expandMemory(inputOffset, inputLength);
      frame.expandMemory(outputOffset, outputLength);
      frame.incrementRemainingGas(gasForChild + cost);
      final int newTop = top - getStackItemsConsumed() + 1;
      StackArithmetic.putAt(s, newTop, 0, 0L, 0L, 0L, 0L);
      frame.setTopV2(newTop);
      final SoftFailureReason softFailureReason =
          insufficientBalance ? LEGACY_INSUFFICIENT_BALANCE : LEGACY_MAX_CALL_DEPTH;
      return new OperationResult(cost, 1, softFailureReason, gasForChild);
    }

    final Bytes inputData = frame.readMutableMemory(inputOffset, inputLength);
    final Code code = getCode(evm, frame, contract);

    MessageFrame.Builder builder =
        MessageFrame.builder()
            .parentMessageFrame(frame)
            .type(MessageFrame.Type.MESSAGE_CALL)
            .initialGas(gasForChild)
            .address(recipientAddress)
            .contract(to)
            .inputData(inputData)
            .sender(sender(frame))
            .value(transferValue)
            .apparentValue(apparentValue(frame, s, top))
            .code(code)
            .isStatic(isStatic(frame))
            .enableEvmV2(true)
            .completer(child -> complete(frame, child));

    if (frame.getEip7928AccessList().isPresent()) {
      builder.eip7928AccessList(frame.getEip7928AccessList().get());
    }

    builder.build();
    frame.incrementRemainingGas(cost);

    frame.setState(MessageFrame.State.CODE_SUSPENDED);
    return new OperationResult(cost, null, 0);
  }

  /**
   * Called when the child frame has finished executing. Restores the parent frame and pushes the
   * result onto the v2 stack.
   *
   * @param frame the parent message frame
   * @param childFrame the completed child message frame
   */
  public void complete(final MessageFrame frame, final MessageFrame childFrame) {
    frame.setState(MessageFrame.State.CODE_EXECUTING);

    final long[] s = frame.stackDataV2();
    final int top = frame.stackTopV2();

    final long outputOffset = outputDataOffset(s, top);
    final long outputSize = outputDataLength(s, top);
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

    frame.incrementRemainingGas(childFrame.getRemainingGas());

    final int newTop = top - getStackItemsConsumed() + 1;
    final long resultU0 = childFrame.getState() == State.COMPLETED_SUCCESS ? 1L : 0L;
    StackArithmetic.putAt(s, newTop, 0, 0L, 0L, 0L, resultU0);
    frame.setTopV2(newTop);

    frame.setPC(frame.getPC() + 1);
  }

  /**
   * Gets the executable code for the given account, resolving EIP-7702 code delegation if present.
   *
   * @param evm the EVM
   * @param frame the current message frame
   * @param account the account whose code is needed
   * @return the resolved code, or {@link Code#EMPTY_CODE} if none
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
    if (accountHasCodeCache) {
      code = account.getOrCreateCachedCode();
    } else {
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
      return target.code();
    }
    final Code targetCode = target.code();
    return evm.getOrCreateCachedJumpDest(targetCode.getCodeHash(), targetCode.getBytes());
  }
}
