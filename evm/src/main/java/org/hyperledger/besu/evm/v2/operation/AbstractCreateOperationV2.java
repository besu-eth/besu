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

import static org.hyperledger.besu.evm.frame.SoftFailureReason.INVALID_STATE;
import static org.hyperledger.besu.evm.frame.SoftFailureReason.LEGACY_INSUFFICIENT_BALANCE;
import static org.hyperledger.besu.evm.frame.SoftFailureReason.LEGACY_MAX_CALL_DEPTH;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.clampedToLong;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.pushAddress;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.pushZero;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readWeiAt;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.frame.SoftFailureReason;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.StateGasCostCalculator;

import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import org.apache.tuweni.bytes.Bytes;

/** A skeleton class for implementing create operations on the v2 long[] stack. */
public abstract class AbstractCreateOperationV2 extends AbstractOperationV2 {

  /**
   * Instantiates a new Abstract create operation.
   *
   * @param opcode the opcode
   * @param name the name
   * @param stackItemsConsumed the stack items consumed
   * @param stackItemsProduced the stack items produced
   * @param gasCalculator the gas calculator
   */
  protected AbstractCreateOperationV2(
      final int opcode,
      final String name,
      final int stackItemsConsumed,
      final int stackItemsProduced,
      final GasCalculator gasCalculator) {
    super(opcode, name, stackItemsConsumed, stackItemsProduced, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    // all items are checked up front, as some are only read in the complete step
    if (!frame.stackHasItemsV2(getStackItemsConsumed())) {
      return UNDERFLOW_RESPONSE;
    }

    Supplier<Code> codeSupplier = Suppliers.memoize(() -> getInitCode(frame));

    if (frame.isStatic()) {
      return new OperationResult(0, ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    }

    final long cost = cost(frame);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    // EIP-3860: the initcode-size limit is an early exceptional abort, so it must be
    // evaluated against the stack-declared size before initcode is resolved from
    // memory (which would expand memory based on an unvalidated length) and before
    // state gas is charged below.
    if (inputSize(frame) > evm.getMaxInitcodeSize()) {
      frame.setTopV2(frame.stackTopV2() - getStackItemsConsumed());
      return new OperationResult(cost, ExceptionalHaltReason.CODE_TOO_LARGE);
    }

    final Wei value = readWeiAt(frame.stackDataV2(), frame.stackTopV2(), 0);
    final Address address = frame.getRecipientAddress();
    final MutableAccount account = getMutableAccount(address, frame);

    frame.clearReturnData();
    final Code code = codeSupplier.get();

    final boolean insufficientBalance = value.compareTo(account.getBalance()) > 0;
    final boolean maxDepthReached = frame.getDepth() >= 1024;
    final boolean invalidState = account.getNonce() == -1 || code == null;

    if (insufficientBalance || maxDepthReached || invalidState) {
      // EIP-8037: nothing to refund — a silent failure lands before any state gas is charged.
      fail(frame);
      final SoftFailureReason softFailureReason =
          insufficientBalance
              ? LEGACY_INSUFFICIENT_BALANCE
              : (maxDepthReached ? LEGACY_MAX_CALL_DEPTH : INVALID_STATE);
      return new OperationResult(cost, 1, softFailureReason);
    }

    account.incrementNonce();

    // EIP-8037: an existent target adds no leaf, so it owes no NEW_ACCOUNT, and complete() needs
    // the same answer to know whether a failed create has anything to refill. Existent is the
    // EIP-161 sense — the address already has a state trie leaf, i.e. it is present and non-empty.
    // EIP-7928: the existence check is also what puts the target in the block access list, so it
    // stays listed even if the charge below runs out of gas.
    final Address contractAddress = generateTargetContractAddress(frame, code);
    // Pre-Amsterdam forks need neither the existence answer nor the access-list entry.
    final StateGasCostCalculator stateGasCalc = gasCalculator().stateGasCostCalculator();
    boolean targetExists = false;
    if (stateGasCalc.isActive()) {
      final Account existingTarget = getAccount(contractAddress, frame);
      targetExists = existingTarget != null && !existingTarget.isEmpty();
    }
    // EIP-8037: execution gas is deducted before state gas is charged (ordering requirement).
    frame.decrementRemainingGas(cost);
    if (!targetExists && !frame.consumeStateGas(stateGasCalc.newContractStateGas())) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }
    spawnChildMessage(frame, value, code, contractAddress, targetExists);
    frame.incrementRemainingGas(cost);

    return new OperationResult(cost, null, 1);
  }

  /**
   * Cost operation.
   *
   * @param frame the frame
   * @return the long
   */
  protected abstract long cost(final MessageFrame frame);

  /**
   * Generates the address of the contract to create.
   *
   * @param frame the frame
   * @param initcode the initcode generating the new contract
   * @return the address
   */
  protected abstract Address generateTargetContractAddress(MessageFrame frame, Code initcode);

  /**
   * Gets the initcode that will be run.
   *
   * @param frame the current frame
   * @return the initcode
   */
  protected Code getInitCode(final MessageFrame frame) {
    final Bytes inputData = frame.readMemory(inputOffset(frame), inputSize(frame));
    // Never cache CREATEx initcode. The amount of reuse is very low, and caching mostly
    // addresses disk loading delay, and we already have the code.
    return new Code(inputData);
  }

  /**
   * Returns the memory offset of the initcode.
   *
   * @param frame the current frame
   * @return the memory offset, clamped to Long.MAX_VALUE
   */
  protected long inputOffset(final MessageFrame frame) {
    return clampedToLong(frame.stackDataV2(), frame.stackTopV2(), 1);
  }

  /**
   * Returns the size of the initcode.
   *
   * @param frame the current frame
   * @return the size, clamped to Long.MAX_VALUE
   */
  protected long inputSize(final MessageFrame frame) {
    return clampedToLong(frame.stackDataV2(), frame.stackTopV2(), 2);
  }

  private void fail(final MessageFrame frame) {
    frame.readMutableMemory(inputOffset(frame), inputSize(frame));
    popArgumentsAndPush(frame, null);
  }

  private void spawnChildMessage(
      final MessageFrame parent,
      final Wei value,
      final Code code,
      final Address contractAddress,
      final boolean targetExists) {
    final long childGasStipend =
        gasCalculator().gasAvailableForChildCreate(parent.getRemainingGas());
    parent.decrementRemainingGas(childGasStipend);

    // frame addition is automatically handled by parent messageFrameStack
    MessageFrame.Builder builder =
        MessageFrame.builder()
            .parentMessageFrame(parent)
            .type(MessageFrame.Type.CONTRACT_CREATION)
            .initialGas(childGasStipend)
            .address(contractAddress)
            .contract(contractAddress)
            .inputData(Bytes.EMPTY)
            .sender(parent.getRecipientAddress())
            .value(value)
            .apparentValue(value)
            .code(code)
            .completer(child -> complete(parent, child, targetExists));

    if (parent.getEip7928AccessList().isPresent()) {
      builder.eip7928AccessList(parent.getEip7928AccessList().get());
    }

    builder.build();

    parent.setState(MessageFrame.State.CODE_SUSPENDED);
  }

  private void complete(
      final MessageFrame frame, final MessageFrame childFrame, final boolean targetExists) {
    frame.setState(MessageFrame.State.CODE_EXECUTING);

    frame.incrementRemainingGas(childFrame.getRemainingGas());
    frame.addLogs(childFrame.getLogs());
    frame.addSelfDestructs(childFrame.getSelfDestructs());
    frame.addCreates(childFrame.getCreates());

    if (childFrame.getState() == MessageFrame.State.COMPLETED_SUCCESS) {
      // The parent takes over the child's spill, so later refunds in this frame unwind the
      // combined spill rather than only this frame's share.
      frame.incrementStateGasSpilled(childFrame.getStateGasSpilled());
      // EIP-8037: a successful create adds the leaf it was charged for, so the charge stands.
      frame.settleStateGasOnChildSuccess();
      popArgumentsAndPush(frame, childFrame.getContractAddress());
      frame.setReturnData(Bytes.EMPTY);
    } else {
      // EIP-8037: no account was created, so refill whatever was charged for it. The child's own
      // state gas was already unwound by AbstractMessageProcessor.
      if (!targetExists) {
        frame.refillStateGasReservoir(
            gasCalculator().stateGasCostCalculator().newContractStateGas());
      }
      frame.setReturnData(childFrame.getOutputData());
      popArgumentsAndPush(frame, null);
    }

    final int currentPC = frame.getPC();
    frame.setPC(currentPC + 1);
  }

  /** Replaces the arguments by the created address, or by zero when there is none. */
  private void popArgumentsAndPush(final MessageFrame frame, final Address createdAddress) {
    final int resultSlot = frame.stackTopV2() - getStackItemsConsumed();
    if (createdAddress == null) {
      pushZero(frame.stackDataV2(), resultSlot);
    } else {
      pushAddress(createdAddress, frame.stackDataV2(), resultSlot);
    }
    frame.setTopV2(resultSlot + 1);
  }
}
