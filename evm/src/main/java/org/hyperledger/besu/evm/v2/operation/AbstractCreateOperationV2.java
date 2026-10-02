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
import org.hyperledger.besu.evm.v2.StackArithmetic;

import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import org.apache.tuweni.bytes.Bytes;

/**
 * Base class for EVM v2 CREATE-family operations (CREATE, CREATE2).
 *
 * <p>Reads arguments from the v2 {@code long[]} stack, creates a child {@link MessageFrame} with
 * {@code enableEvmV2(true)}, suspends the parent, and writes the result back when the child
 * completes.
 */
public abstract class AbstractCreateOperationV2 extends AbstractOperationV2 {

  /** Underflow response returned when the stack does not have enough items. */
  protected static final OperationResult UNDERFLOW_RESPONSE =
      new OperationResult(0L, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);

  /**
   * Instantiates a new abstract create operation for EVM v2.
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
    if (!frame.stackHasItemsV2(getStackItemsConsumed())) {
      return UNDERFLOW_RESPONSE;
    }

    Supplier<Code> codeSupplier = Suppliers.memoize(() -> getInitCode(frame, evm));

    if (frame.isStatic()) {
      return new OperationResult(0, ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    }

    final long[] s = frame.stackDataV2();
    final int top = frame.stackTopV2();

    final long cost = cost(frame, s, top, codeSupplier);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    // EIP-3860: the initcode-size limit is an early exceptional abort, so it must be
    // evaluated against the stack-declared size before initcode is resolved from
    // memory (which would expand memory based on an unvalidated length) and before
    // state gas is charged below.
    if (getInputSize(s, top) > evm.getMaxInitcodeSize()) {
      frame.setTopV2(top - getStackItemsConsumed());
      return new OperationResult(cost, ExceptionalHaltReason.CODE_TOO_LARGE);
    }

    final Wei value = Wei.wrap(Bytes.wrap(StackArithmetic.getAt(s, top, 0).toBytesBE()));

    final Address address = frame.getRecipientAddress();
    final MutableAccount account = getMutableAccount(address, frame);

    frame.clearReturnData();

    final Code code = codeSupplier.get();

    final boolean insufficientBalance = value.compareTo(account.getBalance()) > 0;
    final boolean maxDepthReached = frame.getDepth() >= 1024;
    final boolean invalidState = account.getNonce() == -1 || code == null;

    if (insufficientBalance || maxDepthReached || invalidState) {
      // EIP-8037: nothing to refund — a silent failure lands before any state gas is charged.
      fail(frame, s, top);
      final SoftFailureReason softFailureReason =
          insufficientBalance
              ? LEGACY_INSUFFICIENT_BALANCE
              : (maxDepthReached ? LEGACY_MAX_CALL_DEPTH : INVALID_STATE);
      return new OperationResult(cost, getPcIncrement(), softFailureReason);
    }

    account.incrementNonce();

    // EIP-8037: an existent target adds no leaf, so it owes no NEW_ACCOUNT, and complete() needs
    // the same answer to know whether a failed create has anything to refill. Existent is the
    // EIP-161 sense — the address already has a state trie leaf, i.e. it is present and non-empty.
    // EIP-7928: the existence check is also what puts the target in the block access list, so it
    // stays listed even if the charge below runs out of gas.
    final Address contractAddress = generateTargetContractAddress(frame, s, top, code);
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

    return new OperationResult(cost, null, getPcIncrement());
  }

  /**
   * How many bytes does this operation occupy?
   *
   * @return the number of bytes the operation and immediate arguments occupy
   */
  protected int getPcIncrement() {
    return 1;
  }

  /**
   * Computes the gas cost for this create operation.
   *
   * @param frame the current message frame
   * @param s the v2 stack array
   * @param top the current stack top
   * @param codeSupplier a supplier for the initcode, if needed for costing
   * @return the gas cost
   */
  protected abstract long cost(MessageFrame frame, long[] s, int top, Supplier<Code> codeSupplier);

  /**
   * Generates the target contract address.
   *
   * @param frame the current message frame
   * @param s the v2 stack array
   * @param top the current stack top
   * @param initcode the initcode
   * @return the target address
   */
  protected abstract Address generateTargetContractAddress(
      MessageFrame frame, long[] s, int top, Code initcode);

  /**
   * Gets the initcode to run.
   *
   * @param frame the current message frame
   * @param evm the EVM executing the message frame
   * @return the initcode
   */
  protected abstract Code getInitCode(MessageFrame frame, EVM evm);

  /**
   * Returns the declared initcode size from the stack, clamped to a long. Used for the EIP-3860
   * size check before initcode is resolved from memory.
   *
   * @param s the v2 stack array
   * @param top the current stack top
   * @return the requested initcode size
   */
  protected long getInputSize(final long[] s, final int top) {
    return StackArithmetic.clampedToLong(s, top, 2);
  }

  /**
   * Handles stack when the operation fails (insufficient balance, depth limit, invalid state).
   *
   * @param frame the current execution frame
   * @param s the v2 stack array
   * @param top the current stack top
   */
  protected void fail(final MessageFrame frame, final long[] s, final int top) {
    final long inputOffset = StackArithmetic.clampedToLong(s, top, 1);
    final long inputSize = StackArithmetic.clampedToLong(s, top, 2);
    frame.readMutableMemory(inputOffset, inputSize);
    final int newTop = top - getStackItemsConsumed() + 1;
    StackArithmetic.putAt(s, newTop, 0, 0L, 0L, 0L, 0L);
    frame.setTopV2(newTop);
  }

  private void spawnChildMessage(
      final MessageFrame parent,
      final Wei value,
      final Code code,
      final Address contractAddress,
      final boolean targetExists) {
    final Bytes inputData = getInputData(parent);

    final long childGasStipend =
        gasCalculator().gasAvailableForChildCreate(parent.getRemainingGas());
    parent.decrementRemainingGas(childGasStipend);

    MessageFrame.Builder builder =
        MessageFrame.builder()
            .parentMessageFrame(parent)
            .type(MessageFrame.Type.CONTRACT_CREATION)
            .initialGas(childGasStipend)
            .address(contractAddress)
            .contract(contractAddress)
            .inputData(inputData)
            .sender(parent.getRecipientAddress())
            .value(value)
            .apparentValue(value)
            .code(code)
            .enableEvmV2(true)
            .completer(child -> complete(parent, child, targetExists));

    if (parent.getEip7928AccessList().isPresent()) {
      builder.eip7928AccessList(parent.getEip7928AccessList().get());
    }

    builder.build();

    parent.setState(MessageFrame.State.CODE_SUSPENDED);
  }

  /**
   * Returns the input data to append for this create operation. Default is empty (for CREATE and
   * CREATE2).
   *
   * @param frame the current message frame
   * @return the input data bytes
   */
  protected Bytes getInputData(final MessageFrame frame) {
    return Bytes.EMPTY;
  }

  private void complete(
      final MessageFrame frame, final MessageFrame childFrame, final boolean targetExists) {
    frame.setState(MessageFrame.State.CODE_EXECUTING);

    frame.incrementRemainingGas(childFrame.getRemainingGas());
    frame.addLogs(childFrame.getLogs());
    frame.addSelfDestructs(childFrame.getSelfDestructs());
    frame.addCreates(childFrame.getCreates());

    final long[] s = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int newTop = top - getStackItemsConsumed() + 1;

    if (childFrame.getState() == MessageFrame.State.COMPLETED_SUCCESS) {
      final Address createdAddress = childFrame.getContractAddress();
      // The parent takes over the child's spill, so later refunds in this frame unwind the
      // combined spill rather than only this frame's share.
      frame.incrementStateGasSpilled(childFrame.getStateGasSpilled());
      // EIP-8037: a successful create adds the leaf it was charged for, so the charge stands.
      frame.settleStateGasOnChildSuccess();
      frame.setTopV2(StackArithmetic.pushAddress(s, newTop - 1, createdAddress));
      frame.setReturnData(Bytes.EMPTY);
      onSuccess(frame, createdAddress);
    } else {
      // EIP-8037: no account was created, so refill whatever was charged for it. The child's own
      // state gas was already unwound by AbstractMessageProcessor.
      if (!targetExists) {
        frame.refillStateGasReservoir(
            gasCalculator().stateGasCostCalculator().newContractStateGas());
      }
      frame.setReturnData(childFrame.getOutputData());
      StackArithmetic.putAt(s, newTop, 0, 0L, 0L, 0L, 0L);
      frame.setTopV2(newTop);
      onFailure(frame, childFrame.getExceptionalHaltReason());
    }

    frame.setPC(frame.getPC() + getPcIncrement());
  }

  /**
   * Called on successful child contract creation.
   *
   * @param frame the parent frame
   * @param createdAddress the address of the newly created contract
   */
  protected void onSuccess(final MessageFrame frame, final Address createdAddress) {}

  /**
   * Called on failed child contract creation.
   *
   * @param frame the parent frame
   * @param haltReason the exceptional halt reason
   */
  protected void onFailure(
      final MessageFrame frame, final Optional<ExceptionalHaltReason> haltReason) {}
}
