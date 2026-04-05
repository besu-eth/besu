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
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.frame.SoftFailureReason;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
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
    if (!frame.stackHasItems(getStackItemsConsumed())) {
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

    final Wei value = Wei.wrap(Bytes.wrap(StackArithmetic.getAt(s, top, 0).toBytesBE()));

    final Address address = frame.getRecipientAddress();
    final MutableAccount account = getMutableAccount(address, frame);

    frame.clearReturnData();

    Code code = codeSupplier.get();

    if (code != null && code.getSize() > evm.getMaxInitcodeSize()) {
      final int newTop = top - getStackItemsConsumed();
      frame.setTopV2(newTop);
      return new OperationResult(cost, ExceptionalHaltReason.CODE_TOO_LARGE);
    }

    final boolean insufficientBalance = value.compareTo(account.getBalance()) > 0;
    final boolean maxDepthReached = frame.getDepth() >= 1024;
    final boolean invalidState = account.getNonce() == -1 || code == null;

    if (insufficientBalance || maxDepthReached || invalidState) {
      fail(frame, s, top);
      final SoftFailureReason softFailureReason =
          insufficientBalance
              ? LEGACY_INSUFFICIENT_BALANCE
              : (maxDepthReached ? LEGACY_MAX_CALL_DEPTH : INVALID_STATE);
      return new OperationResult(cost, getPcIncrement(), softFailureReason);
    }

    account.incrementNonce();
    frame.decrementRemainingGas(cost);

    // EIP-8037: Charge state gas for CREATE
    if (!gasCalculator().stateGasCostCalculator().chargeCreateStateGas(frame)) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    spawnChildMessage(frame, s, top, value, code);
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
      final MessageFrame parent, final long[] s, final int top, final Wei value, final Code code) {
    final Address contractAddress = generateTargetContractAddress(parent, s, top, code);
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
            .completer(child -> complete(parent, child));

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

  private void complete(final MessageFrame frame, final MessageFrame childFrame) {
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
      frame.setTopV2(StackArithmetic.pushAddress(s, newTop - 1, createdAddress));
      frame.setReturnData(Bytes.EMPTY);
      onSuccess(frame, createdAddress);
    } else {
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
