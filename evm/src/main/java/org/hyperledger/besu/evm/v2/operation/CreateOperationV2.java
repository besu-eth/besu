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

import static org.hyperledger.besu.evm.internal.Words.clampedAdd;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.v2.StackArithmetic;

import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;

/** EVM v2 CREATE operation (opcode 0xF0). Stack layout: value, offset, size → contractAddress. */
public class CreateOperationV2 extends AbstractCreateOperationV2 {

  /**
   * Instantiates a new CREATE operation for EVM v2.
   *
   * @param gasCalculator the gas calculator
   */
  public CreateOperationV2(final GasCalculator gasCalculator) {
    super(0xF0, "CREATE", 3, 1, gasCalculator);
  }

  @Override
  protected long cost(
      final MessageFrame frame, final long[] s, final int top, final Supplier<Code> unused) {
    final int inputOffset = StackArithmetic.clampedToInt(s, top, 1);
    final int inputSize = StackArithmetic.clampedToInt(s, top, 2);
    return clampedAdd(
        clampedAdd(
            gasCalculator().txCreateCost(),
            gasCalculator().memoryExpansionGasCost(frame, inputOffset, inputSize)),
        gasCalculator().initcodeCost(inputSize));
  }

  @Override
  protected Address generateTargetContractAddress(
      final MessageFrame frame, final long[] s, final int top, final Code initcode) {
    final Account sender = getAccount(frame.getRecipientAddress(), frame);
    return Address.contractAddress(frame.getRecipientAddress(), sender.getNonce() - 1L);
  }

  @Override
  protected Code getInitCode(final MessageFrame frame, final EVM evm) {
    final long[] s = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final long inputOffset = StackArithmetic.clampedToLong(s, top, 1);
    final long inputSize = StackArithmetic.clampedToLong(s, top, 2);
    final Bytes inputData = frame.readMemory(inputOffset, inputSize);
    return new Code(inputData);
  }

  /**
   * Performs the CREATE operation on the v2 stack.
   *
   * @param frame the current message frame
   * @param s the v2 stack array
   * @param gasCalculator the gas calculator
   * @param evm the EVM
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame, final long[] s, final GasCalculator gasCalculator, final EVM evm) {
    return new CreateOperationV2(gasCalculator).execute(frame, evm);
  }
}
