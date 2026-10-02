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

import static org.hyperledger.besu.crypto.Hash.keccak256;
import static org.hyperledger.besu.evm.internal.Words.clampedAdd;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.InvalidOperation;
import org.hyperledger.besu.evm.v2.StackArithmetic;

import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * EVM v2 CREATE2 operation (opcode 0xF5, Constantinople+). Stack layout: value, offset, size, salt
 * → contractAddress.
 */
public class Create2OperationV2 extends AbstractCreateOperationV2 {

  private static final Bytes PREFIX = Bytes.fromHexString("0xFF");

  /**
   * Instantiates a new CREATE2 operation for EVM v2.
   *
   * @param gasCalculator the gas calculator
   */
  public Create2OperationV2(final GasCalculator gasCalculator) {
    super(0xF5, "CREATE2", 4, 1, gasCalculator);
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
        clampedAdd(
            gasCalculator().createKeccakCost(inputSize), gasCalculator().initcodeCost(inputSize)));
  }

  @Override
  protected Address generateTargetContractAddress(
      final MessageFrame frame, final long[] s, final int top, final Code initcode) {
    final Address sender = frame.getRecipientAddress();
    final byte[] saltBytes = new byte[32];
    StackArithmetic.toBytesAt(s, top, 3, saltBytes);
    final Bytes32 salt = Bytes32.wrap(saltBytes);
    final Bytes32 hash =
        keccak256(
            Bytes.concatenate(PREFIX, sender.getBytes(), salt, initcode.getCodeHash().getBytes()));
    return Address.extract(hash);
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
   * Performs the CREATE2 operation on the v2 stack. Only available from Constantinople onwards.
   *
   * @param frame the current message frame
   * @param s the v2 stack array
   * @param gasCalculator the gas calculator
   * @param evm the EVM
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame, final long[] s, final GasCalculator gasCalculator, final EVM evm) {
    if (EvmSpecVersion.CONSTANTINOPLE.ordinal() > evm.getEvmVersion().ordinal()) {
      return InvalidOperation.invalidOperationResult(0xF5);
    }
    return new Create2OperationV2(gasCalculator).execute(frame, evm);
  }
}
