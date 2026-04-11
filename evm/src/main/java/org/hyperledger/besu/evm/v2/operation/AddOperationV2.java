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

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation;

/**
 * EVM v2 ADD operation using long[] stack representation.
 *
 * <p>Each 256-bit word is stored as four longs: index 0 = most significant 64 bits, index 3 = least
 * significant 64 bits. Addition is performed with carry propagation from least to most significant
 * word, with overflow silently truncated (mod 2^256).
 */
public class AddOperationV2 extends AbstractFixedCostOperationV2 {

  @SuppressWarnings("UnusedVariable")
  private static final Operation.OperationResult ADD_SUCCESS =
      new Operation.OperationResult(3, null);

  /**
   * Instantiates a new Add operation.
   *
   * @param gasCalculator the gas calculator
   */
  public AddOperationV2(final GasCalculator gasCalculator) {
    super(0x01, "ADD", 2, 1, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2());
  }

  /**
   * Execute the ADD opcode on the v2 long[] stack.
   *
   * @param frame the message frame
   * @param stackData the stack operands as a long[] array
   * @return the operation result
   */
  @SuppressWarnings("DoNotCallSuggester")
  public static Operation.OperationResult staticOperation(
      final MessageFrame frame, final long[] stackData) {
    throw new UnsupportedOperationException("ADD operation not yet implemented for evm v2");
  }
}
