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
package org.hyperledger.besu.evm.operation.v2;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation;

/**
 * EVM v2 SDIV operation using long[] stack representation.
 *
 * <p>Performs signed 256-bit integer division using two's complement representation. Division by
 * zero returns zero per EVM specification.
 */
public class SDivOperationV2 extends AbstractFixedCostOperationV2 {

  private static final Operation.OperationResult SDIV_SUCCESS =
      new Operation.OperationResult(5, null);

  /**
   * Instantiates a new SDiv operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SDivOperationV2(final GasCalculator gasCalculator) {
    super(0x05, "SDIV", 2, 1, gasCalculator, gasCalculator.getLowTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2());
  }

  /**
   * Performs SDIV on the v2 long[] stack.
   *
   * @param frame the message frame
   * @param s the stack data as a long[] array
   * @return the operation result
   */
  public static Operation.OperationResult staticOperation(
      final MessageFrame frame, final long[] s) {
    final int top = frame.stackTopV2();
    final int a = (top - 1) * 4;
    final int b = (top - 2) * 4;

    if (s[b] == 0L && s[b + 1] == 0L && s[b + 2] == 0L && s[b + 3] == 0L) {
      s[b] = 0L;
      s[b + 1] = 0L;
      s[b + 2] = 0L;
      s[b + 3] = 0L;
    } else {
      final UInt256 val0 = new UInt256(s[a], s[a + 1], s[a + 2], s[a + 3]);
      final UInt256 val1 = new UInt256(s[b], s[b + 1], s[b + 2], s[b + 3]);
      final UInt256 result = val0.signedDiv(val1);

      s[b] = result.u3();
      s[b + 1] = result.u2();
      s[b + 2] = result.u1();
      s[b + 3] = result.u0();
    }

    frame.setTopV2(top - 1);
    return SDIV_SUCCESS;
  }
}
