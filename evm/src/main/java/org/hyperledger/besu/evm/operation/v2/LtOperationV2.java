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
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation;

/** EVM v2 LT operation using long[] stack representation. */
public class LtOperationV2 extends AbstractFixedCostOperationV2 {

  private static final Operation.OperationResult LT_SUCCESS =
      new Operation.OperationResult(3, null);

  /**
   * Instantiates a new Lt operation.
   *
   * @param gasCalculator the gas calculator
   */
  public LtOperationV2(final GasCalculator gasCalculator) {
    super(0x10, "LT", 2, 1, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2());
  }

  /**
   * Performs unsigned LT on the v2 long[] stack.
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

    int cmp = Long.compareUnsigned(s[a], s[b]);
    if (cmp == 0) {
      cmp = Long.compareUnsigned(s[a + 1], s[b + 1]);
      if (cmp == 0) {
        cmp = Long.compareUnsigned(s[a + 2], s[b + 2]);
        if (cmp == 0) {
          cmp = Long.compareUnsigned(s[a + 3], s[b + 3]);
        }
      }
    }

    s[b] = 0L;
    s[b + 1] = 0L;
    s[b + 2] = 0L;
    s[b + 3] = cmp < 0 ? 1L : 0L;

    frame.setTopV2(top - 1);
    return LT_SUCCESS;
  }
}
