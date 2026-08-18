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

import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/** The Push operation. */
public class PushOperationV2 extends AbstractFixedCostOperationV2 {
  /** The constant PUSH_BASE. */
  public static final int PUSH_BASE = 0x5F;

  private final int length;

  /** The Push operation success result. */
  static final OperationResult pushSuccess = new OperationResult(3, null);

  /**
   * Instantiates a new Push operation.
   *
   * @param length the length
   * @param gasCalculator the gas calculator
   */
  public PushOperationV2(final int length, final GasCalculator gasCalculator) {
    super(
        PUSH_BASE + length,
        "PUSH" + length,
        0,
        1,
        gasCalculator,
        gasCalculator.getVeryLowTierGasCost());
    this.length = length;
  }

  @Override
  public OperationResult executeFixedCostOperation(final MessageFrame frame) {
    final byte[] code = frame.getCode().getBytes().toArrayUnsafe();
    return staticOperation(frame, code, frame.getPC(), length);
  }

  /**
   * Performs Push operation.
   *
   * @param frame the frame
   * @param code the code
   * @param pc the pc
   * @param pushSize the push size
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame, final byte[] code, final int pc, final int pushSize) {
    final int copyStart = pc + 1;
    final int remainingSize = Math.max(0, code.length - copyStart);
    final int copyLength = Math.min(remainingSize, pushSize);

    UInt256 pushValue = UInt256.fromBytesBE(code, copyStart, copyStart + copyLength);
    pushValue = pushValue.shl0(Math.max(0, (pushSize - remainingSize) * 8));

    final int top = frame.stackTopV2();
    final int offset = top << 2;
    frame.stackDataV2()[offset] = pushValue.u3();
    frame.stackDataV2()[offset + 1] = pushValue.u2();
    frame.stackDataV2()[offset + 2] = pushValue.u1();
    frame.stackDataV2()[offset + 3] = pushValue.u0();
    frame.setTopV2(top + 1);

    frame.setPC(pc + pushSize);
    return pushSuccess;
  }
}
