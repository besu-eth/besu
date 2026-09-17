/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.evm.operation;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.bytes.Bytes;

/** The Push operation. */
public class PushOperation extends AbstractFixedCostOperation {

  /** The constant PUSH_BASE. */
  public static final int PUSH_BASE = 0x5F;

  /** The constant PUSH_MAX. */
  public static final int PUSH_MAX = 0x7F;

  private final int length;

  /** The Push operation success result. */
  static final OperationResult pushSuccess = new OperationResult(3, null);

  /**
   * Instantiates a new Push operation.
   *
   * @param length the length
   * @param gasCalculator the gas calculator
   */
  public PushOperation(final int length, final GasCalculator gasCalculator) {
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
    Bytes push;
    if (code.length <= copyStart) {
      push = Bytes.EMPTY;
    } else {
      final int copyLength = Math.min(pushSize, code.length - pc - 1);
      final int rightPad = pushSize - copyLength;
      if (rightPad == 0) {
        push = Bytes.wrap(code, copyStart, copyLength);
      } else {
        // Right Pad the push with 0s up to pushSize if greater than the copyLength
        var bytecodeLocal = new byte[pushSize];
        System.arraycopy(code, copyStart, bytecodeLocal, 0, copyLength);
        push = Bytes.wrap(bytecodeLocal);
      }
    }
    frame.pushStackItem(push);
    frame.setPC(pc + pushSize);
    return pushSuccess;
  }
}
