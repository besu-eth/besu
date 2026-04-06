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
package org.hyperledger.besu.evm.operation;

import static org.hyperledger.besu.evm.operation.Shift256Operations.getLong;
import static org.hyperledger.besu.evm.operation.Shift256Operations.isShiftOverflow;
import static org.hyperledger.besu.evm.operation.Shift256Operations.putLong;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * The optimized SHL (Shift Left) operation.
 *
 * <p>This implementation uses direct byte[] manipulation instead of Tuweni's Bytes.shiftLeft() to
 * avoid intermediate object allocation and improve performance.
 */
public class ShlOperationOptimized extends AbstractFixedCostOperation {

  /** The Shl operation success result. */
  static final OperationResult shlSuccess = new OperationResult(3, null);

  /**
   * Instantiates a new optimized Shl operation.
   *
   * @param gasCalculator the gas calculator
   */
  public ShlOperationOptimized(final GasCalculator gasCalculator) {
    super(0x1b, "SHL", 2, 1, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    return staticOperation(frame);
  }

  /**
   * Performs optimized Shift Left operation.
   *
   * @param frame the frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame) {
    final Bytes shiftAmount = frame.popStackItem();
    final Bytes value = frame.popStackItem();
    if (value.isZero()) {
      frame.pushStackItem(Bytes.EMPTY);
      return shlSuccess;
    }

    final byte[] valueBytes = Bytes32.leftPad(value).toArrayUnsafe();
    final byte[] shiftBytes = shiftAmount.toArrayUnsafe();

    // shift >= 256, push All 0s
    if (isShiftOverflow(shiftBytes)) {
      frame.pushStackItem(Bytes.EMPTY);
      return shlSuccess;
    }

    final int shift = shiftBytes.length == 0 ? 0 : (shiftBytes[shiftBytes.length - 1] & 0xFF);

    frame.pushStackItem(shl256(valueBytes, shift));
    return shlSuccess;
  }

  /**
   * Performs a 256-bit logical left shift (EVM SHL).
   *
   * <p>The shift amount is in the range {@code [0..255]} and is assumed to have been validated by
   * the caller. For shift values >= 256, zero is returned by the caller.
   *
   * @param in the raw 32-byte array of the input value
   * @param shift the left shift amount in bits (0–255)
   * @return the shifted 256-bit value
   */
  private static Bytes shl256(final byte[] in, final int shift) {
    if (shift == 0) return Bytes.wrap(in);
    long w0 = getLong(in, 0);
    long w1 = getLong(in, 8);
    long w2 = getLong(in, 16);
    long w3 = getLong(in, 24);

    // Number of whole 64-bit words to shift (shift / 64).
    final int wordShift = shift >>> 6;
    // Remaining intra-word bit shift (shift % 64).
    final int bitShift = shift & 63;
    switch (wordShift) {
      case 0:
        w0 = shiftLeft(w0, w1, bitShift);
        w1 = shiftLeft(w1, w2, bitShift);
        w2 = shiftLeft(w2, w3, bitShift);
        w3 = shiftLeft(w3, 0, bitShift);
        break;
      case 1:
        w0 = shiftLeft(w1, w2, bitShift);
        w1 = shiftLeft(w2, w3, bitShift);
        w2 = shiftLeft(w3, 0, bitShift);
        w3 = 0;
        break;
      case 2:
        w0 = shiftLeft(w2, w3, bitShift);
        w1 = shiftLeft(w3, 0, bitShift);
        w2 = 0;
        w3 = 0;
        break;
      case 3:
        w0 = shiftLeft(w3, 0, bitShift);
        w1 = 0;
        w2 = 0;
        w3 = 0;
        break;
    }
    final byte[] out = new byte[32];
    putLong(out, 0, w0);
    putLong(out, 8, w1);
    putLong(out, 16, w2);
    putLong(out, 24, w3);
    return Bytes.wrap(out);
  }

  /**
   * Shifts a 64-bit word left and carries in bits from the next less-significant word.
   *
   * <p>The {@code bitShift == 0} fast path avoids Java long-shift masking, where a shift by 64 is
   * treated as a shift by 0.
   *
   * @param value the current word
   * @param nextValue the next less-significant word
   * @param bitShift the intra-word shift amount in the range {@code [0..63]}
   * @return the shifted word
   */
  private static long shiftLeft(final long value, final long nextValue, final int bitShift) {
    if (bitShift == 0) return value;
    return (value << bitShift) | (nextValue >>> (64 - bitShift));
  }
}
