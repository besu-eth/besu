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
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.AbstractOperation;
import org.hyperledger.besu.evm.operation.Operation;

import java.math.BigInteger;

/**
 * EVM v2 EXP operation using long[] stack representation.
 *
 * <p>Performs modular exponentiation: base^exponent mod 2^256. Gas cost is variable, depending on
 * the byte-size of the exponent, so this operation does not extend AbstractFixedCostOperationV2.
 */
public class ExpOperationV2 extends AbstractOperation {

  private static final BigInteger MOD_BASE = BigInteger.TWO.pow(256);

  /**
   * Instantiates a new Exp operation.
   *
   * @param gasCalculator the gas calculator
   */
  public ExpOperationV2(final GasCalculator gasCalculator) {
    super(0x0A, "EXP", 2, 1, gasCalculator);
  }

  @Override
  public Operation.OperationResult execute(final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2(), gasCalculator());
  }

  /**
   * Performs EXP on the v2 long[] stack.
   *
   * @param frame the message frame
   * @param s the stack data as a long[] array
   * @param gasCalculator the gas calculator for computing variable gas cost
   * @return the operation result
   */
  public static Operation.OperationResult staticOperation(
      final MessageFrame frame, final long[] s, final GasCalculator gasCalculator) {
    final int top = frame.stackTopV2();
    final int a = (top - 1) * 4;
    final int b = (top - 2) * 4;

    // Calculate the byte-size of the exponent for gas cost
    final int exponentBitLength = bitLength(s[b], s[b + 1], s[b + 2], s[b + 3]);
    final int numBytes = (exponentBitLength + 7) / 8;

    final long cost = gasCalculator.expOperationGasCost(numBytes);
    if (frame.getRemainingGas() < cost) {
      return new Operation.OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    if (exponentBitLength == 0) {
      // anything^0 = 1
      s[b] = 0L;
      s[b + 1] = 0L;
      s[b + 2] = 0L;
      s[b + 3] = 1L;
    } else {
      final BigInteger base = toBigInteger(s[a], s[a + 1], s[a + 2], s[a + 3]);
      final BigInteger exponent = toBigInteger(s[b], s[b + 1], s[b + 2], s[b + 3]);
      final BigInteger result = base.modPow(exponent, MOD_BASE);

      fromBigInteger(result, s, b);
    }

    frame.setTopV2(top - 1);
    return new Operation.OperationResult(cost, null);
  }

  private static int bitLength(final long u3, final long u2, final long u1, final long u0) {
    if (u3 != 0) return 192 + 64 - Long.numberOfLeadingZeros(u3);
    if (u2 != 0) return 128 + 64 - Long.numberOfLeadingZeros(u2);
    if (u1 != 0) return 64 + 64 - Long.numberOfLeadingZeros(u1);
    if (u0 != 0) return 64 - Long.numberOfLeadingZeros(u0);
    return 0;
  }

  private static BigInteger toBigInteger(
      final long u3, final long u2, final long u1, final long u0) {
    final byte[] bytes = new byte[32];
    for (int i = 0; i < 8; i++) {
      bytes[i] = (byte) (u3 >>> (56 - i * 8));
      bytes[i + 8] = (byte) (u2 >>> (56 - i * 8));
      bytes[i + 16] = (byte) (u1 >>> (56 - i * 8));
      bytes[i + 24] = (byte) (u0 >>> (56 - i * 8));
    }
    return new BigInteger(1, bytes);
  }

  private static void fromBigInteger(final BigInteger value, final long[] s, final int offset) {
    final byte[] bytes = value.toByteArray();
    final int len = bytes.length;

    long u3 = 0, u2 = 0, u1 = 0, u0 = 0;
    for (int i = 0; i < len && i < 32; i++) {
      final int pos = len - 1 - i;
      final long b = bytes[pos] & 0xFFL;
      if (i < 8) {
        u0 |= b << (i * 8);
      } else if (i < 16) {
        u1 |= b << ((i - 8) * 8);
      } else if (i < 24) {
        u2 |= b << ((i - 16) * 8);
      } else {
        u3 |= b << ((i - 24) * 8);
      }
    }

    s[offset] = u3;
    s[offset + 1] = u2;
    s[offset + 2] = u1;
    s[offset + 3] = u0;
  }
}
