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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * EVM v2 CHAINID operation — pushes the pre-computed chain ID onto the stack (Istanbul+).
 *
 * <p>The four 64-bit limbs of the chain ID are computed once at construction time.
 */
public class ChainIdOperationV2 extends AbstractFixedCostOperationV2 {

  private static final VarHandle LONG_BE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

  /** CHAINID opcode number. */
  public static final int OPCODE = 0x46;

  private final long chainIdU3;
  private final long chainIdU2;
  private final long chainIdU1;
  private final long chainIdU0;

  /**
   * Instantiates a new Chain ID operation.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain ID (left-padded to 32 bytes big-endian)
   */
  public ChainIdOperationV2(final GasCalculator gasCalculator, final Bytes chainId) {
    super(OPCODE, "CHAINID", 0, 1, gasCalculator, gasCalculator.getBaseTierGasCost());
    final byte[] b = Bytes32.leftPad(chainId).toArrayUnsafe();
    this.chainIdU3 = (long) LONG_BE.get(b, 0);
    this.chainIdU2 = (long) LONG_BE.get(b, 8);
    this.chainIdU1 = (long) LONG_BE.get(b, 16);
    this.chainIdU0 = (long) LONG_BE.get(b, 24);
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    if (!frame.stackHasSpace(1)) return OVERFLOW_RESPONSE;
    final long[] s = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int dst = top << 2;
    s[dst] = chainIdU3;
    s[dst + 1] = chainIdU2;
    s[dst + 2] = chainIdU1;
    s[dst + 3] = chainIdU0;
    frame.setTopV2(top + 1);
    return successResponse;
  }
}
