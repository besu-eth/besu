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

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes32;

/** The Chain id operation. */
public class ChainIdOperationV2 extends AbstractFixedCostOperationV2 {

  private static final VarHandle LONG_BE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

  /** The CHAINID Opcode number */
  public static final int OPCODE = 0x46;

  private final Bytes32 chainId;

  // Cached big-endian limbs of the chainId for zero-allocation pushes to the EVM v2 stack.
  private final long chainIdU3;
  private final long chainIdU2;
  private final long chainIdU1;
  private final long chainIdU0;

  /**
   * Instantiates a new Chain id operation.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   */
  public ChainIdOperationV2(final GasCalculator gasCalculator, final Bytes32 chainId) {
    super(OPCODE, "CHAINID", 0, 1, gasCalculator, gasCalculator.getBaseTierGasCost());
    this.chainId = chainId;
    final byte[] b = chainId.toArrayUnsafe();
    this.chainIdU3 = (long) LONG_BE.get(b, 0);
    this.chainIdU2 = (long) LONG_BE.get(b, 8);
    this.chainIdU1 = (long) LONG_BE.get(b, 16);
    this.chainIdU0 = (long) LONG_BE.get(b, 24);
  }

  /**
   * Returns the chain ID this operation uses.
   *
   * @return the chain id
   */
  @VisibleForTesting
  Bytes32 getChainId() {
    return chainId;
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    if (!frame.stackHasSpaceV2(1)) return OVERFLOW_RESPONSE;
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int offset = top << 2;
    stack[offset] = chainIdU3;
    stack[offset + 1] = chainIdU2;
    stack[offset + 2] = chainIdU1;
    stack[offset + 3] = chainIdU0;
    frame.setTopV2(top + 1);
    return successResponse;
  }
}
