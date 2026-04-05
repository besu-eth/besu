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

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.v2.StackArithmetic;

import org.apache.tuweni.bytes.Bytes;

/**
 * EVM v2 KECCAK256 operation (0x20).
 *
 * <p>Pops offset and size from the stack, hashes the corresponding memory region with Keccak-256,
 * and pushes the 32-byte hash result. Net stack effect: 2 popped, 1 pushed (top - 1).
 */
public class Keccak256OperationV2 extends AbstractOperationV2 {

  /**
   * Instantiates a new Keccak256 operation.
   *
   * @param gasCalculator the gas calculator
   */
  public Keccak256OperationV2(final GasCalculator gasCalculator) {
    super(0x20, "KECCAK256", 2, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2(), gasCalculator());
  }

  /**
   * Execute KECCAK256 on the v2 long[] stack.
   *
   * @param frame the message frame
   * @param s the stack data
   * @param gasCalculator the gas calculator
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame, final long[] s, final GasCalculator gasCalculator) {
    if (!frame.stackHasItems(2)) {
      return new OperationResult(0, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    }
    final int top = frame.stackTopV2();
    final long from = StackArithmetic.clampedToLong(s, top, 0);
    final long length = StackArithmetic.clampedToLong(s, top, 1);

    final long cost = gasCalculator.keccak256OperationGasCost(frame, from, length);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    final Bytes bytes = frame.readMutableMemory(from, length);
    final byte[] hashBytes = keccak256(bytes).toArrayUnsafe();
    // Pop 2, push 1: net effect is top - 1
    final int newTop = top - 1;
    frame.setTopV2(newTop);
    StackArithmetic.fromBytesAt(s, newTop, 0, hashBytes, 0, hashBytes.length);

    return new OperationResult(cost, null);
  }
}
