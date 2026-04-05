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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.v2.StackArithmetic;

/**
 * EVM v2 BLOCKHASH operation (0x40).
 *
 * <p>Pops a block number from the stack and pushes its hash, or zero if the block is out of the
 * lookback window (up to 256 blocks prior to the current block).
 */
public class BlockHashOperationV2 extends AbstractOperationV2 {

  /**
   * Instantiates a new BlockHash operation.
   *
   * @param gasCalculator the gas calculator
   */
  public BlockHashOperationV2(final GasCalculator gasCalculator) {
    super(0x40, "BLOCKHASH", 1, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    return staticOperation(frame, frame.stackDataV2());
  }

  /**
   * Execute BLOCKHASH on the v2 long[] stack.
   *
   * @param frame the message frame
   * @param s the stack data
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame, final long[] s) {
    final long cost = 20L;
    if (!frame.stackHasItems(1)) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    }
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    final int top = frame.stackTopV2();
    // If blockArg doesn't fit in a non-negative long, it's out of range
    if (!StackArithmetic.fitsInLong(s, top, 0)) {
      StackArithmetic.putAt(s, top, 0, 0L, 0L, 0L, 0L);
      return new OperationResult(cost, null);
    }

    final long soughtBlock = StackArithmetic.longAt(s, top, 0);
    final BlockValues blockValues = frame.getBlockValues();
    final long currentBlockNumber = blockValues.getNumber();
    final BlockHashLookup blockHashLookup = frame.getBlockHashLookup();

    // If the sought block is in the future, the current block, or outside the lookback window,
    // return zero.
    if (soughtBlock < 0
        || soughtBlock >= currentBlockNumber
        || soughtBlock < (currentBlockNumber - blockHashLookup.getLookback())) {
      StackArithmetic.putAt(s, top, 0, 0L, 0L, 0L, 0L);
    } else {
      final Hash blockHash = blockHashLookup.apply(frame, soughtBlock);
      final byte[] hashBytes = blockHash.getBytes().toArrayUnsafe();
      StackArithmetic.fromBytesAt(s, top, 0, hashBytes, 0, hashBytes.length);
    }

    return new OperationResult(cost, null);
  }
}
