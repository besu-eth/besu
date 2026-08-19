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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.pushHash;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.pushZero;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/** The Block hash operation. */
public class BlockHashOperationV2 extends AbstractOperationV2 {

  /** The BLOCKHASH opcode number. */
  public static final int OPCODE = 0x40;

  /**
   * Instantiates a new Block hash operation.
   *
   * @param gasCalculator the gas calculator
   */
  public BlockHashOperationV2(final GasCalculator gasCalculator) {
    super(OPCODE, "BLOCKHASH", 1, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    if (!frame.stackHasItemsV2(1)) return UNDERFLOW_RESPONSE;
    final long cost = gasCalculator().getBlockHashOperationGasCost();
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int offset = (top - 1) << 2;
    final long soughtBlock = stack[offset + 3];
    if ((stack[offset] | stack[offset + 1] | stack[offset + 2]) != 0 || soughtBlock < 0) {
      pushZero(stack, top - 1);
      return new OperationResult(cost, null);
    }

    final BlockValues blockValues = frame.getBlockValues();
    final long currentBlockNumber = blockValues.getNumber();
    final BlockHashLookup blockHashLookup = frame.getBlockHashLookup();
    if (soughtBlock >= currentBlockNumber
        || soughtBlock < currentBlockNumber - blockHashLookup.getLookback()) {
      pushZero(stack, top - 1);
    } else {
      final Hash blockHash = blockHashLookup.apply(frame, soughtBlock);
      pushHash(blockHash, stack, top - 1);
    }
    return new OperationResult(cost, null);
  }
}
