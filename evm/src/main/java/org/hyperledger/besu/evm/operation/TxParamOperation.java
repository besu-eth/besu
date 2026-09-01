/*
 * Copyright contributors to Hyperledger Besu.
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

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.Words;

import org.apache.tuweni.bytes.Bytes;

/**
 * The EIP-8141 TXPARAM operation (0xb0): transaction-scoped introspection of a frame transaction.
 */
public class TxParamOperation extends AbstractOperation {

  /** The TXPARAM opcode. */
  public static final int OPCODE = 0xb0;

  private static final long GAS_COST = 2L;

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   */
  public TxParamOperation(final GasCalculator gasCalculator) {
    super(OPCODE, "TXPARAM", 1, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final var maybeContext = frame.getFrameTransactionContext();
    if (maybeContext.isEmpty()) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.INVALID_OPERATION);
    }
    final FrameTransactionContext context = maybeContext.get();

    final Bytes paramBytes = frame.popStackItem().trimLeadingZeros();
    if (paramBytes.size() > 1) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.INVALID_OPERATION);
    }
    final int param = paramBytes.isEmpty() ? 0 : Byte.toUnsignedInt(paramBytes.get(0));

    final Bytes result =
        switch (param) {
          case 0x00 -> Bytes.of(0x06);
          case 0x01 -> Words.longBytes(context.nonce());
          case 0x02 -> context.sender().getBytes();
          case 0x03 -> context.maxPriorityFeePerGas().toBytes();
          case 0x04 -> context.maxFeePerGas().toBytes();
          case 0x05 -> context.maxFeePerBlobGas().toBytes();
          case 0x06 -> context.maxCost().toBytes();
          case 0x07 ->
              Words.longBytes(frame.getVersionedHashes().map(hashes -> hashes.size()).orElse(0));
          case 0x08 -> context.signatureHash();
          case 0x09 -> Words.longBytes(context.frames().size());
          case 0x0A -> Words.longBytes(context.currentFrameIndex());
          case 0x0B -> Words.longBytes(context.signatures().size());
          case 0x0C -> Words.longBytes(frame.getStateGasReservoir());
          default -> null;
        };
    if (result == null) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.INVALID_OPERATION);
    }
    frame.pushStackItem(result);
    return new OperationResult(GAS_COST, null);
  }
}
