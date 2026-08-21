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

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.UnderflowException;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Implements the TXPARAM (0xb0) operation defined in EIP-8141. Provides access to transaction-level
 * metadata for frame transactions.
 *
 * <p>Stack: param_id -> value
 *
 * <p>Gas cost: 2
 */
public class TxParamOperation extends AbstractOperation {

  /** TXPARAM opcode number. */
  public static final int OPCODE = 0xb0;

  /** Param: current transaction type. */
  public static final int PARAM_TX_TYPE = 0x00;

  /** Param: nonce. */
  public static final int PARAM_NONCE = 0x01;

  /** Param: sender address. */
  public static final int PARAM_SENDER = 0x02;

  /** Param: max priority fee per gas. */
  public static final int PARAM_MAX_PRIORITY_FEE = 0x03;

  /** Param: max fee per gas. */
  public static final int PARAM_MAX_FEE = 0x04;

  /** Param: max fee per blob gas. */
  public static final int PARAM_MAX_BLOB_FEE = 0x05;

  /** Param: maximum total cost. */
  public static final int PARAM_MAX_TOTAL_COST = 0x06;

  /** Param: blob versioned hash count. */
  public static final int PARAM_BLOB_HASH_COUNT = 0x07;

  /** Param: canonical signature hash. */
  public static final int PARAM_SIG_HASH = 0x08;

  /** Param: number of frames. */
  public static final int PARAM_FRAME_COUNT = 0x09;

  /** Param: currently executing frame index. */
  public static final int PARAM_CURRENT_FRAME = 0x0A;

  /** Param: number of signatures. */
  public static final int PARAM_SIG_COUNT = 0x0B;

  private static final long GAS_COST = 2L;

  /**
   * Instantiates a new TxParam operation.
   *
   * @param gasCalculator the gas calculator
   */
  public TxParamOperation(final GasCalculator gasCalculator) {
    super(OPCODE, "TXPARAM", 1, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    try {
      final Bytes paramIdBytes = frame.popStackItem();
      if (frame.getRemainingGas() < GAS_COST) {
        return new OperationResult(GAS_COST, ExceptionalHaltReason.INSUFFICIENT_GAS);
      }

      final Bytes trimmed = paramIdBytes.trimLeadingZeros();
      final int paramId = trimmed.isEmpty() ? 0 : trimmed.toInt();

      final Bytes result =
          switch (paramId) {
            case PARAM_TX_TYPE -> Bytes32.leftPad(Bytes.of(0x06));
            case PARAM_SENDER -> Bytes32.leftPad(frame.getOriginatorAddress().getBytes());
            default -> Bytes32.ZERO;
          };

      frame.pushStackItem(result);
      return new OperationResult(GAS_COST, null);
    } catch (final UnderflowException ufe) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    }
  }
}
