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
 * Implements the FRAMEPARAM (0xb3) operation defined in EIP-8141. Provides access to per-frame
 * metadata.
 *
 * <p>Stack: frameIndex, param -> value
 *
 * <p>Gas cost: 2
 */
public class FrameParamOperation extends AbstractOperation {

  /** FRAMEPARAM opcode number. */
  public static final int OPCODE = 0xb3;

  /** Param: resolved target address. */
  public static final int PARAM_TARGET = 0x00;

  /** Param: gas limit. */
  public static final int PARAM_GAS_LIMIT = 0x01;

  /** Param: mode (0/1/2). */
  public static final int PARAM_MODE = 0x02;

  /** Param: flags. */
  public static final int PARAM_FLAGS = 0x03;

  /** Param: data length. */
  public static final int PARAM_DATA_LENGTH = 0x04;

  /** Param: status (1=success, 0=fail). */
  public static final int PARAM_STATUS = 0x05;

  /** Param: allowed scope (flags &amp; 3). */
  public static final int PARAM_ALLOWED_SCOPE = 0x06;

  /** Param: atomic batch flag (bit 2). */
  public static final int PARAM_ATOMIC_BATCH = 0x07;

  /** Param: value. */
  public static final int PARAM_VALUE = 0x08;

  private static final long GAS_COST = 2L;

  /**
   * Instantiates a new FrameParam operation.
   *
   * @param gasCalculator the gas calculator
   */
  public FrameParamOperation(final GasCalculator gasCalculator) {
    super(OPCODE, "FRAMEPARAM", 2, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    try {
      final Bytes frameIndexBytes = frame.popStackItem();
      frame.popStackItem(); // param

      if (frame.getRemainingGas() < GAS_COST) {
        return new OperationResult(GAS_COST, ExceptionalHaltReason.INSUFFICIENT_GAS);
      }

      // Out-of-bounds frameIndex causes exceptional halt
      final Bytes trimmedIndex = frameIndexBytes.trimLeadingZeros();
      if (trimmedIndex.size() > 4) {
        return new OperationResult(GAS_COST, ExceptionalHaltReason.OUT_OF_BOUNDS);
      }

      // For now, push zero - full frame metadata access requires MessageFrame integration
      frame.pushStackItem(Bytes32.ZERO);
      return new OperationResult(GAS_COST, null);
    } catch (final UnderflowException ufe) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    }
  }
}
