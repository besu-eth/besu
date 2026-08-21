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
 * Implements the SIGPARAM (0xb4) operation defined in EIP-8141. Provides access to signature
 * metadata.
 *
 * <p>Stack: signatureIndex, param -> value
 *
 * <p>Gas cost: 2
 */
public class SigParamOperation extends AbstractOperation {

  /** SIGPARAM opcode number. */
  public static final int OPCODE = 0xb4;

  /** Param: effective signer address. */
  public static final int PARAM_SIGNER = 0x00;

  /** Param: scheme (0=SECP256K1, 1=P256). */
  public static final int PARAM_SCHEME = 0x01;

  /** Param: message digest. */
  public static final int PARAM_MSG = 0x02;

  /** Param: signature length. */
  public static final int PARAM_SIG_LENGTH = 0x03;

  private static final long GAS_COST = 2L;

  /**
   * Instantiates a new SigParam operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SigParamOperation(final GasCalculator gasCalculator) {
    super(OPCODE, "SIGPARAM", 2, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    try {
      final Bytes sigIndexBytes = frame.popStackItem();
      frame.popStackItem(); // param

      if (frame.getRemainingGas() < GAS_COST) {
        return new OperationResult(GAS_COST, ExceptionalHaltReason.INSUFFICIENT_GAS);
      }

      // Out-of-bounds signatureIndex causes exceptional halt
      final Bytes trimmedIndex = sigIndexBytes.trimLeadingZeros();
      if (trimmedIndex.size() > 4) {
        return new OperationResult(GAS_COST, ExceptionalHaltReason.OUT_OF_BOUNDS);
      }

      // For now, push zero - full signature metadata access requires MessageFrame integration
      frame.pushStackItem(Bytes32.ZERO);
      return new OperationResult(GAS_COST, null);
    } catch (final UnderflowException ufe) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    }
  }
}
