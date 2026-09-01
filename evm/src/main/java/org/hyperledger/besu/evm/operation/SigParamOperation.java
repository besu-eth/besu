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

/** The EIP-8141 SIGPARAM operation (0xb4): signature-entry introspection. */
public class SigParamOperation extends AbstractOperation {

  /** The SIGPARAM opcode. */
  public static final int OPCODE = 0xb4;

  private static final long GAS_COST = 2L;

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SigParamOperation(final GasCalculator gasCalculator) {
    super(OPCODE, "SIGPARAM", 2, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final var maybeContext = frame.getFrameTransactionContext();
    if (maybeContext.isEmpty()) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.INVALID_OPERATION);
    }
    final FrameTransactionContext context = maybeContext.get();

    final Bytes sigIndexWord = frame.popStackItem().trimLeadingZeros();
    final Bytes paramBytes = frame.popStackItem().trimLeadingZeros();

    if (sigIndexWord.size() > 4) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.OUT_OF_BOUNDS);
    }
    final int sigIndex = sigIndexWord.isEmpty() ? 0 : sigIndexWord.toInt();
    if (sigIndex < 0 || sigIndex >= context.signatures().size()) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.OUT_OF_BOUNDS);
    }
    if (paramBytes.size() > 1) {
      return new OperationResult(GAS_COST, ExceptionalHaltReason.INVALID_OPERATION);
    }
    final int param = paramBytes.isEmpty() ? 0 : Byte.toUnsignedInt(paramBytes.get(0));
    final FrameTransactionContext.SignatureInfo signature = context.signatures().get(sigIndex);
    final boolean isArbitrary = signature.scheme() == FrameTransactionContext.SCHEME_ARBITRARY;

    final Bytes result;
    switch (param) {
      case 0x00 -> {
        // ARBITRARY entries have no resolved signer.
        if (isArbitrary) {
          return new OperationResult(GAS_COST, ExceptionalHaltReason.INVALID_OPERATION);
        }
        result = signature.resolvedSigner().getBytes();
      }
      case 0x01 -> result = Words.longBytes(signature.scheme());
      case 0x02 -> result = signature.msg() == null ? Bytes.EMPTY : signature.msg();
      case 0x03 -> {
        // The byte length of protocol-validated raw signatures is not introspectable.
        if (!isArbitrary) {
          return new OperationResult(GAS_COST, ExceptionalHaltReason.INVALID_OPERATION);
        }
        result = Words.longBytes(signature.arbitraryBytes().size());
      }
      default -> {
        return new OperationResult(GAS_COST, ExceptionalHaltReason.INVALID_OPERATION);
      }
    }
    frame.pushStackItem(result);
    return new OperationResult(GAS_COST, null);
  }
}
