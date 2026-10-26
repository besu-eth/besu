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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.readClampedIntAt;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.setAddress;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.setBytes32;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.setLong;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.SigParamOperation;

/** The EIP-8141 SIGPARAM operation (0xb4): signature-entry introspection. */
public class SigParamOperationV2 extends AbstractOperationV2 {

  private static final long GAS_COST = 2L;
  private static final OperationResult SUCCESS = new OperationResult(GAS_COST, null);
  private static final OperationResult INVALID =
      new OperationResult(GAS_COST, ExceptionalHaltReason.INVALID_OPERATION);
  private static final OperationResult OUT_OF_BOUNDS =
      new OperationResult(GAS_COST, ExceptionalHaltReason.OUT_OF_BOUNDS);

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SigParamOperationV2(final GasCalculator gasCalculator) {
    super(SigParamOperation.OPCODE, "SIGPARAM", 2, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final var maybeContext = frame.getFrameTransactionContext();
    if (maybeContext.isEmpty()) {
      return INVALID;
    }
    final FrameTransactionContext context = maybeContext.get();
    if (!frame.stackHasItemsV2(2)) return haltPoppingAll(frame, UNDERFLOW_RESPONSE);
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int sigIndex = readClampedIntAt(stack, top, 0);
    final int param = readClampedIntAt(stack, top, 1);

    if (sigIndex >= context.signatures().size()) {
      return haltPopping(frame, 2, OUT_OF_BOUNDS);
    }
    final FrameTransactionContext.SignatureInfo signature = context.signatures().get(sigIndex);
    final boolean isArbitrary = signature.scheme() == FrameTransactionContext.SCHEME_ARBITRARY;
    final int slot = top - 2;
    switch (param) {
      case 0x00 -> {
        // ARBITRARY entries have no resolved signer.
        if (isArbitrary) {
          return haltPopping(frame, 2, INVALID);
        }
        setAddress(signature.resolvedSigner(), stack, slot);
      }
      case 0x01 -> setLong(signature.scheme(), stack, slot);
      case 0x02 -> setBytes32(signature.msg(), stack, slot);
      case 0x03 -> {
        // The byte length of protocol-validated raw signatures is not introspectable.
        if (!isArbitrary) {
          return haltPopping(frame, 2, INVALID);
        }
        setLong(signature.arbitraryBytes().size(), stack, slot);
      }
      default -> {
        return haltPopping(frame, 2, INVALID);
      }
    }
    frame.setTopV2(top - 1);
    return SUCCESS;
  }
}
