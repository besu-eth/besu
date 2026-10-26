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

import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.SigDataCopyOperation;

import org.apache.tuweni.bytes.Bytes;

/**
 * The EIP-8141 SIGDATACOPY operation (0xb5): copies an ARBITRARY signature entry's raw bytes into
 * memory.
 */
public class SigDataCopyOperationV2 extends AbstractFrameDataCopyOperationV2 {

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SigDataCopyOperationV2(final GasCalculator gasCalculator) {
    super(SigDataCopyOperation.OPCODE, "SIGDATACOPY", gasCalculator);
  }

  @Override
  protected ExceptionalHaltReason check(final FrameTransactionContext context, final int sigIndex) {
    if (sigIndex >= context.signatures().size()) {
      return ExceptionalHaltReason.OUT_OF_BOUNDS;
    }
    return context.signatures().get(sigIndex).scheme() == FrameTransactionContext.SCHEME_ARBITRARY
        ? null
        : ExceptionalHaltReason.INVALID_OPERATION;
  }

  @Override
  protected Bytes data(final FrameTransactionContext context, final int sigIndex) {
    return context.signatures().get(sigIndex).arbitraryBytes();
  }
}
