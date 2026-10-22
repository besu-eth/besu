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
import org.hyperledger.besu.evm.operation.FrameDataCopyOperation;

import org.apache.tuweni.bytes.Bytes;

/** The EIP-8141 FRAMEDATACOPY operation (0xb2): copies a chosen frame's data into memory. */
public class FrameDataCopyOperationV2 extends AbstractFrameDataCopyOperationV2 {

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   */
  public FrameDataCopyOperationV2(final GasCalculator gasCalculator) {
    super(FrameDataCopyOperation.OPCODE, "FRAMEDATACOPY", gasCalculator);
  }

  @Override
  protected ExceptionalHaltReason check(
      final FrameTransactionContext context, final int frameIndex) {
    return frameIndex < context.frames().size() ? null : ExceptionalHaltReason.OUT_OF_BOUNDS;
  }

  @Override
  protected Bytes data(final FrameTransactionContext context, final int frameIndex) {
    return context.frames().get(frameIndex).data();
  }
}
