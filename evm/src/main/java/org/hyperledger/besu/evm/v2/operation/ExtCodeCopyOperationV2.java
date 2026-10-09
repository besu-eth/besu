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

import static org.hyperledger.besu.evm.internal.Words.clampedAdd;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.clampedToLong;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readAddressAt;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.bytes.Bytes;

/** The Ext code copy operation. */
public class ExtCodeCopyOperationV2 extends AbstractOperationV2 {

  /**
   * Instantiates a new Ext code copy operation.
   *
   * @param gasCalculator the gas calculator
   */
  public ExtCodeCopyOperationV2(final GasCalculator gasCalculator) {
    super(0x3C, "EXTCODECOPY", 4, 0, gasCalculator);
  }

  /**
   * Cost of Ext code copy operation, including the warm or cold account access.
   *
   * @param frame the frame
   * @param memOffset the mem offset
   * @param length the length
   * @param accountIsWarm whether the account was already warm
   * @return the long
   */
  protected long cost(
      final MessageFrame frame,
      final long memOffset,
      final long length,
      final boolean accountIsWarm) {
    return clampedAdd(
        gasCalculator().extCodeCopyOperationGasCost(frame, memOffset, length),
        accountIsWarm
            ? gasCalculator().getWarmStorageReadCost()
            : gasCalculator().getColdAccountAccessCost());
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    if (!frame.stackHasItemsV2(4)) return UNDERFLOW_RESPONSE;
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final Address address = readAddressAt(stack, top, 0);
    final long memOffset = clampedToLong(stack, top, 1);
    final long sourceOffset = clampedToLong(stack, top, 2);
    final long numBytes = clampedToLong(stack, top, 3);

    final boolean accountIsWarm =
        frame.warmUpAddress(address) || gasCalculator().isPrecompile(address);
    final long cost = cost(frame, memOffset, numBytes, accountIsWarm);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    final Account account = getAccount(address, frame);
    final Bytes code = account != null ? account.getCode() : Bytes.EMPTY;
    frame.writeMemory(memOffset, sourceOffset, numBytes, code);
    frame.setTopV2(top - 4);
    return new OperationResult(cost, null);
  }
}
