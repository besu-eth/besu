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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.readAddressAt;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.SendAllOperation;

/**
 * The EIP-4758 SENDALL operation (0xFF). The sweep itself is {@link SendAllOperation#sendAll},
 * shared with the v1 operation.
 */
public class SendAllOperationV2 extends AbstractOperationV2 {

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SendAllOperationV2(final GasCalculator gasCalculator) {
    super(SendAllOperation.OPCODE, "SENDALL", 1, 0, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    if (frame.isStatic()) {
      return new OperationResult(0, ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    }
    if (!frame.stackHasItemsV2(1)) return UNDERFLOW_RESPONSE;
    final int top = frame.stackTopV2();
    final Address beneficiaryAddress = readAddressAt(frame.stackDataV2(), top, 0);
    frame.setTopV2(top - 1);
    return SendAllOperation.sendAll(frame, beneficiaryAddress, gasCalculator());
  }
}
