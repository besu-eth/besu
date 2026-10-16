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

import static org.hyperledger.besu.evm.v2.operation.AbstractOperationV2.UNDERFLOW_RESPONSE;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readAddressAt;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.log.TransferLogEmitter;
import org.hyperledger.besu.evm.operation.SelfDestructOperation;

/**
 * The Self destruct operation on the v2 long[] stack. The self destruct itself is the one of {@link
 * SelfDestructOperation}.
 */
public class SelfDestructOperationV2 extends SelfDestructOperation {

  /**
   * Instantiates a new Self destruct operation.
   *
   * @param gasCalculator the gas calculator
   * @param eip6780Semantics whether only accounts created in the same transaction are destroyed
   * @param transferLogEmitter the emitter of the log for the transferred balance
   */
  public SelfDestructOperationV2(
      final GasCalculator gasCalculator,
      final boolean eip6780Semantics,
      final TransferLogEmitter transferLogEmitter) {
    super(gasCalculator, eip6780Semantics, transferLogEmitter);
  }

  /**
   * Instantiates the v2 version of a fork's Self destruct operation, with its settings.
   *
   * @param operation the operation the fork registers
   */
  public SelfDestructOperationV2(final SelfDestructOperation operation) {
    super(operation);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    // checking for static violations first means fewer account accesses
    if (frame.isStatic()) {
      return new OperationResult(0, ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    }
    if (!frame.stackHasItemsV2(1)) return UNDERFLOW_RESPONSE;
    final int top = frame.stackTopV2();
    final Address beneficiaryAddress = readAddressAt(frame.stackDataV2(), top, 0);
    frame.setTopV2(top - 1);
    return selfDestruct(frame, beneficiaryAddress);
  }
}
