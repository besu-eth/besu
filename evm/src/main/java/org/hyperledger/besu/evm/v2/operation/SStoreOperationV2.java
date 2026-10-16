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
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readBytes32At;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.SStoreOperation;

import org.apache.tuweni.units.bigints.UInt256;

/**
 * The SStore operation on the v2 long[] stack. The store itself, with its gas, state gas and block
 * access list entries, is the one of {@link SStoreOperation}.
 */
public class SStoreOperationV2 extends SStoreOperation {

  /**
   * Instantiates a new SStore operation.
   *
   * @param gasCalculator the gas calculator
   * @param minimumGasRemaining the minimum gas remaining
   */
  public SStoreOperationV2(final GasCalculator gasCalculator, final long minimumGasRemaining) {
    super(gasCalculator, minimumGasRemaining);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    if (!frame.stackHasItemsV2(2)) return UNDERFLOW_RESPONSE;
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final UInt256 key = UInt256.fromBytes(readBytes32At(stack, top, 0));
    final UInt256 newValue = UInt256.fromBytes(readBytes32At(stack, top, 1));
    frame.setTopV2(top - 2);
    return store(frame, key, newValue);
  }
}
