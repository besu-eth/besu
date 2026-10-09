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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.clampedToLong;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readAddressAt;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/** The Delegate call operation. */
public class DelegateCallOperationV2 extends AbstractCallOperationV2 {

  /**
   * Instantiates a new Delegate call operation.
   *
   * @param gasCalculator the gas calculator
   */
  public DelegateCallOperationV2(final GasCalculator gasCalculator) {
    super(0xF4, "DELEGATECALL", 6, 1, gasCalculator);
  }

  @Override
  protected Address to(final MessageFrame frame) {
    return readAddressAt(frame.stackDataV2(), frame.stackTopV2(), 1);
  }

  @Override
  protected Wei value(final MessageFrame frame) {
    return Wei.ZERO;
  }

  @Override
  protected Wei apparentValue(final MessageFrame frame) {
    return frame.getApparentValue();
  }

  @Override
  protected long inputDataOffset(final MessageFrame frame) {
    return clampedToLong(frame.stackDataV2(), frame.stackTopV2(), 2);
  }

  @Override
  protected long inputDataLength(final MessageFrame frame) {
    return clampedToLong(frame.stackDataV2(), frame.stackTopV2(), 3);
  }

  @Override
  protected long outputDataOffset(final MessageFrame frame) {
    return clampedToLong(frame.stackDataV2(), frame.stackTopV2(), 4);
  }

  @Override
  protected long outputDataLength(final MessageFrame frame) {
    return clampedToLong(frame.stackDataV2(), frame.stackTopV2(), 5);
  }

  @Override
  protected Address address(final MessageFrame frame) {
    return frame.getRecipientAddress();
  }

  @Override
  protected Address sender(final MessageFrame frame) {
    return frame.getSenderAddress();
  }

  @Override
  public long gasAvailableForChildCall(final MessageFrame frame) {
    return gasCalculator().gasAvailableForChildCall(frame, gas(frame), false);
  }
}
