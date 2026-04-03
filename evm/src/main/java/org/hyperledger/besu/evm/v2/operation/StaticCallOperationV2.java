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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.InvalidOperation;
import org.hyperledger.besu.evm.v2.StackArithmetic;

/**
 * EVM v2 STATICCALL operation (opcode 0xFA, Byzantium+). Stack layout: gas, to, inOffset, inSize,
 * outOffset, outSize → result.
 */
public class StaticCallOperationV2 extends AbstractCallOperationV2 {

  /**
   * Instantiates a new STATICCALL operation for EVM v2.
   *
   * @param gasCalculator the gas calculator
   */
  public StaticCallOperationV2(final GasCalculator gasCalculator) {
    super(0xFA, "STATICCALL", 6, 1, gasCalculator);
  }

  @Override
  protected Address to(final long[] s, final int top) {
    return StackArithmetic.toAddressAt(s, top, 1);
  }

  @Override
  protected Wei value(final long[] s, final int top) {
    return Wei.ZERO;
  }

  @Override
  protected Wei apparentValue(final MessageFrame frame, final long[] s, final int top) {
    return value(s, top);
  }

  @Override
  protected long inputDataOffset(final long[] s, final int top) {
    return StackArithmetic.clampedToLong(s, top, 2);
  }

  @Override
  protected long inputDataLength(final long[] s, final int top) {
    return StackArithmetic.clampedToLong(s, top, 3);
  }

  @Override
  protected long outputDataOffset(final long[] s, final int top) {
    return StackArithmetic.clampedToLong(s, top, 4);
  }

  @Override
  protected long outputDataLength(final long[] s, final int top) {
    return StackArithmetic.clampedToLong(s, top, 5);
  }

  @Override
  protected Address address(final MessageFrame frame, final long[] s, final int top) {
    return to(s, top);
  }

  @Override
  protected Address sender(final MessageFrame frame) {
    return frame.getRecipientAddress();
  }

  @Override
  public long gasAvailableForChildCall(final MessageFrame frame, final long[] s, final int top) {
    return gasCalculator().gasAvailableForChildCall(frame, gas(s, top), false);
  }

  @Override
  protected boolean isStatic(final MessageFrame frame) {
    return true;
  }

  /**
   * Performs the STATICCALL operation on the v2 stack. Only available from Byzantium onwards.
   *
   * @param frame the current message frame
   * @param s the v2 stack array
   * @param gasCalculator the gas calculator
   * @param evm the EVM
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame, final long[] s, final GasCalculator gasCalculator, final EVM evm) {
    if (EvmSpecVersion.BYZANTIUM.ordinal() > evm.getEvmVersion().ordinal()) {
      return InvalidOperation.invalidOperationResult(0xFA);
    }
    return new StaticCallOperationV2(gasCalculator).execute(frame, evm);
  }
}
