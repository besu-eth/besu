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

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/** EVM v2 INVALID operation. Always halts with INVALID_OPERATION. */
public class InvalidOperationV2 extends AbstractOperationV2 {

  /** The constant OPCODE. */
  public static final int OPCODE = 0xFE;

  /** The constant INVALID_RESULT. */
  public static final OperationResult INVALID_RESULT =
      new OperationResult(0, ExceptionalHaltReason.INVALID_OPERATION);

  /**
   * Instantiates a new Invalid operation.
   *
   * @param gasCalculator the gas calculator
   */
  public InvalidOperationV2(final GasCalculator gasCalculator) {
    super(OPCODE, "INVALID", -1, -1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    return INVALID_RESULT;
  }
}
