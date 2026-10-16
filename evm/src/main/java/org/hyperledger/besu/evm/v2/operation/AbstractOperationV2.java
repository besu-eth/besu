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
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.AbstractOperation;
import org.hyperledger.besu.evm.operation.Operation;

import org.apache.tuweni.bytes.Bytes;

/**
 * The base of the V2 {@link Operation} implementations that have no v1 counterpart to extend. It
 * shares the account and storage accessors of {@link AbstractOperation}, which record accesses in
 * the block access list.
 */
public abstract class AbstractOperationV2 extends AbstractOperation {
  static final Bytes BYTES_ONE = Bytes.of(1);

  /** Shared underflow response (zero gas cost). */
  public static final Operation.OperationResult UNDERFLOW_RESPONSE =
      new Operation.OperationResult(0L, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);

  /** Shared overflow response (zero gas cost). */
  public static final Operation.OperationResult OVERFLOW_RESPONSE =
      new Operation.OperationResult(0L, ExceptionalHaltReason.TOO_MANY_STACK_ITEMS);

  /**
   * Instantiates a new Abstract operation.
   *
   * @param opcode the opcode
   * @param name the name
   * @param stackItemsConsumed the stack items consumed
   * @param stackItemsProduced the stack items produced
   * @param gasCalculator the gas calculator
   */
  protected AbstractOperationV2(
      final int opcode,
      final String name,
      final int stackItemsConsumed,
      final int stackItemsProduced,
      final GasCalculator gasCalculator) {
    super(opcode, name, stackItemsConsumed, stackItemsProduced, gasCalculator);
  }
}
