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
import static org.hyperledger.besu.evm.v2.operation.StackUtil.clampedToInt;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/** The Create operation. */
public class CreateOperationV2 extends AbstractCreateOperationV2 {

  /**
   * Instantiates a new Create operation.
   *
   * @param gasCalculator the gas calculator
   */
  public CreateOperationV2(final GasCalculator gasCalculator) {
    super(0xF0, "CREATE", 3, 1, gasCalculator);
  }

  @Override
  protected long cost(final MessageFrame frame) {
    final int inputOffset = clampedToInt(frame.stackDataV2(), frame.stackTopV2(), 1);
    final int inputSize = clampedToInt(frame.stackDataV2(), frame.stackTopV2(), 2);
    return clampedAdd(
        clampedAdd(
            gasCalculator().txCreateCost(),
            gasCalculator().memoryExpansionGasCost(frame, inputOffset, inputSize)),
        gasCalculator().initcodeCost(inputSize));
  }

  @Override
  protected Address generateTargetContractAddress(final MessageFrame frame, final Code initcode) {
    final Account sender = getAccount(frame.getRecipientAddress(), frame);
    // Decrement nonce by 1 to normalize the effect of transaction execution
    return Address.contractAddress(frame.getRecipientAddress(), sender.getNonce() - 1L);
  }
}
