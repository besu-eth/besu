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

import static org.hyperledger.besu.crypto.Hash.keccak256;
import static org.hyperledger.besu.evm.internal.Words.clampedAdd;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.clampedToInt;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.readBytes32At;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** The Create2 operation. */
public class Create2OperationV2 extends AbstractCreateOperationV2 {

  private static final Bytes PREFIX = Bytes.fromHexString("0xFF");

  /**
   * Instantiates a new Create2 operation.
   *
   * @param gasCalculator the gas calculator
   */
  public Create2OperationV2(final GasCalculator gasCalculator) {
    super(0xF5, "CREATE2", 4, 1, gasCalculator);
  }

  @Override
  protected long cost(final MessageFrame frame) {
    final int inputOffset = clampedToInt(frame.stackDataV2(), frame.stackTopV2(), 1);
    final int inputSize = clampedToInt(frame.stackDataV2(), frame.stackTopV2(), 2);
    return clampedAdd(
        clampedAdd(
            gasCalculator().txCreateCost(),
            gasCalculator().memoryExpansionGasCost(frame, inputOffset, inputSize)),
        clampedAdd(
            gasCalculator().createKeccakCost(inputSize), gasCalculator().initcodeCost(inputSize)));
  }

  @Override
  protected Address generateTargetContractAddress(final MessageFrame frame, final Code initcode) {
    final Address sender = frame.getRecipientAddress();
    final Bytes32 salt = readBytes32At(frame.stackDataV2(), frame.stackTopV2(), 3);
    final Bytes32 hash =
        keccak256(
            Bytes.concatenate(PREFIX, sender.getBytes(), salt, initcode.getCodeHash().getBytes()));
    return Address.extract(hash);
  }
}
