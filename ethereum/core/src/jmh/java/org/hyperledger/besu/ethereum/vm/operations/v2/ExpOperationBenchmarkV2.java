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
package org.hyperledger.besu.ethereum.vm.operations.v2;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.PragueGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.v2.ExpOperationV2;

public class ExpOperationBenchmarkV2 extends BinaryOperationBenchmarkV2 {

  private static final GasCalculator PRAGUE_GAS_CALCULATOR = new PragueGasCalculator();

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return ExpOperationV2.staticOperation(frame, frame.stackDataV2(), PRAGUE_GAS_CALCULATOR);
  }
}
