/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.evm.operation;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** The Chain id operation. */
public class ChainIdOperation extends AbstractFixedCostOperation {

  /** The CHAINID Opcode number */
  public static final int OPCODE = 0x46;

  private final Bytes32 chainId;

  /**
   * Instantiates a new Chain id operation.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   */
  public ChainIdOperation(final GasCalculator gasCalculator, final Bytes32 chainId) {
    super(OPCODE, "CHAINID", 0, 1, gasCalculator, gasCalculator.getBaseTierGasCost());
    this.chainId = chainId;
  }

  /**
   * Returns the chain ID this operation uses
   *
   * @return then chainID;
   */
  public Bytes getChainId() {
    return chainId;
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(final MessageFrame frame) {
    frame.pushStackItem(chainId);

    return successResponse;
  }
}
