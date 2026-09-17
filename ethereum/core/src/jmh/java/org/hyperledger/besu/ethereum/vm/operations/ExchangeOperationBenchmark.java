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
package org.hyperledger.besu.ethereum.vm.operations;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.ExchangeOperation;
import org.hyperledger.besu.evm.operation.Operation;

import org.openjdk.jmh.infra.BenchmarkParams;

/** JMH benchmark for the EXCHANGE operation (EIP-8024). */
public class ExchangeOperationBenchmark extends ImmediateByteOperationBenchmark
    implements GasCostBenchmark {

  @Override
  protected int getOpcode() {
    return ExchangeOperation.OPCODE;
  }

  @Override
  protected byte getImmediate() {
    // Immediate 0x00 decodes to n=1, m=1 (swap 2nd with 2nd - effectively a no-op for testing)
    // Using 0x01 would give n=1, m=2 (swap 2nd with 3rd stack item)
    return 0x01;
  }

  @Override
  protected Operation.OperationResult invoke(
      final MessageFrame frame, final byte[] code, final int pc) {
    return ExchangeOperation.staticOperation(frame, code, pc);
  }

  @Override
  protected int getStackDelta() {
    // EXCHANGE does not change stack size
    return 0;
  }

  @Override
  public long getGasCost(final BenchmarkParams params, final GasCalculator calc) {
    return new ExchangeOperation(calc).getGasCost();
  }
}
