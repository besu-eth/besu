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
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.operation.DupNOperationV2;

/** JMH benchmark for the v2 DUPN operation (EIP-8024). */
public class DupNOperationBenchmarkV2 extends ImmediateByteOperationBenchmarkV2 {

  @Override
  protected int getOpcode() {
    return DupNOperationV2.OPCODE;
  }

  @Override
  protected byte getImmediate() {
    // Immediate 0x80 decodes to n=17 (duplicate 17th stack item), as in the v1 benchmark
    return (byte) 0x80;
  }

  @Override
  protected Operation.OperationResult invoke(
      final MessageFrame frame, final byte[] code, final int pc) {
    return DupNOperationV2.staticOperation(frame, code, pc);
  }

  @Override
  protected int getStackDelta() {
    return 1;
  }
}
