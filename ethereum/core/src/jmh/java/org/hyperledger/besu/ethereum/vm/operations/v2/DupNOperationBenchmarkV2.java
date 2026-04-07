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
import org.hyperledger.besu.evm.operation.DupNOperation;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.operation.DupNOperationV2;

/** JMH benchmark for the EVM v2 DUPN operation (EIP-8024). */
public class DupNOperationBenchmarkV2 extends ImmediateByteOperationBenchmarkV2 {

  @Override
  protected int getOpcode() {
    return DupNOperation.OPCODE;
  }

  @Override
  protected byte getImmediate() {
    // Immediate 0x00 decodes to n=17 (duplicate 17th stack item)
    return 0x00;
  }

  @Override
  protected Operation.OperationResult invoke(
      final MessageFrame frame, final byte[] code, final int pc) {
    return DupNOperationV2.staticOperation(frame, frame.stackDataV2(), code, pc);
  }

  @Override
  protected int getStackDelta() {
    // DUPN adds one item to the stack
    return 1;
  }
}
