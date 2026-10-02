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
import org.hyperledger.besu.evm.v2.operation.PushOperationV2;

import org.openjdk.jmh.annotations.Param;

public class PushOperationMultiLimbV2 extends PushOperationBenchmarkBaseV2 {
  @Param({"9", "14", "20", "32", "RANDOM"})
  protected String pushSize;

  @Param protected Position pc;

  @Param({"SMALL", "BIG"})
  private String codeSize;

  @Override
  protected Position getPc() {
    return pc;
  }

  @Override
  protected String getPushSize() {
    return pushSize;
  }

  @Override
  protected String getCodeSize() {
    return codeSize;
  }

  @Override
  protected int[] getPushRandomization() {
    return new int[] {9, 33};
  }

  @Override
  protected Operation.OperationResult invoke(
      final MessageFrame frame, final byte[] code, final int pc, final int pushSize) {
    return PushOperationV2.MultiLimb.staticOperation(frame, code, pc, pushSize);
  }
}
