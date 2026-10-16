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

import org.hyperledger.besu.ethereum.vm.operations.PushOperationBenchmarkBase;
import org.hyperledger.besu.evm.frame.MessageFrame;

/** Runs the PUSH benchmarks on a frame set up for EVM v2. */
public abstract class PushOperationBenchmarkBaseV2 extends PushOperationBenchmarkBase {

  @Override
  protected MessageFrame createFrame() {
    return BenchmarkHelperV2.createMessageCallFrame();
  }

  @Override
  protected void popResult(final MessageFrame frame) {
    frame.setTopV2(frame.stackTopV2() - 1);
  }
}
