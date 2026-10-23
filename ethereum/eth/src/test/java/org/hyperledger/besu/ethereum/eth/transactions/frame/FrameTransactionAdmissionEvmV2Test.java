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
package org.hyperledger.besu.ethereum.eth.transactions.frame;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import org.junit.jupiter.api.Test;

/**
 * Every admission case on EVM v2 ({@code --Xevm-v2}), whose frames have no v1 operand stack: the
 * validation trace rules must read operands through {@code TracedStack}.
 */
class FrameTransactionAdmissionEvmV2Test extends FrameTransactionAdmissionTest {

  private static final EvmConfiguration EVM_V2 =
      new EvmConfiguration(32_000L, EvmConfiguration.WorldUpdaterMode.STACKED, true, true);

  @Override
  EvmConfiguration evmConfiguration() {
    return EVM_V2;
  }

  @Test
  void simulatesOnEvmV2() {
    assertThat(
            protocolSchedule
                .getForNextBlockHeader(head, head.getTimestamp() + 1)
                .getTransactionProcessor()
                .getMessageProcessor(MessageFrame.Type.MESSAGE_CALL)
                .runsOnEvmV2())
        .isTrue();
  }
}
