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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.mainnet.TopFrameHaltTestFixture;
import org.hyperledger.besu.ethereum.mainnet.TopFrameHaltTestFixture.Halt;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;

import java.util.Map;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class FourByteTracerTopFrameHaltTest {

  private static final TopFrameHaltTestFixture FIXTURE = new TopFrameHaltTestFixture();

  @ParameterizedTest
  @EnumSource(Halt.class)
  void countsSelectorOfMessageCallThatRunsOutOfGasBeforeExecutionStarts(final Halt halt) {
    final FourByteTracer tracer =
        new FourByteTracer(FIXTURE.spec().getPrecompileContractRegistry());

    final TransactionProcessingResult result = FIXTURE.process(FIXTURE.transaction(halt), tracer);

    assertThat(result.isInvalid()).as(result.getValidationResult().toString()).isFalse();
    assertThat(result.getExceptionalHaltReason()).contains(ExceptionalHaltReason.INSUFFICIENT_GAS);
    // Like geth, which enters the halted top frame before failing it.
    final Map<String, Integer> expected =
        halt.frameType() == MessageFrame.Type.CONTRACT_CREATION
            ? Map.of()
            : Map.of("0xabcdef12-32", 1);
    assertThat(tracer.buildResult().selectorCounts()).isEqualTo(expected);
  }
}
