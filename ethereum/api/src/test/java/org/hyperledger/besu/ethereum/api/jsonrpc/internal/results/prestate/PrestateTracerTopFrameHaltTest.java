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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.prestate;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.debug.TracerType;
import org.hyperledger.besu.ethereum.mainnet.TopFrameHaltTestFixture;
import org.hyperledger.besu.ethereum.mainnet.TopFrameHaltTestFixture.Halt;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;

import java.util.Map;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class PrestateTracerTopFrameHaltTest {

  private static final TopFrameHaltTestFixture FIXTURE = new TopFrameHaltTestFixture();

  @ParameterizedTest
  @EnumSource(Halt.class)
  void includesCoinbaseWhenTopFrameRunsOutOfGasBeforeExecutionStarts(final Halt halt) {
    // includeEmpty, because the coinbase does not exist in the genesis state.
    final PrestateTracer tracer =
        new PrestateTracer(
            new TraceOptions(TracerType.PRESTATE_TRACER, null, Map.of("includeEmpty", true)),
            FIXTURE.spec());

    final TransactionProcessingResult result = FIXTURE.process(FIXTURE.transaction(halt), tracer);

    assertThat(result.isInvalid()).as(result.getValidationResult().toString()).isFalse();
    assertThat(result.getExceptionalHaltReason()).contains(ExceptionalHaltReason.INSUFFICIENT_GAS);
    final PrestateTracerResult.Prestate prestate =
        (PrestateTracerResult.Prestate) tracer.buildResult();
    assertThat(prestate.accounts()).containsKey(FIXTURE.coinbase().toHexString());
  }
}
