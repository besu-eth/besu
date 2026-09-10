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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.calltrace;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.CallTracerResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("FlatCallTracer")
class FlatCallTracerTest {

  private static final Address PRECOMPILE_1 =
      Address.fromHexString("0x0000000000000000000000000000000000000001");

  @Test
  @DisplayName("DELEGATECALL child to precompile is kept when includePrecompiles is false")
  void delegateCallToPrecompileIsKeptWhenPrecompilesNotIncluded() {
    final CallTracerResult delegateCall =
        CallTracerResult.builder()
            .type("DELEGATECALL")
            .from("0x1000000000000000000000000000000000000001")
            .to(PRECOMPILE_1.getBytes().toHexString())
            .gas(10000L)
            .gasUsed(500L)
            .value("0x0")
            .input("0x1234")
            .output("0x5678")
            .build();

    final CallTracerResult regularCall =
        CallTracerResult.builder()
            .type("CALL")
            .from("0x1000000000000000000000000000000000000001")
            .to(PRECOMPILE_1.getBytes().toHexString())
            .gas(10000L)
            .gasUsed(500L)
            .value("0x0")
            .input("0x1234")
            .output("0x5678")
            .build();

    final CallTracerResult root =
        CallTracerResult.builder()
            .type("CALL")
            .from("0x0000000000000000000000000000000000000000")
            .to("0x1000000000000000000000000000000000000001")
            .gas(50000L)
            .gasUsed(20000L)
            .value("0x0")
            .input("0x")
            .output("0x")
            .addCall(delegateCall)
            .addCall(regularCall)
            .build();

    final FlatCallTracer.Context ctx =
        new FlatCallTracer.Context("0xabc", 1L, "0xdef", 0, false, false, Set.of(PRECOMPILE_1));

    final List<FlatCallTracerResult> out = new ArrayList<>();
    FlatCallTracer.flatten(root, List.of(), ctx, out);

    // regularCall to precompile is pruned, delegateCall is kept
    assertThat(out).hasSize(2);
    assertThat(out.get(0).subtraces()).isEqualTo(1);
    assertThat(out.get(1).traceAddress()).containsExactly(0);
    assertThat(out.get(1).action().to()).isEqualTo(PRECOMPILE_1.getBytes().toHexString());
  }

  @Test
  @DisplayName("convertParityErrors maps known error strings and preserves unmapped ones")
  void convertParityErrorsMappings() {
    assertThat(FlatCallTracer.toParityError("insufficient balance for transfer"))
        .isEqualTo("insufficient balance for transfer");
    assertThat(FlatCallTracer.toParityError("Precompile error")).isEqualTo("Built-in failed");
    assertThat(FlatCallTracer.toParityError("precompile failed")).isEqualTo("Built-in failed");
    assertThat(FlatCallTracer.toParityError("execution reverted")).isEqualTo("Reverted");
    assertThat(FlatCallTracer.toParityError("Out of gas")).isEqualTo("Out of gas");
    assertThat(FlatCallTracer.toParityError("Code is too large")).isEqualTo("Out of gas");
    assertThat(FlatCallTracer.toParityError("Bad jump destination"))
        .isEqualTo("Bad jump destination");
    assertThat(FlatCallTracer.toParityError("Bad instruction")).isEqualTo("Bad instruction");
    assertThat(FlatCallTracer.toParityError("Invalid opcode: 0xfe")).isEqualTo("Bad instruction");
    assertThat(FlatCallTracer.toParityError("invalid input length for ecrecover"))
        .isEqualTo("Built-in failed");
  }

  @Test
  @DisplayName("STATICCALL child with null value produces 0x0 action value")
  void staticCallChildWithNullValueProducesZeroHexValue() {
    final CallTracerResult staticCall =
        CallTracerResult.builder()
            .type("STATICCALL")
            .from("0x1000000000000000000000000000000000000001")
            .to("0x2000000000000000000000000000000000000002")
            .gas(10000L)
            .gasUsed(500L)
            .value(null)
            .input("0x")
            .output("0x")
            .build();

    final CallTracerResult root =
        CallTracerResult.builder()
            .type("CALL")
            .from("0x0000000000000000000000000000000000000000")
            .to("0x1000000000000000000000000000000000000001")
            .gas(50000L)
            .gasUsed(20000L)
            .value("0x0")
            .input("0x")
            .output("0x")
            .addCall(staticCall)
            .build();

    final FlatCallTracer.Context ctx =
        new FlatCallTracer.Context("0xabc", 1L, "0xdef", 0, false, false, Set.of());

    final List<FlatCallTracerResult> out = new ArrayList<>();
    FlatCallTracer.flatten(root, List.of(), ctx, out);

    assertThat(out).hasSize(2);
    assertThat(out.get(1).action().value()).isEqualTo("0x0");
  }
}
