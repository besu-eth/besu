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

import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.tracing.TraceFrame;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class StackExtractorTest {

  private static final Bytes MEMORY_WORD = Bytes32.fromHexString("0x" + "ab".repeat(32));
  private static final Bytes[] MEMORY = new Bytes[] {MEMORY_WORD};

  @Test
  void extractCallInputPrefersTheCapturedCallInputData() {
    final Bytes captured = Bytes.fromHexString("0xdeadbeef");
    final TraceFrame frame =
        traceFrameBuilder()
            .setCallInputData(Optional.of(captured))
            // A stack/memory pair decoding to something else, to prove it is not consulted.
            .setStack(Optional.of(callStack(0, 32)))
            .setMemory(Optional.of(MEMORY))
            .build();

    assertThat(StackExtractor.extractCallInputFromMemory(frame, "CALL")).isEqualTo(captured);
  }

  @Test
  void extractCallInputFallsBackToMemoryWhenNoCallInputDataWasCaptured() {
    final TraceFrame frame =
        traceFrameBuilder()
            .setStack(Optional.of(callStack(4, 8)))
            .setMemory(Optional.of(MEMORY))
            .build();

    assertThat(StackExtractor.extractCallInputFromMemory(frame, "CALL"))
        .isEqualTo(MEMORY_WORD.slice(4, 8));
  }

  @Test
  void extractCallInputFallbackUsesTheShallowerSlotsForDelegateAndStaticCall() {
    // Bottom to top: outSize, outOffset, inSize, inOffset, to, gas.
    final Bytes[] stack = {
      Bytes.EMPTY,
      Bytes.EMPTY,
      Bytes.ofUnsignedLong(8),
      Bytes.ofUnsignedLong(4),
      Bytes.EMPTY,
      Bytes.EMPTY,
    };
    final TraceFrame frame =
        traceFrameBuilder().setStack(Optional.of(stack)).setMemory(Optional.of(MEMORY)).build();

    assertThat(StackExtractor.extractCallInputFromMemory(frame, "DELEGATECALL"))
        .isEqualTo(MEMORY_WORD.slice(4, 8));
    assertThat(StackExtractor.extractCallInputFromMemory(frame, "STATICCALL"))
        .isEqualTo(MEMORY_WORD.slice(4, 8));
  }

  @Test
  void extractCreateInitCodePrefersDeployedCodeWhenTheCreateEntered() {
    final Bytes deployed = Bytes.fromHexString("0x6001600155");
    final TraceFrame frame =
        traceFrameBuilder()
            .setMaybeCode(Optional.of(new Code(deployed)))
            .setCallInputData(Optional.of(Bytes.fromHexString("0xdeadbeef")))
            .build();

    assertThat(StackExtractor.extractCreateInitCode(frame, true)).isEqualTo(deployed);
  }

  @Test
  void extractCreateInitCodeUsesCapturedInitCodeWhenTheCreateDidNotEnter() {
    // getMaybeCode() holds the caller's code when no callee frame was spawned, not the initcode.
    final Bytes initCode = Bytes.fromHexString("0x60806040");
    final TraceFrame frame =
        traceFrameBuilder()
            .setMaybeCode(Optional.of(new Code(Bytes.fromHexString("0xfefefe"))))
            .setCallInputData(Optional.of(initCode))
            .build();

    assertThat(StackExtractor.extractCreateInitCode(frame, false)).isEqualTo(initCode);
  }

  @Test
  void extractCreateInitCodeReadsTheSameStackSlotsForCreateAndCreate2() {
    // Bottom to top: size, offset, value.
    final Bytes[] createStack = {
      Bytes.ofUnsignedLong(6), Bytes.ofUnsignedLong(2), Bytes.EMPTY,
    };
    // CREATE2 adds a salt below the same three.
    final Bytes[] create2Stack = {
      Bytes.EMPTY, Bytes.ofUnsignedLong(6), Bytes.ofUnsignedLong(2), Bytes.EMPTY,
    };

    final Bytes expected = MEMORY_WORD.slice(2, 6);
    assertThat(
            StackExtractor.extractCreateInitCode(
                traceFrameBuilder()
                    .setStack(Optional.of(createStack))
                    .setMemory(Optional.of(MEMORY))
                    .build(),
                false))
        .isEqualTo(expected);
    assertThat(
            StackExtractor.extractCreateInitCode(
                traceFrameBuilder()
                    .setStack(Optional.of(create2Stack))
                    .setMemory(Optional.of(MEMORY))
                    .build(),
                false))
        .isEqualTo(expected);
  }

  /** CALL stack, bottom to top: outSize, outOffset, inSize, inOffset, value, to, gas. */
  private static Bytes[] callStack(final long inOffset, final long inSize) {
    return new Bytes[] {
      Bytes.EMPTY,
      Bytes.EMPTY,
      Bytes.ofUnsignedLong(inSize),
      Bytes.ofUnsignedLong(inOffset),
      Bytes.EMPTY,
      Bytes.EMPTY,
      Bytes.EMPTY,
    };
  }

  private static TraceFrame.Builder traceFrameBuilder() {
    return TraceFrame.builder().setDepth(1).setInputData(Bytes.EMPTY);
  }
}
