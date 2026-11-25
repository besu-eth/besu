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
package org.hyperledger.besu.evm.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.evm.v2.ProgramRun.CALLEE;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.v2.ProgramRun.Outcome;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Calls a contract that loads its call data at every offset up to just past its end, with the call
 * data taken from the caller's memory as a call does, and requires v1 and v2 to agree.
 */
class CallDataLoadV1V2DifferentialTest {

  private static final EVM V1 = MainnetEVMs.osaka(BigInteger.ONE, ProgramRun.V1);
  private static final EVM V2 = MainnetEVMs.osaka(BigInteger.ONE, ProgramRun.V2);

  private static final int[] INPUT_SIZES = {0, 1, 4, 7, 8, 9, 31, 32, 33, 36, 68, 100};
  // a word boundary, just past one, the free memory pointer, and the last byte of its word
  private static final int[] INPUT_OFFSETS = {0, 1, 0x80, 0x9f};

  /**
   * For i = 0 to CALLDATASIZE + 1, stores CALLDATALOAD(i) at memory i * 32, then returns all the
   * words.
   */
  private static final Bytes LOADING_CODE =
      Bytes.fromHexString(
          "0x"
              + "6000" // PUSH1 0
              + "5b" // loop: JUMPDEST
              + "8035" // DUP1 CALLDATALOAD
              + "8160051b52" // DUP2 PUSH1 5 SHL MSTORE
              + "600101" // PUSH1 1 ADD
              + "8036600201" // DUP1 CALLDATASIZE PUSH1 2 ADD
              + "11600257" // GT PUSH1 loop JUMPI
              + "60051b6000f3"); // PUSH1 5 SHL PUSH1 0 RETURN

  @ParameterizedTest(name = "{0}, caller memory filled past the call data: {2}")
  @CsvSource({
    "CALL, 0xf1, true",
    "CALL, 0xf1, false",
    "CALLCODE, 0xf2, true",
    "CALLCODE, 0xf2, false",
    "DELEGATECALL, 0xf4, true",
    "DELEGATECALL, 0xf4, false",
    "STATICCALL, 0xfa, true",
    "STATICCALL, 0xfa, false"
  })
  void loadsTheCallDataACallTakesFromMemoryAsV1Does(
      final String name, final int opcode, final boolean fillPastCallData) {
    final List<String> differences = new ArrayList<>();
    for (final int inputSize : INPUT_SIZES) {
      for (final int inputOffset : INPUT_OFFSETS) {
        // without the filler, the caller's memory can end where the call data does
        final int filled = fillPastCallData ? inputOffset + inputSize + 64 : 0;
        final Bytes code = callerCode(opcode, inputOffset, inputSize, filled);
        final Outcome onV1 =
            ProgramRun.run(V1, code, LOADING_CODE, Bytes.EMPTY, 10_000_000L, false, List.of());
        final Outcome onV2 =
            ProgramRun.run(V2, code, LOADING_CODE, Bytes.EMPTY, 10_000_000L, false, List.of());
        // the callee ran to its RETURN, so the comparison covers its loads
        assertThat(Bytes.fromHexString(onV1.returnData()).size()).isEqualTo((inputSize + 2) * 32);
        if (!onV1.equals(onV2)) {
          differences.add(
              String.format(
                  "call data of %d bytes at %d:%n  v1 %s%n  v2 %s",
                  inputSize, inputOffset, onV1, onV2));
        }
      }
    }
    assertThat(differences).isEmpty();
  }

  /**
   * Fills memory up to {@code filled} with non-zero bytes, then calls CALLEE with {@code inputSize}
   * bytes of memory from {@code inputOffset} as its call data.
   */
  private static Bytes callerCode(
      final int opcode, final int inputOffset, final int inputSize, final int filled) {
    final ByteArrayOutputStream code = new ByteArrayOutputStream();
    for (int word = 0; word * 32 < filled; word++) {
      code.write(0x7f); // PUSH32
      for (int i = 0; i < 32; i++) {
        code.write(0x80 | (word * 32 + i) * 31 + 7);
      }
      push2(code, word * 32);
      code.write(0x52); // MSTORE
    }
    push2(code, 0); // output size
    push2(code, 0); // output offset
    push2(code, inputSize);
    push2(code, inputOffset);
    if (opcode == 0xf1 || opcode == 0xf2) {
      push2(code, 0); // value
    }
    code.write(0x73); // PUSH20
    code.writeBytes(CALLEE.getBytes().toArrayUnsafe());
    code.write(0x5a); // GAS
    code.write(opcode);
    code.write(0x00); // STOP
    return Bytes.wrap(code.toByteArray());
  }

  private static void push2(final ByteArrayOutputStream code, final int value) {
    code.write(0x61);
    code.write(value >>> 8);
    code.write(value);
  }
}
