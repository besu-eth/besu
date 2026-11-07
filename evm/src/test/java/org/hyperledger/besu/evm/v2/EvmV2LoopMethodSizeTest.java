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

import org.hyperledger.besu.evm.EVM;

import java.io.InputStream;
import java.lang.classfile.Attributes;
import java.lang.classfile.ClassFile;
import java.lang.classfile.ClassModel;
import java.lang.classfile.MethodModel;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

/** The sizes of the v2 loop's methods that decide how C2 compiles it. */
class EvmV2LoopMethodSizeTest {

  /** HotSpot's HugeMethodLimit. A method above this is never compiled and never OSR-compiled. */
  private static final int HUGE_METHOD_LIMIT = 8000;

  /** HotSpot's FreqInlineSize, the most C2 inlines at a call site it rates hot. */
  private static final int FREQ_INLINE_SIZE = 325;

  /**
   * The loop's own bytecode counts against the 8000 bytes C2 inlines into it, so it is held well
   * under that. Raising this is a decision about the room left for the operations, not a formality.
   */
  private static final int LOOP_BUDGET = 1500;

  @Test
  void theUntracedLoopLeavesRoomToInlineInto() {
    assertThat(methodSizes().get("runToHaltV2Untraced")).isLessThan(LOOP_BUDGET);
  }

  @Test
  void theGeneralPathIsNeverInlinedIntoTheLoopAndStillCompiled() {
    assertThat(methodSizes().get("executeOperationV2"))
        .isGreaterThan(FREQ_INLINE_SIZE)
        .isLessThan(HUGE_METHOD_LIMIT);
  }

  private static Map<String, Integer> methodSizes() {
    final ClassModel model;
    try (InputStream in = EVM.class.getResourceAsStream("EVM.class")) {
      assertThat(in).as("EVM.class must be readable from the classpath").isNotNull();
      model = ClassFile.of().parse(in.readAllBytes());
    } catch (final Exception e) {
      throw new AssertionError("could not read EVM.class", e);
    }
    final Map<String, Integer> sizes = new LinkedHashMap<>();
    for (final MethodModel method : model.methods()) {
      method
          .findAttribute(Attributes.code())
          .ifPresent(
              code -> sizes.merge(method.methodName().stringValue(), code.codeLength(), Math::max));
    }
    return sizes;
  }
}
