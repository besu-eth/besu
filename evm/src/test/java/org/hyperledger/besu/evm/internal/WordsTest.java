/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.evm.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.evm.internal.Words.clampedToInt;
import static org.hyperledger.besu.evm.internal.Words.unsignedMin;

import java.util.Arrays;
import java.util.Collection;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@TestInstance(Lifecycle.PER_CLASS)
class WordsTest {

  Collection<Object[]> unsignedMinLongTestVector() {
    return Arrays.asList(
        new Object[][] {
          {10, 10, 10},
          {-10, -10, -10},
          {10, 20, 10},
          {20, 10, 10},
          {10, -10, 10},
          {-10, 10, 10},
          {-20, -10, -20},
          {-10, -20, -20},
        });
  }

  @ParameterizedTest
  @MethodSource("unsignedMinLongTestVector")
  void unsignedMinLongTest(final long a, final long b, final long min) {
    assertThat(unsignedMin(a, b)).isEqualTo(min);
  }

  Collection<Object[]> clampedToLongTestVector() {
    return Arrays.asList(
        new Object[][] {
          {Bytes.fromHexStringLenient("0x0"), 0x0},
          {Bytes.fromHexStringLenient("0x1"), 0x1},
          {Bytes.fromHexString("0x10"), 0x10},
          {Bytes.fromHexStringLenient("0x100"), 0x100},
          {Bytes.fromHexString("0x1000"), 0x1000},
          {Bytes.fromHexStringLenient("0x10000"), 0x10000},
          {Bytes.fromHexString("0x100000"), 0x100000},
          {Bytes.fromHexStringLenient("0x1000000"), 0x1000000},
          {Bytes.fromHexString("0x10000000"), 0x10000000},
          {Bytes.fromHexString("0x20000000"), 0x20000000},
          {Bytes.fromHexString("0x40000000"), 0x40000000},
          {Bytes.fromHexString("0x0040000000"), 0x40000000},
          {Bytes.fromHexString("0x7fffffff"), Integer.MAX_VALUE},
          {Bytes.fromHexString("0x80000000"), Integer.MAX_VALUE},
          {Bytes.fromHexString("0x80000001"), Integer.MAX_VALUE},
          {Bytes.fromHexString("0x1000000000000000"), Integer.MAX_VALUE},
          {Bytes.fromHexString("0x10000000000000000000000000000000"), Integer.MAX_VALUE},
          {
            Bytes.fromHexString(
                "0x1000000000000000000000000000000000000000000000000000000000000000"),
            Integer.MAX_VALUE
          },
          {
            Bytes.fromHexString(
                "0x0000000000000000000000000000000000000000000000000000000040000000"),
            0x40000000
          },
          {
            Bytes.fromHexString(
                "0x000000000000000000000000000000000000000000000000000000007fffffff"),
            Integer.MAX_VALUE
          },
          {
            Bytes.fromHexString(
                "0x0000000000000000000000000000000000000000000000000000000080000000"),
            Integer.MAX_VALUE
          },
          {
            Bytes.fromHexString(
                "0x0000000000000000000000000000000000000000000000000000000080000001"),
            Integer.MAX_VALUE
          },
        });
  }

  @ParameterizedTest
  @MethodSource("clampedToLongTestVector")
  void clampedToIntTest(final Bytes theBytes, final int theExpectedInt) {
    assertThat(clampedToInt(theBytes)).isEqualTo(theExpectedInt);
  }
}
