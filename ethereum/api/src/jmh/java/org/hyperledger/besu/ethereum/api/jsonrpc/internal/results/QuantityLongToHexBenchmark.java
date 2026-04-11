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

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.units.bigints.UInt256;
import org.apache.tuweni.units.bigints.UInt256Value;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(value = TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class QuantityLongToHexBenchmark {
  private static final String HEX_PREFIX = "0x";
  public static final String HEX_ZERO = "0x0";
  private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

  @Param({"0", "255", "65535", "1000000", "9007199254740991", "9223372036854775807"})
  public long value;

  @Benchmark
  public void current(final Blackhole blackhole) {
    blackhole.consume(uint256ToHex(UInt256.fromHexString(Long.toHexString(value))));
  }

  @Benchmark
  public void simpleHexString(final Blackhole blackhole) {
    blackhole.consume(simpleHexString(value));
  }

  @Benchmark
  public void directCharArray(final Blackhole blackhole) {
    blackhole.consume(directCharArray(value));
  }

  @Benchmark
  public void unrolled(final Blackhole blackhole) {
    blackhole.consume(unrolled(value));
  }

  private static String simpleHexString(final long value) {
    return HEX_PREFIX + Long.toHexString(value);
  }

  private static String unrolled(final long value) {
    if (value == 0L) {
      return HEX_ZERO;
    }
    final int leadingZeroNibbles = Long.numberOfLeadingZeros(value) >>> 2;
    final char[] buf = new char[18];
    buf[2] = HEX_DIGITS[(int) ((value >>> 60) & 0xF)];
    buf[3] = HEX_DIGITS[(int) ((value >>> 56) & 0xF)];
    buf[4] = HEX_DIGITS[(int) ((value >>> 52) & 0xF)];
    buf[5] = HEX_DIGITS[(int) ((value >>> 48) & 0xF)];
    buf[6] = HEX_DIGITS[(int) ((value >>> 44) & 0xF)];
    buf[7] = HEX_DIGITS[(int) ((value >>> 40) & 0xF)];
    buf[8] = HEX_DIGITS[(int) ((value >>> 36) & 0xF)];
    buf[9] = HEX_DIGITS[(int) ((value >>> 32) & 0xF)];
    buf[10] = HEX_DIGITS[(int) ((value >>> 28) & 0xF)];
    buf[11] = HEX_DIGITS[(int) ((value >>> 24) & 0xF)];
    buf[12] = HEX_DIGITS[(int) ((value >>> 20) & 0xF)];
    buf[13] = HEX_DIGITS[(int) ((value >>> 16) & 0xF)];
    buf[14] = HEX_DIGITS[(int) ((value >>> 12) & 0xF)];
    buf[15] = HEX_DIGITS[(int) ((value >>> 8) & 0xF)];
    buf[16] = HEX_DIGITS[(int) ((value >>> 4) & 0xF)];
    buf[17] = HEX_DIGITS[(int) (value & 0xF)];
    buf[leadingZeroNibbles] = '0';
    buf[leadingZeroNibbles + 1] = 'x';
    return new String(buf, leadingZeroNibbles, 18 - leadingZeroNibbles);
  }

  private static String directCharArray(final long value) {
    if (value == 0L) {
      return HEX_ZERO;
    }
    int nibbles = 16;
    while (nibbles > 1 && ((value >>> ((nibbles - 1) * 4)) & 0xFL) == 0L) {
      nibbles--;
    }
    final char[] buf = new char[2 + nibbles];
    buf[0] = '0';
    buf[1] = 'x';
    for (int i = 0; i < nibbles; i++) {
      buf[2 + i] = HEX_DIGITS[(int) ((value >>> ((nibbles - 1 - i) * 4)) & 0xFL)];
    }
    return new String(buf);
  }

  private static String uint256ToHex(final UInt256Value<?> value) {
    return value == null ? null : formatMinimalValue(value.toMinimalBytes().toShortHexString());
  }

  private static String formatMinimalValue(final String hexValue) {
    final String prefixedHexString = prefixHexNotation(hexValue);
    return Objects.equals(prefixedHexString, HEX_PREFIX) ? HEX_ZERO : prefixedHexString;
  }

  private static String prefixHexNotation(final String hexValue) {
    return hexValue.startsWith(HEX_PREFIX) ? hexValue : HEX_PREFIX + hexValue;
  }
}
