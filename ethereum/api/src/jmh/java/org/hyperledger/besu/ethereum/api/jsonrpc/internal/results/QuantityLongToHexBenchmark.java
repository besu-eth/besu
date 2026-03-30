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

import static com.google.common.base.Preconditions.checkArgument;

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

  private static String simpleHexString(final long value) {
    checkArgument(value >= 0);
    return HEX_PREFIX + Long.toHexString(value);
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
