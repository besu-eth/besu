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
package org.hyperledger.besu.ethereum.blockcreation;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Stopwatch;

public class BlockCreationTiming {
  private final Map<String, Duration> timing = new LinkedHashMap<>();
  // Steps whose value is a standalone duration (not a cumulative stopwatch reading),
  // and so should be printed as-is without computing a delta from the previous entry.
  private final Set<String> standaloneValueSteps = new HashSet<>();
  private final Stopwatch stopwatch;
  private final Instant startedAt = Instant.now();
  public static final BlockCreationTiming EMPTY = createEmpty();

  public BlockCreationTiming() {
    this.stopwatch = Stopwatch.createStarted();
  }

  private static BlockCreationTiming createEmpty() {
    BlockCreationTiming empty = new BlockCreationTiming();
    empty.timing.put("empty-block-created", Duration.ZERO);
    empty.stopwatch.stop();
    return empty;
  }

  public void register(final String step) {
    timing.put(step, stopwatch.elapsed());
  }

  public void registerValue(final String step, final Duration value) {
    timing.put(step, value);
    standaloneValueSteps.add(step);
  }

  public void registerAll(final BlockCreationTiming subTiming) {
    final var offset = Duration.between(startedAt, subTiming.startedAt);
    for (final var entry : subTiming.timing.entrySet()) {
      timing.put(entry.getKey(), offset.plus(entry.getValue()));
    }
  }

  public Duration end(final String step) {
    if (stopwatch.isRunning()) {
      stopwatch.stop();
    }
    final var elapsed = stopwatch.elapsed();
    timing.put(step, elapsed);
    return elapsed;
  }

  public Instant startedAt() {
    return startedAt;
  }

  @Override
  public String toString() {
    final var sb = new StringBuilder("started at " + startedAt + ", ");

    var prevDuration = Duration.ZERO;
    for (final var entry : timing.entrySet()) {
      final boolean isStandalone = standaloneValueSteps.contains(entry.getKey());
      final Duration displayed =
          isStandalone ? entry.getValue() : entry.getValue().minus(prevDuration);
      sb.append(entry.getKey()).append("=").append(displayed.toMillis()).append("ms, ");
      if (!isStandalone) {
        prevDuration = entry.getValue();
      }
    }
    sb.delete(sb.length() - 2, sb.length());

    return sb.toString();
  }
}
