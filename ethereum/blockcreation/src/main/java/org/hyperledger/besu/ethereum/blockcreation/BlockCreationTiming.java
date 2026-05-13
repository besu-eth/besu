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
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.base.Stopwatch;

public class BlockCreationTiming {
  // A standalone entry holds a duration that should be printed as-is, instead of as a delta
  // from the previous step in the timing chain.
  private record TimingEntry(Duration duration, boolean standalone) {}

  private final Map<String, TimingEntry> timing = new LinkedHashMap<>();
  private final Stopwatch stopwatch;
  private final Instant startedAt = Instant.now();
  public static final BlockCreationTiming EMPTY = createEmpty();

  public BlockCreationTiming() {
    this.stopwatch = Stopwatch.createStarted();
  }

  private static BlockCreationTiming createEmpty() {
    BlockCreationTiming empty = new BlockCreationTiming();
    empty.timing.put("empty-block-created", new TimingEntry(Duration.ZERO, false));
    empty.stopwatch.stop();
    return empty;
  }

  public void register(final String step) {
    timing.put(step, new TimingEntry(stopwatch.elapsed(), false));
  }

  public void registerValue(final String step, final Duration value) {
    timing.put(step, new TimingEntry(value, true));
  }

  public void registerAll(final BlockCreationTiming subTiming) {
    final var offset = Duration.between(startedAt, subTiming.startedAt);
    for (final var entry : subTiming.timing.entrySet()) {
      final TimingEntry te = entry.getValue();
      final Duration adjusted = te.standalone() ? te.duration() : offset.plus(te.duration());
      timing.put(entry.getKey(), new TimingEntry(adjusted, te.standalone()));
    }
  }

  public Duration end(final String step) {
    if (stopwatch.isRunning()) {
      stopwatch.stop();
    }
    final var elapsed = stopwatch.elapsed();
    timing.put(step, new TimingEntry(elapsed, false));
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
      final TimingEntry te = entry.getValue();
      final Duration displayed =
          te.standalone() ? te.duration() : te.duration().minus(prevDuration);
      sb.append(entry.getKey()).append("=").append(displayed.toMillis()).append("ms, ");
      if (!te.standalone()) {
        prevDuration = te.duration();
      }
    }
    sb.delete(sb.length() - 2, sb.length());

    return sb.toString();
  }
}
