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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import java.time.Duration;
import java.util.Locale;
import java.util.Optional;

/** Formats the figures of the SNAP2 logs: counts, block numbers, shares, rates and durations. */
public final class ProgressFormat {

  private static final Duration LONGEST_ETA = Duration.ofDays(365);

  private ProgressFormat() {}

  /** Returns a count with thousands separators, as in 11,901,107. */
  public static String count(final long value) {
    return String.format(Locale.ROOT, "%,d", value);
  }

  /** Returns a block number, as in #11,901,107. */
  public static String block(final long number) {
    return "#" + count(number);
  }

  /** Returns a compact count, as in 2,140, 82.9K, 1.31M, 82.9M or 1.20B. */
  public static String compact(final long value) {
    final long magnitude = Math.abs(value);
    if (magnitude < 10_000) {
      return count(value);
    }
    if (magnitude < roundsUpAt(1_000_000, 100)) {
      return String.format(Locale.ROOT, "%.1fK", value / 1e3);
    }
    if (magnitude < roundsUpAt(10_000_000, 10_000)) {
      return String.format(Locale.ROOT, "%.2fM", value / 1e6);
    }
    if (magnitude < roundsUpAt(1_000_000_000, 100_000)) {
      return String.format(Locale.ROOT, "%.1fM", value / 1e6);
    }
    return String.format(Locale.ROOT, "%.2fB", value / 1e9);
  }

  // the first value that rounds up to the next unit, half a shown step below it
  private static long roundsUpAt(final long nextUnit, final long shownStep) {
    return nextUnit - shownStep / 2;
  }

  /** Returns a duration in two units at most, as in 45s, 3m 10s, 2h 31m or 1d 2h. */
  public static String duration(final Duration duration) {
    final long seconds = Math.max(0, duration.toSeconds());
    final long days = seconds / 86_400;
    final long hours = seconds % 86_400 / 3_600;
    final long minutes = seconds % 3_600 / 60;
    final long rest = seconds % 60;
    if (days > 0) {
      return twoUnits(days, "d", hours, "h");
    }
    if (hours > 0) {
      return twoUnits(hours, "h", minutes, "m");
    }
    if (minutes > 0) {
      return twoUnits(minutes, "m", rest, "s");
    }
    return rest + "s";
  }

  // the second unit only when it is not zero
  private static String twoUnits(
      final long first, final String firstUnit, final long second, final String secondUnit) {
    return second > 0 ? first + firstUnit + " " + second + secondUnit : first + firstUnit;
  }

  /** Returns the share of a total as a percentage, as in 37.4%, 0.0% without a total. */
  public static String percent(final long done, final long total) {
    final double share = total > 0 ? Math.clamp((double) done / total, 0, 1) : 0;
    return String.format(Locale.ROOT, "%.1f%%", 100 * share);
  }

  /** Returns a rate in mebibytes per second, as in 11.8 MiB/s. */
  public static String mibPerSecond(final double bytesPerSecond) {
    return String.format(Locale.ROOT, "%.1f MiB/s", Math.max(0, bytesPerSecond) / (1024 * 1024));
  }

  /**
   * Returns the time left to do what remains at the average rate since the start, as in ~45m or ~2h
   * 31m, or empty with nothing done, nothing left, no time elapsed, or beyond a year.
   */
  public static Optional<String> eta(
      final long done, final long remaining, final Duration elapsed) {
    if (done <= 0 || remaining <= 0 || elapsed.toMillis() <= 0) {
      return Optional.empty();
    }
    final double elapsedSeconds = elapsed.toMillis() / 1000.0;
    final double seconds = Math.ceil(remaining * elapsedSeconds / done);
    if (seconds > LONGEST_ETA.toSeconds()) {
      return Optional.empty();
    }
    // a minute or more reads without its seconds
    final Duration rounded =
        seconds >= 60
            ? Duration.ofMinutes(Math.round(seconds / 60))
            : Duration.ofSeconds((long) seconds);
    return Optional.of("~" + duration(rounded));
  }
}
