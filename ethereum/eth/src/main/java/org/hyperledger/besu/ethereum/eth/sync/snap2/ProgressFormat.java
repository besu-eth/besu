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

/** Formats the figures of the SNAP2 logs: counts, block numbers and durations. */
public final class ProgressFormat {

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
}
