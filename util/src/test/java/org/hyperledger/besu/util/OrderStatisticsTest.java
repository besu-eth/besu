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
package org.hyperledger.besu.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.SplittableRandom;

import org.junit.jupiter.api.Test;

class OrderStatisticsTest {

  @Test
  void throwsOnEmpty() {
    assertThrows(
        IllegalArgumentException.class, () -> OrderStatistics.selectKthInPlace(new Integer[0], 0));
  }

  @Test
  void throwsOnOutOfRangeK() {
    assertThrows(
        IllegalArgumentException.class,
        () -> OrderStatistics.selectKthInPlace(new Integer[] {1, 2, 3}, -1));
    assertThrows(
        IllegalArgumentException.class,
        () -> OrderStatistics.selectKthInPlace(new Integer[] {1, 2, 3}, 3));
  }

  @Test
  void selectKthMatchesSortForRandom() {
    final SplittableRandom rnd = new SplittableRandom(1L);
    for (int size : new int[] {1, 2, 3, 10, 31, 32, 33, 128, 1024}) {
      final Integer[] a = new Integer[size];
      for (int i = 0; i < size; i++) {
        a[i] = rnd.nextInt(0, 1_000_000);
      }
      for (int k = 0; k < size; k = Math.max(k + 1, (int) (k * 1.5))) {
        assertKthMatchesSort(a, k);
      }
      assertKthMatchesSort(a, 0);
      assertKthMatchesSort(a, size - 1);
      assertKthMatchesSort(a, size / 2);
    }
  }

  @Test
  void handlesDuplicates() {
    final Integer[] a = new Integer[10_000];
    for (int i = 0; i < a.length; i++) {
      a[i] = i % 7;
    }
    assertKthMatchesSort(a, 0);
    assertKthMatchesSort(a, 123);
    assertKthMatchesSort(a, a.length / 2);
    assertKthMatchesSort(a, a.length - 1);
  }

  @Test
  void handlesAlreadySortedAndReversed() {
    final Integer[] sorted = new Integer[2048];
    for (int i = 0; i < sorted.length; i++) {
      sorted[i] = i;
    }
    final Integer[] reversed = sorted.clone();
    for (int i = 0, j = reversed.length - 1; i < j; i++, j--) {
      final Integer tmp = reversed[i];
      reversed[i] = reversed[j];
      reversed[j] = tmp;
    }

    for (int k : new int[] {0, 1, 17, 1024, sorted.length - 2, sorted.length - 1}) {
      assertKthMatchesSort(sorted, k);
      assertKthMatchesSort(reversed, k);
    }
  }

  @Test
  void longsThrowOnEmptyOrOutOfRangeK() {
    assertThrows(
        IllegalArgumentException.class, () -> OrderStatistics.selectKthInPlace(new long[0], 0));
    assertThrows(
        IllegalArgumentException.class,
        () -> OrderStatistics.selectKthInPlace(new long[] {1, 2, 3}, -1));
    assertThrows(
        IllegalArgumentException.class,
        () -> OrderStatistics.selectKthInPlace(new long[] {1, 2, 3}, 3));
  }

  @Test
  void longsSelectKthMatchesSortForRandom() {
    final SplittableRandom rnd = new SplittableRandom(1L);
    for (int size : new int[] {1, 2, 3, 10, 31, 32, 33, 128, 1024, 10_000}) {
      final long[] a = new long[size];
      for (int i = 0; i < size; i++) {
        a[i] = rnd.nextLong();
      }
      for (int k = 0; k < size; k = Math.max(k + 1, (int) (k * 1.5))) {
        assertKthMatchesSort(a, k);
      }
      assertKthMatchesSort(a, size - 1);
      assertKthMatchesSort(a, size / 2);
    }
  }

  @Test
  void longsHandleDuplicatesAndExtremes() {
    final long[] a = new long[10_000];
    for (int i = 0; i < a.length; i++) {
      a[i] =
          switch (i % 4) {
            case 0 -> Long.MIN_VALUE;
            case 1 -> Long.MAX_VALUE;
            default -> i % 7;
          };
    }
    for (int k : new int[] {0, 123, 2_499, 2_500, a.length / 2, 7_499, 7_500, a.length - 1}) {
      assertKthMatchesSort(a, k);
    }
  }

  @Test
  void longsHandleAlreadySortedAndReversed() {
    final long[] sorted = new long[2048];
    final long[] reversed = new long[2048];
    for (int i = 0; i < sorted.length; i++) {
      sorted[i] = i;
      reversed[i] = sorted.length - 1L - i;
    }
    for (int k : new int[] {0, 1, 17, 1024, sorted.length - 2, sorted.length - 1}) {
      assertKthMatchesSort(sorted, k);
      assertKthMatchesSort(reversed, k);
    }
  }

  private static void assertKthMatchesSort(final long[] input, final int k) {
    final long[] sorted = input.clone();
    Arrays.sort(sorted);
    assertEquals(sorted[k], OrderStatistics.selectKthInPlace(input.clone(), k));
  }

  private static void assertKthMatchesSort(final Integer[] input, final int k) {
    final Integer[] a1 = input.clone();
    final Integer[] a2 = input.clone();

    Arrays.sort(a1);
    final Integer expected = a1[k];
    final Integer got = OrderStatistics.selectKthInPlace(a2, k);
    assertEquals(expected, got);
  }
}
