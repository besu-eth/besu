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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

/** Order-statistics utilities (e.g. selecting the kth smallest element). */
public class OrderStatistics {

  // Small partitions are faster to finish via sorting than repeated partitioning.
  private static final int SMALL_PARTITION_SORT_THRESHOLD = 32;

  private OrderStatistics() {}

  /**
   * Select the kth-smallest element from {@code array} (0-based) using an in-place selection
   * algorithm.
   *
   * <p>Note: the input array will be mutated (elements may be reordered).
   *
   * @param array the array to select from
   * @param k the desired order statistic (0-based)
   * @param <T> element type
   * @return the element that would be at position {@code k} if the array were fully sorted
   */
  public static <T extends Comparable<? super T>> T selectKthInPlace(final T[] array, final int k) {
    return selectKthInPlace(array, k, Comparator.naturalOrder());
  }

  /**
   * Select the kth-smallest element from {@code array} (0-based) using an in-place selection
   * algorithm.
   *
   * <p>Note: the input array will be mutated (elements may be reordered).
   *
   * @param array the array to select from
   * @param k the desired order statistic (0-based)
   * @param comparator ordering comparator
   * @param <T> element type
   * @return the element that would be at position {@code k} if the array were fully sorted
   */
  public static <T> T selectKthInPlace(
      final T[] array, final int k, final Comparator<? super T> comparator) {
    Objects.requireNonNull(array);
    Objects.requireNonNull(comparator);

    if (array.length == 0) {
      throw new IllegalArgumentException("array must not be empty");
    }
    if (k < 0 || k >= array.length) {
      throw new IllegalArgumentException("k out of range: " + k);
    }

    int left = 0;
    int right = array.length - 1;

    // Iterative quickselect with 3-way partitioning to handle duplicates efficiently.
    while (true) {
      if (left == right) {
        return array[left];
      }

      if (right - left < SMALL_PARTITION_SORT_THRESHOLD) {
        Arrays.sort(array, left, right + 1, comparator);
        return array[k];
      }

      final int mid = left + ((right - left) >>> 1);
      final int pivotIndex = medianOfThree(array, left, mid, right, comparator);
      final Partition p = partition3Way(array, left, right, pivotIndex, comparator);

      if (k < p.lt) {
        right = p.lt - 1;
      } else if (k > p.gt) {
        left = p.gt + 1;
      } else {
        return array[k];
      }
    }
  }

  /**
   * Select the kth-smallest element from {@code array} (0-based) using an in-place selection
   * algorithm.
   *
   * <p>Note: the input array will be mutated (elements may be reordered).
   *
   * @param array the array to select from
   * @param k the desired order statistic (0-based)
   * @return the element that would be at position {@code k} if the array were fully sorted
   */
  public static long selectKthInPlace(final long[] array, final int k) {
    Objects.requireNonNull(array);

    if (array.length == 0) {
      throw new IllegalArgumentException("array must not be empty");
    }
    if (k < 0 || k >= array.length) {
      throw new IllegalArgumentException("k out of range: " + k);
    }

    int left = 0;
    int right = array.length - 1;

    while (true) {
      if (left == right) {
        return array[left];
      }

      if (right - left < SMALL_PARTITION_SORT_THRESHOLD) {
        Arrays.sort(array, left, right + 1);
        return array[k];
      }

      final long pivot =
          medianOfThree(array[left], array[left + ((right - left) >>> 1)], array[right]);

      // 3-way partition: [left, lt) < pivot, [lt, gt] == pivot, (gt, right] > pivot.
      int lt = left;
      int i = left;
      int gt = right;
      while (i <= gt) {
        if (array[i] < pivot) {
          swap(array, lt++, i++);
        } else if (array[i] > pivot) {
          swap(array, i, gt--);
        } else {
          i++;
        }
      }

      if (k < lt) {
        right = lt - 1;
      } else if (k > gt) {
        left = gt + 1;
      } else {
        return pivot;
      }
    }
  }

  private static long medianOfThree(final long a, final long b, final long c) {
    return Math.max(Math.min(a, b), Math.min(Math.max(a, b), c));
  }

  private static void swap(final long[] a, final int i, final int j) {
    final long tmp = a[i];
    a[i] = a[j];
    a[j] = tmp;
  }

  private static <T> int medianOfThree(
      final T[] a, final int i, final int j, final int k, final Comparator<? super T> comparator) {
    final T ai = a[i];
    final T aj = a[j];
    final T ak = a[k];

    // Return index of the median value among (i, j, k).
    if (comparator.compare(ai, aj) < 0) {
      if (comparator.compare(aj, ak) < 0) {
        return j; // ai < aj < ak
      }
      return comparator.compare(ai, ak) < 0 ? k : i; // ai < ak <= aj OR ak <= ai < aj
    } else {
      if (comparator.compare(ai, ak) < 0) {
        return i; // aj <= ai < ak
      }
      return comparator.compare(aj, ak) < 0 ? k : j; // aj < ak <= ai OR ak <= aj <= ai
    }
  }

  private static <T> Partition partition3Way(
      final T[] a,
      final int left,
      final int right,
      final int pivotIndex,
      final Comparator<? super T> comparator) {
    final T pivot = a[pivotIndex];
    swap(a, left, pivotIndex);

    int lt = left;
    int i = left + 1;
    int gt = right;

    while (i <= gt) {
      final int cmp = comparator.compare(a[i], pivot);
      if (cmp < 0) {
        swap(a, lt++, i++);
      } else if (cmp > 0) {
        swap(a, i, gt--);
      } else {
        i++;
      }
    }

    return new Partition(lt, gt);
  }

  private static <T> void swap(final T[] a, final int i, final int j) {
    if (i == j) {
      return;
    }
    final T tmp = a[i];
    a[i] = a[j];
    a[j] = tmp;
  }

  private record Partition(int lt, int gt) {}
}
