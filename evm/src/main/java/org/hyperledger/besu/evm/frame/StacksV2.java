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
package org.hyperledger.besu.evm.frame;

/**
 * The free EVM v2 stacks of a thread, one per call depth. A full stack is 32 KB, too large for the
 * thread's allocation buffer, so allocating one per frame contends on the heap lock with every
 * other allocating thread.
 */
final class StacksV2 {

  // Depths 0 to 20 hold 99.99% of mainnet frames. Deeper frames allocate, so that a deep call
  // chain does not leave megabytes of stacks on the thread.
  static final int CACHED_DEPTHS = 21;

  // Only stacks of completed frames are kept, so a stack is either here or used by one frame, and
  // a transaction that aborts with frames on its frame stack leaves nothing of it behind.
  private static final ThreadLocal<long[][]> STACKS =
      ThreadLocal.withInitial(() -> new long[CACHED_DEPTHS][]);

  private StacksV2() {}

  /**
   * Returns a stack for a frame at the given call depth. Its contents are undefined.
   *
   * @param depth the call depth of the frame
   * @param length the length of the stack in longs
   * @return the stack
   */
  static long[] take(final int depth, final int length) {
    if (depth >= CACHED_DEPTHS) {
      return new long[length];
    }
    final long[][] stacks = STACKS.get();
    final long[] stack = stacks[depth];
    if (stack == null || stack.length != length) {
      return new long[length];
    }
    stacks[depth] = null;
    return stack;
  }

  /**
   * Hands the stack of a completed frame to the next frame at its call depth on this thread.
   *
   * @param depth the call depth of the frame
   * @param stack the stack, which nothing reads or writes afterwards
   */
  static void release(final int depth, final long[] stack) {
    if (depth < CACHED_DEPTHS) {
      STACKS.get()[depth] = stack;
    }
  }
}
