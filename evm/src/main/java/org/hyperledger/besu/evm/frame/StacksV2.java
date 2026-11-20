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
 * The EVM v2 stacks used by each thread.
 *
 * <p>A thread keeps one stack for each frame call depth below {@link #CACHED_DEPTHS}. For deeper
 * frames, there is a pool of stacks shared across threads. Stacks are lazily initialized. At, or
 * above {@link #CACHED_DEPTHS}, a thread reserves up to {@link #THREAD_DEEP_STACKS} for its own use
 * from the shared pool, or allocates them on demand if shared pool is empty. The reservation from
 * the shared pool happens in batches of up to {@link #BATCH} stacks at a time.
 *
 * <p>Upon releasing, each thread spills stacks back to the shared stack pool in batches or drops
 * stacks entirely if the shared pool has been fully filled up ({@link #sharedCount} == {@link
 * #SHARED_STACKS}). The shared pool is lazily populated by {@link #release(int, long[])} when it is
 * cold.
 *
 * <p>EVM invariant: A caller can never take more than one stack at a certain call depth.
 */
final class StacksV2 {

  // Depths 0 to 20 hold 99.99% of mainnet frames and are cached within the current running thread.
  static final int CACHED_DEPTHS = 21;

  // A cap on the memory the free stacks keep, 64 MB once used. Under the transaction gas cap of
  // 2^24 a call chain reaches at most depth 493, so this keeps the deep stacks of four such chains
  // that ran at the same time on any threads. Past the cap a released stack is dropped, and a later
  // deep frame allocates its own, as without the shared stacks.
  static final int SHARED_STACKS = 2 * 1024;

  static final int THREAD_DEEP_STACKS = 16;

  // the maximum amount of stacks that can be taken at any given time from the shared stack pool
  // when {@link #take} is called.
  private static final int BATCH = THREAD_DEEP_STACKS / 2;

  // EVM v2 supports only the default stack size
  private static final int LENGTH = MessageFrame.DEFAULT_MAX_STACK_SIZE * 4;

  private static final ThreadLocal<ThreadStacks> STACKS =
      ThreadLocal.withInitial(ThreadStacks::new);

  // a stack of free stacks, guarded by itself
  private static final long[][] SHARED = new long[SHARED_STACKS][];
  private static int sharedCount;

  private static final class ThreadStacks {
    private final long[][] byDepth = new long[CACHED_DEPTHS][];
    // a stack of free stacks for frames deeper than the cached depths
    private final long[][] deep = new long[THREAD_DEEP_STACKS][];
    private int deepCount;
  }

  private StacksV2() {}

  /**
   * Reserves an exclusive stack for the caller at a certain depth.
   *
   * @param depth the call depth of the frame
   * @return the stack
   */
  static long[] take(final int depth) {
    final ThreadStacks stacks = STACKS.get();
    if (depth >= CACHED_DEPTHS) {
      return takeDeep(stacks);
    }
    final long[] stack = stacks.byDepth[depth];
    if (stack == null) {
      return (stacks.byDepth[depth] = new long[LENGTH]);
    }
    return stack;
  }

  /**
   * Releases the stack previously reserved at a certain depth so other callers can take it.
   *
   * @param depth the call depth of the frame
   * @param stack the stack, which nothing reads or writes after this
   */
  static void release(final int depth, final long[] stack) {
    if (depth >= CACHED_DEPTHS) {
      releaseDeep(STACKS.get(), stack);
    }
  }

  private static long[] takeDeep(final ThreadStacks stacks) {
    if (stacks.deepCount == 0) {
      synchronized (SHARED) {
        final int moved = Math.min(BATCH, sharedCount);
        for (int i = 0; i < moved; i++) {
          stacks.deep[stacks.deepCount++] = SHARED[--sharedCount];
        }
      }
      if (stacks.deepCount == 0) {
        return new long[LENGTH];
      }
    }
    return stacks.deep[--stacks.deepCount];
  }

  private static void releaseDeep(final ThreadStacks stacks, final long[] stack) {
    if (stacks.deepCount == THREAD_DEEP_STACKS) {
      synchronized (SHARED) {
        for (int i = 0; i < BATCH; i++) {
          final long[] spilled = stacks.deep[--stacks.deepCount];
          if (sharedCount < SHARED_STACKS) {
            SHARED[sharedCount++] = spilled;
          }
        }
      }
    }
    stacks.deep[stacks.deepCount++] = stack;
  }
}
