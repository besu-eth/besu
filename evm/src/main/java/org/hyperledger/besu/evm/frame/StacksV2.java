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
 * The free EVM v2 stacks: those of a thread, one per call depth plus a few for deeper frames, and
 * shared ones for deeper frames. A full stack is 32 KB, too large for the thread's allocation
 * buffer, so allocating one per frame contends on the heap lock with every other allocating thread.
 */
final class StacksV2 {

  // Depths 0 to 20 hold 99.99% of mainnet frames. Deeper frames share stacks across threads, so
  // that a deep call chain does not leave megabytes of stacks on every thread that ran one.
  static final int CACHED_DEPTHS = 21;

  // The stacks of two full call chains, up to 64 MB, which stay allocated once used. Without them,
  // a transaction calling in a loop past the cached depths, or recursing through them again and
  // again, allocates 32 KB for every frame.
  static final int SHARED_STACKS = 2 * 1024;

  // A thread moves half of these to or from the shared stacks at a time, so a deep call chain
  // takes the lock once per few frames, and the threads running it rarely wait for each other.
  static final int THREAD_DEEP_STACKS = 16;

  private static final int BATCH = THREAD_DEEP_STACKS / 2;

  private static final int SHARED_LENGTH = MessageFrame.DEFAULT_MAX_STACK_SIZE * 4;

  // Only stacks of completed frames are kept, so a stack is either here or used by one frame, and
  // a transaction that aborts with frames on its frame stack leaves nothing of it behind.
  private static final ThreadLocal<ThreadStacks> STACKS =
      ThreadLocal.withInitial(ThreadStacks::new);

  // a stack of free stacks, guarded by itself
  private static final long[][] SHARED = new long[SHARED_STACKS][];
  private static int sharedCount;

  private static final class ThreadStacks {
    private final long[][] byDepth = new long[CACHED_DEPTHS][];
    // a stack of free full-length stacks for frames deeper than the cached depths
    private final long[][] deep = new long[THREAD_DEEP_STACKS][];
    private int deepCount;
  }

  private StacksV2() {}

  /**
   * Returns a stack for a frame at the given call depth. Its contents are undefined.
   *
   * @param depth the call depth of the frame
   * @param length the length of the stack in longs
   * @return the stack
   */
  static long[] take(final int depth, final int length) {
    final ThreadStacks stacks = STACKS.get();
    if (depth >= CACHED_DEPTHS) {
      return length == SHARED_LENGTH ? takeDeep(stacks) : new long[length];
    }
    final long[] stack = stacks.byDepth[depth];
    if (stack == null || stack.length != length) {
      return new long[length];
    }
    stacks.byDepth[depth] = null;
    return stack;
  }

  /**
   * Hands the stack of a completed frame to the next frame at its call depth on this thread.
   *
   * @param depth the call depth of the frame
   * @param stack the stack, which nothing reads or writes afterwards
   */
  static void release(final int depth, final long[] stack) {
    final ThreadStacks stacks = STACKS.get();
    if (depth < CACHED_DEPTHS) {
      stacks.byDepth[depth] = stack;
    } else if (stack.length == SHARED_LENGTH) {
      releaseDeep(stacks, stack);
    }
  }

  private static long[] takeDeep(final ThreadStacks stacks) {
    if (stacks.deepCount == 0) {
      synchronized (SHARED) {
        final int moved = Math.min(BATCH, sharedCount);
        for (int i = 0; i < moved; i++) {
          stacks.deep[stacks.deepCount++] = SHARED[--sharedCount];
          SHARED[sharedCount] = null;
        }
      }
      if (stacks.deepCount == 0) {
        return new long[SHARED_LENGTH];
      }
    }
    final long[] stack = stacks.deep[--stacks.deepCount];
    stacks.deep[stacks.deepCount] = null;
    return stack;
  }

  private static void releaseDeep(final ThreadStacks stacks, final long[] stack) {
    if (stacks.deepCount == THREAD_DEEP_STACKS) {
      synchronized (SHARED) {
        for (int i = 0; i < BATCH; i++) {
          final long[] spilled = stacks.deep[--stacks.deepCount];
          stacks.deep[stacks.deepCount] = null;
          if (sharedCount < SHARED_STACKS) {
            SHARED[sharedCount++] = spilled;
          }
        }
      }
    }
    stacks.deep[stacks.deepCount++] = stack;
  }
}
