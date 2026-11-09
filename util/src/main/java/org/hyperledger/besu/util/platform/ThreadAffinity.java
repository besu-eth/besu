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
package org.hyperledger.besu.util.platform;

import java.util.BitSet;

import com.google.common.base.Splitter;
import com.sun.jna.Library;
import com.sun.jna.Native;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Experiment: binds the calling thread to some CPUs, on Linux. */
public final class ThreadAffinity {

  private static final Logger LOG = LoggerFactory.getLogger(ThreadAffinity.class);

  private interface CLibrary extends Library {
    int sched_setaffinity(int pid, long cpusetsize, long[] mask);
  }

  private ThreadAffinity() {}

  /**
   * Parses a CPU list such as "1" or "0,3-19".
   *
   * @param cpus the CPU list
   * @return the CPUs
   */
  public static BitSet parse(final String cpus) {
    final BitSet set = new BitSet();
    for (final String part : Splitter.on(',').split(cpus)) {
      final String range = part.trim();
      if (range.isEmpty()) {
        continue;
      }
      final int dash = range.indexOf('-');
      if (dash < 0) {
        set.set(Integer.parseInt(range));
      } else {
        set.set(
            Integer.parseInt(range.substring(0, dash)),
            Integer.parseInt(range.substring(dash + 1)) + 1);
      }
    }
    return set;
  }

  /**
   * Binds the calling thread to the given CPUs.
   *
   * @param cpus the CPUs
   * @return whether the thread is bound to them
   */
  public static boolean bindCurrentThread(final BitSet cpus) {
    try {
      final CLibrary c = Native.load("c", CLibrary.class);
      final long[] mask = new long[16];
      cpus.stream().forEach(cpu -> mask[cpu / 64] |= 1L << (cpu % 64));
      final int result = c.sched_setaffinity(0, (long) mask.length * Long.BYTES, mask);
      if (result != 0) {
        LOG.warn("Binding thread {} to CPUs {} failed", Thread.currentThread().getName(), cpus);
      }
      return result == 0;
    } catch (final UnsatisfiedLinkError | RuntimeException e) {
      LOG.warn("Binding thread {} to CPUs {} failed", Thread.currentThread().getName(), cpus, e);
      return false;
    }
  }
}
