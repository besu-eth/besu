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
package org.hyperledger.besu.evm.internal;

import java.util.function.Supplier;

/**
 * One cancellable unit of EVM work: records the thread running it so {@link #interrupt()} can stop
 * it through the {@link EvmKillSwitch}, pairing each scoped arm with its release.
 *
 * <p>{@link #run} registers the calling thread for the duration of the task. {@code interrupt} arms
 * the kill switch with a scoped arm, once however often it is called, and interrupts the registered
 * thread, so the task stops at the EVM's next poll. When the task exits, {@code run} releases the
 * arm and clears a leftover interrupt flag, so neither can leak into unrelated work scheduled next
 * on the same pool thread. All transitions happen under this object's lock, which makes interrupt
 * delivery race-free: an interrupt can only land while the task is running.
 *
 * <p>An instance runs one task at a time; {@code interrupt} is safe from any thread, and a call
 * while no task is running is a no-op.
 */
public final class InterruptibleExecution {

  // All guarded by this.
  private Thread thread;
  private boolean interrupted;

  /** Creates an InterruptibleExecution. */
  public InterruptibleExecution() {}

  /**
   * Runs the task with the current thread registered for {@link #interrupt()}.
   *
   * @param task the work that the EVM kill switch may stop
   * @param <T> the type of the task's result
   * @return the task's result
   */
  public <T> T run(final Supplier<T> task) {
    synchronized (this) {
      thread = Thread.currentThread();
    }
    try {
      return task.get();
    } finally {
      synchronized (this) {
        thread = null;
        // clear an interrupt that landed after the task finished its work, so it cannot leak
        // into an unrelated task scheduled next on this pool thread
        Thread.interrupted();
        if (interrupted) {
          interrupted = false;
          EvmKillSwitch.release();
        }
      }
    }
  }

  /**
   * Runs the task with the current thread registered for {@link #interrupt()}.
   *
   * @param task the work that the EVM kill switch may stop
   */
  public void run(final Runnable task) {
    run(
        () -> {
          task.run();
          return null;
        });
  }

  /**
   * Interrupts the thread of the running task, if any, so the EVM kill switch stops it at its next
   * poll. The kill switch stays armed until the task exits.
   */
  public synchronized void interrupt() {
    if (thread != null) {
      if (!interrupted) {
        interrupted = true;
        EvmKillSwitch.armScoped();
      }
      thread.interrupt();
    }
  }
}
