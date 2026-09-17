/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.services.tasks;

import java.io.Closeable;

import org.jspecify.annotations.Nullable;

/**
 * The interface Task collection.
 *
 * @param <T> the type parameter
 */
public interface TaskCollection<T> extends Closeable {
  /**
   * Add some data that needs to be processed.
   *
   * @param taskData The data to be processed.
   */
  void add(T taskData);

  /**
   * Get a task for processing. This task will be tracked as a pending task until either {@code
   * Task.markCompleted}* or {@code Task.requeue} is called.
   *
   * @return The task to be processed, or {@code null} if no task is available.
   */
  @Nullable Task<T> remove();

  /**
   * Returns the number of tasks in the queue.
   *
   * @return The number of tasks in the queue.
   */
  long size();

  /**
   * Returns True if all tasks have been dequeued.
   *
   * @return True if all tasks have been dequeued.
   */
  boolean isEmpty();

  /** Clear all data from the queue. */
  void clear();

  /**
   * Returns True if all tasks have been dequeued and processed.
   *
   * @return True if all tasks have been dequeued and processed.
   */
  boolean allTasksCompleted();
}
