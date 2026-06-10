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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The InMemory task queue.
 *
 * @param <T> the type parameter
 */
public class InMemoryTaskQueue<T> implements TaskCollection<T> {
  private final Queue<T> internalQueue = new ArrayDeque<>();
  private final Set<InMemoryTask<T>> unfinishedOutstandingTasks = new HashSet<>();
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /** Default constructor. */
  public InMemoryTaskQueue() {}

  @Override
  public synchronized void add(final T taskData) {
    assertNotClosed();
    internalQueue.add(taskData);
  }

  @Override
  public synchronized Task<T> remove() {
    assertNotClosed();
    T data = internalQueue.poll();
    if (data == null) {
      return null;
    }
    InMemoryTask<T> task = new InMemoryTask<>(this, data);
    unfinishedOutstandingTasks.add(task);
    return task;
  }

  @Override
  public synchronized long size() {
    assertNotClosed();
    return internalQueue.size();
  }

  @Override
  public synchronized boolean isEmpty() {
    assertNotClosed();
    return size() == 0;
  }

  @Override
  public synchronized void clear() {
    assertNotClosed();

    unfinishedOutstandingTasks.clear();
    internalQueue.clear();
  }

  /** Clear internal queue. */
  public void clearInternalQueue() {
    internalQueue.clear();
  }

  @Override
  public synchronized boolean allTasksCompleted() {
    assertNotClosed();
    return isEmpty() && unfinishedOutstandingTasks.size() == 0;
  }

  /**
   * Returns the number of tasks which are currently being processed.
   *
   * @return number of tasks which are currently being processed
   */
  public synchronized long outstandingTaskCount() {
    assertNotClosed();
    return unfinishedOutstandingTasks.size();
  }

  @Override
  public synchronized void close() {
    closed.set(true);
    internalQueue.clear();
  }

  private void assertNotClosed() {
    if (closed.get()) {
      throw new IllegalStateException("Attempt to access closed " + getClass().getSimpleName());
    }
  }

  /**
   * Return task queue as list.
   *
   * @return the list
   */
  public synchronized List<T> asList() {
    return new ArrayList<>(internalQueue);
  }

  private synchronized void handleFailedTask(final InMemoryTask<T> task) {
    if (markTaskCompleted(task)) {
      add(task.getData());
    }
  }

  private synchronized boolean markTaskCompleted(final InMemoryTask<T> task) {
    return unfinishedOutstandingTasks.remove(task);
  }

  private static class InMemoryTask<T> implements Task<T> {
    private final T data;
    private final InMemoryTaskQueue<T> queue;
    private final AtomicBoolean completed = new AtomicBoolean(false);

    /**
     * Instantiates a new InMemory task.
     *
     * @param queue the queue
     * @param data the data
     */
    public InMemoryTask(final InMemoryTaskQueue<T> queue, final T data) {
      this.queue = queue;
      this.data = data;
    }

    @Override
    public T getData() {
      return data;
    }

    @Override
    public void markCompleted() {
      if (completed.compareAndSet(false, true)) {
        queue.markTaskCompleted(this);
      }
    }

    @Override
    public void markFailed() {
      if (completed.compareAndSet(false, true)) {
        queue.handleFailedTask(this);
      }
    }
  }
}
