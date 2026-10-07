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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.validation.constraints.NotNull;
import org.jspecify.annotations.Nullable;

/**
 * The InMemory tasks priority queues.
 *
 * @param <T> the type parameter
 */
public class InMemoryTasksPriorityQueues<T extends TasksPriorityProvider>
    implements TaskCollection<T> {
  private final List<PriorityQueue<T>> internalQueues = new ArrayList<>(16);
  private final Set<InMemoryTask<T>> unfinishedOutstandingTasks = new HashSet<>();
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /** Instantiates a new InMemory tasks priority queues. */
  public InMemoryTasksPriorityQueues() {
    clearInternalQueues();
  }

  /** Clear internal queues. */
  public synchronized void clearInternalQueues() {
    internalQueues.clear();
    for (int i = 0; i < 16; i++) {
      internalQueues.add(newEmptyQueue());
    }
  }

  @NotNull
  private PriorityQueue<T> newEmptyQueue() {
    return new PriorityQueue<>(Comparator.comparingLong(TasksPriorityProvider::getPriority));
  }

  @Override
  public synchronized void add(final T taskData) {
    assertNotClosed();
    var dequeue = findQueue(taskData.getDepth());
    dequeue.add(taskData);
  }

  private PriorityQueue<T> findQueue(final int priority) {
    while (priority + 1 > internalQueues.size()) {
      internalQueues.add(newEmptyQueue());
    }
    return internalQueues.get(priority);
  }

  @Override
  public synchronized @Nullable Task<T> remove() {
    assertNotClosed();
    final Queue<T> lastNonEmptyQueue = findLastNonEmptyQueue();
    if (lastNonEmptyQueue.isEmpty()) {
      return null;
    }
    T data = lastNonEmptyQueue.remove();
    InMemoryTask<T> task = new InMemoryTask<>(this, data);
    unfinishedOutstandingTasks.add(task);
    return task;
  }

  private Queue<T> findLastNonEmptyQueue() {
    for (int i = internalQueues.size() - 1; i > 0; i--) {
      final Queue<T> queue = internalQueues.get(i);
      if (!queue.isEmpty()) {
        return queue;
      }
    }
    return internalQueues.get(0);
  }

  @Override
  public synchronized long size() {
    return internalQueues.stream().mapToInt(Queue::size).sum();
  }

  @Override
  public synchronized boolean isEmpty() {
    return findLastNonEmptyQueue().isEmpty();
  }

  @Override
  public synchronized void clear() {
    assertNotClosed();

    unfinishedOutstandingTasks.clear();
    clearInternalQueues();
  }

  @Override
  public synchronized boolean allTasksCompleted() {
    return isEmpty() && unfinishedOutstandingTasks.isEmpty();
  }

  @Override
  public synchronized void close() {
    if (closed.compareAndSet(false, true)) {
      internalQueues.clear();
      unfinishedOutstandingTasks.clear();
    }
  }

  private void assertNotClosed() {
    if (closed.get()) {
      throw new IllegalStateException("Attempt to access closed " + getClass().getSimpleName());
    }
  }

  private synchronized void handleFailedTask(final InMemoryTask<T> task) {
    if (markTaskCompleted(task)) {
      add(task.getData());
    }
  }

  private synchronized boolean markTaskCompleted(final InMemoryTask<T> task) {
    return unfinishedOutstandingTasks.remove(task);
  }

  /**
   * Check if queue Contains the task .
   *
   * @param request the request
   * @return the boolean
   */
  public synchronized boolean contains(final T request) {
    final PriorityQueue<T> queue = findQueue(request.getDepth());
    return queue.contains(request)
        || unfinishedOutstandingTasks.stream()
            .map(InMemoryTask::getData)
            .anyMatch(data -> data.equals(request));
  }

  private static class InMemoryTask<T extends TasksPriorityProvider> implements Task<T> {
    private final T data;
    private final InMemoryTasksPriorityQueues<T> queue;
    private final AtomicBoolean completed = new AtomicBoolean(false);

    /**
     * Instantiates a new InMemory task.
     *
     * @param queue the queue
     * @param data the data
     */
    public InMemoryTask(final InMemoryTasksPriorityQueues<T> queue, final T data) {
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
