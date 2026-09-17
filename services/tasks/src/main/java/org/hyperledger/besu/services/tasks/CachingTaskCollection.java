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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import org.jspecify.annotations.Nullable;

/**
 * The Caching task collection.
 *
 * @param <T> the type parameter
 */
public class CachingTaskCollection<T> implements TaskCollection<T> {

  /** The constant DEFAULT_CACHE_SIZE. */
  public static final int DEFAULT_CACHE_SIZE = 1_000_000;

  private final int maxCacheSize;

  // The underlying collection
  private final TaskCollection<T> wrappedCollection;

  /**
   * A cache of tasks to operate on before going to {@link CachingTaskCollection#wrappedCollection}
   */
  private final Queue<Task<T>> cache = new ArrayDeque<>();

  // Tasks that have been removed, but not marked completed yet
  private final Set<Task<T>> outstandingTasks = new HashSet<>();

  private boolean closed = false;

  /**
   * Instantiates a new Caching task collection.
   *
   * @param collection the collection
   * @param maxCacheSize the max cache size
   */
  public CachingTaskCollection(final TaskCollection<T> collection, final int maxCacheSize) {
    this.wrappedCollection = collection;
    this.maxCacheSize = maxCacheSize;
  }

  /**
   * Instantiates a new Caching task collection.
   *
   * @param collection the collection
   */
  public CachingTaskCollection(final TaskCollection<T> collection) {
    this(collection, DEFAULT_CACHE_SIZE);
  }

  @Override
  public synchronized void add(final T taskData) {
    assertNotClosed();
    if (cacheSize() >= maxCacheSize) {
      // Too many tasks in the cache, push this to the underlying collection
      wrappedCollection.add(taskData);
      return;
    }

    Task<T> newTask = new CachedTask<>(this, taskData);
    cache.add(newTask);
  }

  @Override
  public synchronized @Nullable Task<T> remove() {
    assertNotClosed();
    if (cache.size() == 0) {
      return wrappedCollection.remove();
    }

    final Task<T> pendingTask = cache.remove();
    outstandingTasks.add(pendingTask);
    return pendingTask;
  }

  @Override
  public synchronized void clear() {
    assertNotClosed();
    wrappedCollection.clear();
    outstandingTasks.clear();
    cache.clear();
  }

  @Override
  public synchronized long size() {
    return wrappedCollection.size() + cache.size();
  }

  /**
   * Cache size.
   *
   * @return the cache size
   */
  public synchronized int cacheSize() {
    return outstandingTasks.size() + cache.size();
  }

  @Override
  public synchronized boolean isEmpty() {
    return size() == 0;
  }

  /**
   * @return True if all tasks have been removed and processed.
   */
  @Override
  public synchronized boolean allTasksCompleted() {
    return cacheSize() == 0 && wrappedCollection.allTasksCompleted();
  }

  private synchronized boolean completePendingTask(final CachedTask<T> cachedTask) {
    return outstandingTasks.remove(cachedTask);
  }

  private synchronized void failPendingTask(final CachedTask<T> cachedTask) {
    if (completePendingTask(cachedTask)) {
      cache.add(cachedTask);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    outstandingTasks.clear();
    cache.clear();
    wrappedCollection.close();
    closed = true;
  }

  private void assertNotClosed() {
    if (closed) {
      throw new IllegalStateException("Attempt to access closed " + getClass().getSimpleName());
    }
  }

  private static class CachedTask<T> implements Task<T> {
    private final CachingTaskCollection<T> cachingTaskCollection;
    private final T data;

    private CachedTask(final CachingTaskCollection<T> cachingTaskCollection, final T data) {
      this.cachingTaskCollection = cachingTaskCollection;
      this.data = data;
    }

    @Override
    public T getData() {
      return data;
    }

    @Override
    public void markCompleted() {
      cachingTaskCollection.completePendingTask(this);
    }

    @Override
    public void markFailed() {
      cachingTaskCollection.failPendingTask(this);
    }
  }
}
