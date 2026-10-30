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
package org.hyperledger.besu.services.pipeline;

import java.util.Collection;

import org.jspecify.annotations.Nullable;

/**
 * The interface used to read items from a pipe.
 *
 * @param <T> the type of input.
 */
public interface ReadPipe<T> {

  /**
   * Determines if this pipe has more items to be read. The pipe is considered to have no more items
   * when it has either been aborted with {@link WritePipe#abort()} or if all queued items have been
   * read and the pipe has been closed with {@link WritePipe#close()}.
   *
   * @return true if there are more items to process, otherwise false.
   */
  boolean hasMore();

  /**
   * Determines if this pipeline this pipe is a part of has been aborted.
   *
   * @return true if the pipeline has been aborted, otherwise false.
   */
  boolean isAborted();

  /**
   * Get and remove the next item from this pipe. This method will block until the next item is
   * available but may still return <code>null</code> if the pipe is closed or the thread
   * interrupted while waiting.
   *
   * @return the next item or <code>null</code> if the pipe is closed or the thread interrupted.
   */
  @Nullable T get();

  /**
   * Get and remove the next item from this pipe without blocking if it is available.
   *
   * @return the next item or <code>null</code> if the pipe is empty.
   */
  @Nullable T poll();

  /**
   * Removes at most the given number of available elements from the pipe and adds them to the given
   * collection. This method does not block.
   *
   * @param output the collection to transfer elements into
   * @param maxElements the maximum number of elements to transfer
   * @return the number of elements drained in the pipe
   */
  int drainTo(Collection<T> output, int maxElements);
}
