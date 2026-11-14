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
package org.hyperledger.besu.plugin.services.storage;

import java.util.concurrent.CompletableFuture;

/**
 * A bulk load of one or more segments in progress, see {@link
 * SegmentedKeyValueStorage#startBulkLoad}.
 */
public interface SegmentBulkLoad extends AutoCloseable {

  /**
   * Ends the load of one of the segments before the others, as {@link #close()} does for all. Reads
   * of a segment that took in much through regular writes during the load are slow until the
   * storage has caught up on it. The default implementation does nothing.
   *
   * @param segment the segment, one of those the load was started with
   * @return completes once the storage has caught up on the segment; cancelling it only stops the
   *     wait of the caller
   * @throws IllegalArgumentException if the load was not started with the segment
   */
  default CompletableFuture<Void> end(final SegmentIdentifier segment) {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Ends the bulk load. The storage catches up on the reorganisation of the segments it held back,
   * which can take minutes for a large load, and goes back to doing it on its own. The segments can
   * be read and written while this runs.
   */
  @Override
  void close();
}
