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
package org.hyperledger.besu.ethereum.eth.sync.snap2.storage;

import java.util.concurrent.atomic.AtomicLong;

/** How much of the state went into sorted files and how much through the regular write path. */
final class SortedIngestStats {

  private static final long MEGABYTE = 1024L * 1024;

  private final AtomicLong files = new AtomicLong();
  private final AtomicLong fileBytes = new AtomicLong();
  private final AtomicLong regularBytes = new AtomicLong();
  private final AtomicLong outOfOrder = new AtomicLong();

  void file(final long bytes) {
    files.incrementAndGet();
    fileBytes.addAndGet(bytes);
  }

  void regular(final long bytes) {
    regularBytes.addAndGet(bytes);
  }

  void outOfOrder() {
    outOfOrder.incrementAndGet();
  }

  long outOfOrderEntries() {
    return outOfOrder.get();
  }

  @Override
  public String toString() {
    return String.format(
        "%d sorted files with %d MB, %d MB through the regular write path, %d entries out of order",
        files.get(), fileBytes.get() / MEGABYTE, regularBytes.get() / MEGABYTE, outOfOrder.get());
  }
}
