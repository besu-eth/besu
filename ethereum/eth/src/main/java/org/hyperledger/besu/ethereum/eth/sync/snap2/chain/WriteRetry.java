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
package org.hyperledger.besu.ethereum.eth.sync.snap2.chain;

import org.hyperledger.besu.ethereum.eth.sync.StorageExceptionManager;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.util.concurrent.CancellationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tries a write again when RocksDB fails to check the conflicts of its transaction, as the writes
 * of a sync can make it do.
 */
public final class WriteRetry {

  private static final Logger LOG = LoggerFactory.getLogger(WriteRetry.class);
  private static final int ATTEMPTS = 6;

  /** Waits between two tries. */
  public interface Pause {
    void sleep(long millis) throws InterruptedException;
  }

  private WriteRetry() {}

  /**
   * Runs the write up to six times, 20 ms to 320 ms apart, while RocksDB fails to check it; any
   * other failure, or the last one, goes to the caller.
   */
  public static void run(final Runnable write, final Pause pause) {
    for (int attempt = 1; attempt < ATTEMPTS; attempt++) {
      try {
        write.run();
        return;
      } catch (final StorageException e) {
        if (!StorageExceptionManager.canRetryOnError(e)) {
          throw e;
        }
        LOG.debug("Write not committed, retrying: {}", e.getMessage());
        try {
          pause.sleep(10L << attempt);
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new CancellationException("interrupted while retrying a write");
        }
      }
    }
    write.run();
  }
}
