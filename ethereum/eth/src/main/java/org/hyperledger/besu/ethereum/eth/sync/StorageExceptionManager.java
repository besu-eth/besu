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
package org.hyperledger.besu.ethereum.eth.sync;

import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.rocksdb.RocksDBException;
import org.rocksdb.Status;

public final class StorageExceptionManager {

  private static final EnumSet<Status.Code> RETRYABLE_STATUS_CODES =
      EnumSet.of(Status.Code.TimedOut, Status.Code.TryAgain, Status.Code.Busy);

  private static final long ERROR_THRESHOLD = 1000;

  private static final AtomicLong retryableErrorCounter = new AtomicLong();

  /**
   * Determines if an operation can be retried based on the error received. This method checks if
   * the cause of the StorageException is a RocksDBException. If it is, it retrieves the status code
   * of the RocksDBException and checks if it is contained in the list of retryable {@link
   * StorageExceptionManager.RETRYABLE_STATUS_CODES} status codes.
   *
   * @param e the StorageException to check
   * @return true if the operation can be retried, false otherwise
   */
  public static boolean canRetryOnError(final StorageException e) {
    return Optional.ofNullable(e.getCause())
        .filter(z -> z instanceof RocksDBException)
        .map(RocksDBException.class::cast)
        .map(RocksDBException::getStatus)
        .map(Status::getCode)
        .filter(RETRYABLE_STATUS_CODES::contains)
        .map(
            statusCode -> {
              retryableErrorCounter.incrementAndGet();
              return true;
            })
        .orElse(false);
  }

  public static long getRetryableErrorCounter() {
    return retryableErrorCounter.get();
  }

  public static boolean errorCountAtThreshold() {
    return retryableErrorCounter.get() % ERROR_THRESHOLD == 1;
  }
}
