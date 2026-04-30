/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.common.storage;

import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.lang.reflect.Method;
import java.util.Optional;

public final class RocksDbStorageExceptionHelper {

  private static final String ROCKS_DB_EXCEPTION_CLASS_NAME = "org.rocksdb.RocksDBException";
  private static final String BUSY_STATUS_CODE_NAME = "Busy";

  private RocksDbStorageExceptionHelper() {}

  public static boolean isBusyException(final StorageException storageException) {
    return hasStatusCode(storageException, BUSY_STATUS_CODE_NAME);
  }

  public static boolean hasStatusCode(
      final StorageException storageException, final String statusCodeName) {
    return getStatusCodeName(storageException).filter(statusCodeName::equals).isPresent();
  }

  public static Optional<String> getStatusCodeName(final StorageException storageException) {
    return Optional.ofNullable(storageException.getCause())
        .filter(cause -> ROCKS_DB_EXCEPTION_CLASS_NAME.equals(cause.getClass().getName()))
        .flatMap(RocksDbStorageExceptionHelper::extractStatusCodeName);
  }

  private static Optional<String> extractStatusCodeName(final Throwable rocksDbException) {
    try {
      // ethereum/core does not depend directly on rocksdbjni, so inspect the status via
      // reflection instead of relying on exception message text.
      final Method getStatusMethod = rocksDbException.getClass().getMethod("getStatus");
      final Object status = getStatusMethod.invoke(rocksDbException);
      if (status == null) {
        return Optional.empty();
      }

      final Method getCodeMethod = status.getClass().getMethod("getCode");
      final Object statusCode = getCodeMethod.invoke(status);
      if (statusCode == null) {
        return Optional.empty();
      }

      if (statusCode instanceof Enum<?> enumStatusCode) {
        return Optional.of(enumStatusCode.name());
      }
      return Optional.of(statusCode.toString());
    } catch (final ReflectiveOperationException ignored) {
      return Optional.empty();
    }
  }
}
