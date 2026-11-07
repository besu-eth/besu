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

import org.hyperledger.besu.plugin.services.exception.StorageException;

/** Writer of entries in increasing key order, added to a segment at once when committed. */
public interface SortedWriter {

  /**
   * Adds an entry, whose key is greater than every key added before.
   *
   * @param key the key
   * @param value the value
   * @throws StorageException problem encountered when writing the entry, or a key not above the
   *     previous one
   */
  void put(byte[] key, byte[] value) throws StorageException;

  /**
   * Adds the written entries to the segment.
   *
   * @throws StorageException problem encountered when adding the entries
   */
  void commit() throws StorageException;

  /** Drops the written entries. */
  void rollback();
}
