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
package org.hyperledger.besu.plugin.services.storage;

import org.hyperledger.besu.plugin.Unstable;

/** Data storage configuration */
@Unstable
public interface DataStorageConfiguration {

  /**
   * Database format. This sets the list of segmentIdentifiers that should be initialized.
   *
   * @return Database format.
   */
  @Unstable
  DataStorageFormat getDatabaseFormat();

  /**
   * Whether receipt compaction is enabled. When enabled this reduces the storage needed for
   * receipts.
   *
   * @return Whether receipt compaction is enabled
   */
  @Unstable
  boolean getReceiptCompactionEnabled();

  /**
   * Whether revert reasons are persisted in transaction receipts.
   *
   * @return Whether revert reason persistence is enabled
   */
  @Unstable
  default boolean getRevertReasonEnabled() {
    return false;
  }
}
