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

/** Supported database storage format */
public enum DataStorageFormat {
  /** Original format. Store all tries */
  FOREST,
  /** New format. Store one trie, and trie logs to roll forward and backward */
  BONSAI,
  /** The option for storing archive data e.g. state at any block */
  X_BONSAI_ARCHIVE;

  /**
   * Returns whether the storage format is one of the Bonsai DB formats
   *
   * @return true if it is, otherwise false
   */
  public boolean isBonsaiFormat() {
    return this == BONSAI || this == X_BONSAI_ARCHIVE;
  }
}
