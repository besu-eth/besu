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
package org.hyperledger.besu.plugin.data;

/** Represents the reason a block is marked as "bad" */
public interface BadBlockCause {

  /**
   * The reason why the block was categorized as bad
   *
   * @return The reason enum
   */
  BadBlockReason getReason();

  /**
   * A more descriptive explanation for why the block was marked bad
   *
   * @return the description
   */
  String getDescription();

  /** An enum representing the reason why a block is marked bad */
  enum BadBlockReason {
    /** Standard spec-related validation failures */
    SPEC_VALIDATION_FAILURE,
    /** This block is bad because it descends from a bad block */
    DESCENDS_FROM_BAD_BLOCK,
  }
}
