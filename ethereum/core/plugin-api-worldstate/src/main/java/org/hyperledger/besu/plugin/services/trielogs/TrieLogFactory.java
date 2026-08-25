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
package org.hyperledger.besu.plugin.services.trielogs;

import org.hyperledger.besu.plugin.data.BlockHeader;

/** Interface for creating, serializing and deserializing {@link TrieLog} objects. */
public interface TrieLogFactory {

  /**
   * Creates a new TrieLog object.
   *
   * @param accumulator the accumulator
   * @param blockHeader the block header
   * @return a new TrieLog object
   */
  TrieLog create(TrieLogAccumulator accumulator, BlockHeader blockHeader);

  /**
   * Deserializes a TrieLog object.
   *
   * @param bytes the serialized TrieLog
   * @return the deserialized TrieLog
   */
  TrieLog deserialize(final byte[] bytes);

  /**
   * Serializes a TrieLog object.
   *
   * @param layer the TrieLog
   * @return the serialized TrieLog
   */
  byte[] serialize(final TrieLog layer);
}
