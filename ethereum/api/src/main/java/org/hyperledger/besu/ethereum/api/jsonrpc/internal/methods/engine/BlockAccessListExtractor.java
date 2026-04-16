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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePayloadParameter;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.Optional;

/** Version-specific extractor for a block access list from an {@link EnginePayloadParameter}. */
@FunctionalInterface
public interface BlockAccessListExtractor {

  /** No-op extractor for versions that do not carry a block access list. */
  BlockAccessListExtractor NONE = payloadParameter -> Optional.empty();

  Optional<BlockAccessList> extract(EnginePayloadParameter payloadParameter)
      throws InvalidBlockAccessListException;
}
