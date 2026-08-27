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
package org.hyperledger.besu.plugin.services;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.worldstate.WorldView;
import org.hyperledger.besu.plugin.Unstable;

import java.util.Optional;

/** A service that plugin can use to access world state */
@Unstable
public interface WorldStateService extends BesuService {

  /**
   * Returns a view of the head world state.
   *
   * @return the head world view
   */
  WorldView getWorldView();

  /**
   * Returns a view of the world state at the specified block hash.
   *
   * @param blockHash the hash of the block to get the world view for
   * @return the world view at the specified block hash
   */
  Optional<WorldView> getWorldView(final Hash blockHash);
}
