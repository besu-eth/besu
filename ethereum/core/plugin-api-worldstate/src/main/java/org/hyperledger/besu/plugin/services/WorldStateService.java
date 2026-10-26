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
import java.util.function.Function;

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
   * <p>The returned view is backed by a world state that Besu opens for this call and that the
   * caller has no way to close, so each call holds resources until they are reclaimed. Prefer
   * {@link #mapWorldView(Hash, Function)}, which closes the world state after use.
   *
   * @param blockHash the hash of the block to get the world view for
   * @return the world view at the specified block hash
   */
  Optional<WorldView> getWorldView(final Hash blockHash);

  /**
   * Opens the world state at the given block, applies the mapper to it, and closes the world state
   * before returning. The {@link WorldView}, and any account obtained from it, must not be used
   * after the mapper returns.
   *
   * <p>The mapper runs synchronously on the calling thread while the world state is held open, so
   * it should be short. It returns an empty value when it has nothing to return; a {@code null}
   * return is treated as empty. Exceptions thrown by the mapper, including those raised when it
   * reads state that is missing from storage, are propagated to the caller.
   *
   * @param blockHash the hash of the block whose world state to open
   * @param mapper the function to apply to the world state
   * @param <T> the type of the mapped value
   * @return the mapped value, or empty if the block is unknown or its world state is unavailable
   *     (for example pruned) or cannot be opened
   * @throws NullPointerException if {@code blockHash} or {@code mapper} is null
   */
  default <T> Optional<T> mapWorldView(
      final Hash blockHash, final Function<? super WorldView, ? extends Optional<T>> mapper) {
    throw new UnsupportedOperationException();
  }
}
