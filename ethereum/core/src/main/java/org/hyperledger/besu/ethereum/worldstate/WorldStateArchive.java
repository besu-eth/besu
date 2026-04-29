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
package org.hyperledger.besu.ethereum.worldstate;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.evm.worldstate.WorldState;
import org.hyperledger.besu.plugin.data.BlockHeader;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

public interface WorldStateArchive extends Closeable {
  Optional<WorldState> get(Hash rootHash, Hash blockHash);

  boolean isWorldStateAvailable(Hash rootHash, Hash blockHash);

  /**
   * Gets a mutable world state based on the provided query parameters.
   *
   * <p>This method retrieves the mutable world state using the provided query parameters. The query
   * parameters can specify various conditions and filters to determine the specific world state to
   * be retrieved.
   *
   * @param worldStateQueryParams the query parameters
   * @return the mutable world state, if available
   */
  Optional<MutableWorldState> getWorldState(WorldStateQueryParams worldStateQueryParams);

  /**
   * Gets the head world state.
   *
   * <p>This method returns the head world state, which is the most recent state of the world.
   *
   * @return the head world state
   */
  MutableWorldState getWorldState();

  /**
   * Resetting the archive cache and adding the new pivot as the only entry
   *
   * @param blockHeader new pivot block header
   */
  void resetArchiveStateTo(BlockHeader blockHeader);

  /**
   * Retrieves an account proof based on the provided parameters.
   *
   * @param blockHeader The header of the block for which to retrieve the account proof.
   * @param accountAddress The address of the account for which to retrieve the proof.
   * @param accountStorageKeys The storage keys of the account for which to retrieve the proof.
   * @param mapper A function to map the retrieved WorldStateProof to a desired type.
   * @return An Optional containing the mapped result if the account proof is successfully retrieved
   *     and mapped, or an empty Optional otherwise.
   */
  <U> Optional<U> getAccountProof(
      final BlockHeader blockHeader,
      final Address accountAddress,
      final List<UInt256> accountStorageKeys,
      final Function<Optional<WorldStateProof>, ? extends Optional<U>> mapper);

  /**
   * Heal the world state to fix inconsistency
   *
   * @param maybeAccountToRepair the optional account to repair
   * @param location the location of the inconsistency
   */
  void heal(Optional<Address> maybeAccountToRepair, Bytes location);

  /** A world state healer */
  @FunctionalInterface
  interface WorldStateHealer {
    /**
     * Heal the world state to fix inconsistency
     *
     * @param maybeAccountToRepair the optional account to repair
     * @param location the location of the inconsistency
     */
    void heal(Optional<Address> maybeAccountToRepair, Bytes location);
  }
}
