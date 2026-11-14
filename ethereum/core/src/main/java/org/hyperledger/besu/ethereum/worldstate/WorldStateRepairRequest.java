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
package org.hyperledger.besu.ethereum.worldstate;

import static java.util.Objects.requireNonNull;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;

import java.util.Optional;

import com.google.common.base.Throwables;
import org.apache.tuweni.bytes.Bytes;
import org.jspecify.annotations.Nullable;

/** A committed repair target and, when known, the dependency that prevented its use. */
public record WorldStateRepairRequest(
    Hash blockHash, Hash stateRoot, Optional<MissingTrieNode> missingNode) {
  public WorldStateRepairRequest(
      final Hash blockHash, final Hash stateRoot, final Optional<MissingTrieNode> missingNode) {
    this.blockHash = requireNonNull(blockHash);
    this.stateRoot = requireNonNull(stateRoot);
    this.missingNode = requireNonNull(missingNode);
  }

  public WorldStateRepairRequest(final Hash blockHash, final Hash stateRoot) {
    this(blockHash, stateRoot, Optional.empty());
  }

  /** Retains only complete trie-node evidence from an eligible local-state failure. */
  public static WorldStateRepairRequest fromFailure(
      final Hash blockHash, final Hash stateRoot, @Nullable final Throwable failure) {
    if (failure != null && WorldStateFailure.isMissingState(failure)) {
      for (final Throwable cause : Throwables.getCausalChain(failure)) {
        if (cause instanceof MerkleTrieException missing
            && missing.getHash() != null
            && missing.getLocation() != null
            && missing.getMaybeAddress() != null) {
          return new WorldStateRepairRequest(
              blockHash,
              stateRoot,
              Optional.of(
                  new MissingTrieNode(
                      Hash.wrap(missing.getHash()),
                      missing.getLocation(),
                      missing.getMaybeAddress())));
        }
      }
    }
    return new WorldStateRepairRequest(blockHash, stateRoot);
  }

  /** Evidence to prove against the captured base before any peer data is admitted. */
  public record MissingTrieNode(Hash expectedHash, Bytes location, Optional<Address> address) {
    public MissingTrieNode(
        final Hash expectedHash, final Bytes location, final Optional<Address> address) {
      this.expectedHash = requireNonNull(expectedHash);
      this.location = requireNonNull(location).copy();
      this.address = requireNonNull(address);
    }

    @Override
    public Bytes location() {
      return location.copy();
    }
  }
}
