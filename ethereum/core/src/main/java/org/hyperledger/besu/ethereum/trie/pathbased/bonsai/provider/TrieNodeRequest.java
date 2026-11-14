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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider;

import static java.util.Objects.requireNonNull;

import org.hyperledger.besu.datatypes.Hash;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * One hash-verified trie dependency, anchored to the state that proves its location.
 *
 * @param stateRoot root of the proving world state
 * @param accountHash owning account for a storage node; empty for an account trie node
 * @param location node's nibble path within its trie
 * @param expectedNodeHash hash committed by the proving trie
 */
public record TrieNodeRequest(
    Hash stateRoot, Optional<Hash> accountHash, Bytes location, Hash expectedNodeHash) {
  public TrieNodeRequest(
      final Hash stateRoot,
      final Optional<Hash> accountHash,
      final Bytes location,
      final Hash expectedNodeHash) {
    this.stateRoot = requireNonNull(stateRoot);
    this.accountHash = requireNonNull(accountHash);
    this.location = requireNonNull(location).copy();
    this.expectedNodeHash = requireNonNull(expectedNodeHash);
  }

  @Override
  public Bytes location() {
    return location.copy();
  }
}
