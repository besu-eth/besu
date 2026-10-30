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

import org.hyperledger.besu.datatypes.Hash;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/** Fetches a code by its hash from outside the local storage, e.g. from peers. */
@FunctionalInterface
public interface CodeFetcher {

  /**
   * Fetches a code by its hash, waiting a bounded time.
   *
   * @param codeHash the code hash
   * @return the code, or empty when it could not be fetched
   */
  Optional<Bytes> fetch(Hash codeHash);
}
