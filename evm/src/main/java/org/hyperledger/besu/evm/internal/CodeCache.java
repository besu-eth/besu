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
package org.hyperledger.besu.evm.internal;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.Code;

/**
 * CodeCache is an interface for caching bytecode, its size and jump dest analysis. It allows
 * retrieval and storage of code based on its hash.
 */
public interface CodeCache {
  /**
   * Gets the code if present in the cache.
   *
   * @param codeHash the hash of the code to retrieve
   * @return the code if present, otherwise null
   */
  Code getIfPresent(final Hash codeHash);

  /**
   * Puts the code into the cache.
   *
   * <p>An implementation may refuse an entry that cannot be valid, such as empty code under a
   * non-empty hash, so the code is not guaranteed to be present afterwards.
   *
   * @param codeHash the hash of the code to store
   * @param code the code to store
   */
  void put(final Hash codeHash, final Code code);
}
