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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;

/**
 * A cache for jump destination only code.
 *
 * <p>This cache is used to store code that is only used for jump destinations, which can be large
 * and expensive to compute. The cache is limited by weight, not size, to allow for larger code
 * without exceeding memory limits.
 */
public class JumpDestOnlyCodeCache {
  static class CodeScale implements Weigher<Hash, Code> {
    @Override
    public int weigh(final Hash key, final Code code) {
      return ((code.getSize() * 9 + 7) / 8) + key.getBytes().size();
    }
  }

  private final Cache<Hash, Code> cache;

  /**
   * Instantiates a new Code cache.
   *
   * @param config the config
   */
  public JumpDestOnlyCodeCache(final EvmConfiguration config) {
    final long maxWeightBytes = config.getJumpDestCacheWeightBytes();
    this.cache =
        Caffeine.newBuilder().maximumWeight(maxWeightBytes).weigher(new CodeScale()).build();
  }

  /**
   * Gets if present.
   *
   * @param codeHash the code hash
   * @return if present, null otherwise
   */
  public Code getIfPresent(final Hash codeHash) {
    return cache.getIfPresent(codeHash);
  }

  /**
   * Put.
   *
   * <p>Empty code under a non-empty hash is never cached. Such an entry means the code could not be
   * read, and the cache is shared by every execution, so caching it would make all later calls to
   * that code run empty code.
   *
   * @param key the key
   * @param value the value
   */
  public void put(final Hash key, final Code value) {
    if (value.getSize() == 0 && !Hash.EMPTY.equals(key)) {
      return;
    }
    cache.put(key, value);
  }
}
