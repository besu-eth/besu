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
package org.hyperledger.besu.ethereum.trie.pathbased.common.code;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.Code;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class PathBasedCodeCacheTest {

  private PathBasedCodeCache codeCache;

  @BeforeEach
  void setUp() {
    codeCache = new PathBasedCodeCache();
  }

  @Test
  void shouldReturnNullWhenCodeIsNotPresent() {
    Hash missingHash = Hash.hash(Bytes.of(1, 2, 3));

    Code result = codeCache.getIfPresent(missingHash);

    assertThat(result).isNull();
  }

  @Test
  void shouldReturnCodeAfterItIsPut() {
    Hash codeHash = Hash.hash(Bytes.of(10, 20, 30));
    Code code = Mockito.mock(Code.class);

    codeCache.put(codeHash, code);

    Code retrieved = codeCache.getIfPresent(codeHash);

    assertThat(retrieved).isSameAs(code);
  }

  @Test
  void shouldOverwriteExistingCodeForSameHash() {
    Hash codeHash = Hash.hash(Bytes.of(1, 2, 3));
    Code originalCode = Mockito.mock(Code.class);
    Code newCode = Mockito.mock(Code.class);

    codeCache.put(codeHash, originalCode);
    codeCache.put(codeHash, newCode);

    Code retrieved = codeCache.getIfPresent(codeHash);

    assertThat(retrieved).isSameAs(newCode);
  }
}
