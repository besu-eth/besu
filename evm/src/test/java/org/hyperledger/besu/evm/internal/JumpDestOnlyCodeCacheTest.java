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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.Code;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JumpDestOnlyCodeCacheTest {

  private JumpDestOnlyCodeCache codeCache;

  @BeforeEach
  void setUp() {
    codeCache = new JumpDestOnlyCodeCache(EvmConfiguration.DEFAULT);
  }

  @Test
  void shouldReturnCodeAfterItIsPut() {
    Bytes bytes = Bytes.of(10, 20, 30);
    Hash codeHash = Hash.hash(bytes);
    Code code = new Code(bytes);

    codeCache.put(codeHash, code);

    assertThat(codeCache.getIfPresent(codeHash)).isSameAs(code);
  }

  @Test
  void shouldNotCacheEmptyCodeUnderNonEmptyHash() {
    Hash codeHash = Hash.hash(Bytes.of(1, 2, 3));

    codeCache.put(codeHash, new Code(Bytes.EMPTY));

    assertThat(codeCache.getIfPresent(codeHash)).isNull();
  }

  @Test
  void shouldCacheEmptyCodeUnderEmptyHash() {
    codeCache.put(Hash.EMPTY, Code.EMPTY_CODE);

    assertThat(codeCache.getIfPresent(Hash.EMPTY)).isSameAs(Code.EMPTY_CODE);
  }
}
