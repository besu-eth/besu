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
package org.hyperledger.besu.ethereum.eth.sync.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NewPayloadHeaderCacheTest {

  private NewPayloadHeaderCache cache;

  @BeforeEach
  void setUp() {
    cache = new NewPayloadHeaderCache();
  }

  @Test
  void putAndLookupByHashAndNumber() {
    final BlockHeader header = headerAt(100);
    cache.put(header);

    assertThat(cache.getByHash(header.getHash())).contains(header);
    assertThat(cache.getByNumber(100)).contains(header);
    assertThat(cache.size()).isEqualTo(1);
  }

  @Test
  void putSameHashTwiceIsNoop() {
    final BlockHeader header = headerAt(100);
    cache.put(header);
    cache.put(header);

    assertThat(cache.size()).isEqualTo(1);
  }

  @Test
  void putDifferentHashAtSameNumberReplacesOldEntry() {
    final BlockHeader first = headerAt(100, "0x01");
    final BlockHeader second = headerAt(100, "0x02");
    cache.put(first);
    cache.put(second);

    assertThat(cache.size()).isEqualTo(1);
    assertThat(cache.getByNumber(100)).contains(second);
    assertThat(cache.getByHash(first.getHash())).isEmpty();
    assertThat(cache.getByHash(second.getHash())).contains(second);
  }

  @Test
  void evictsOldestWhenOverCapacity() {
    for (int i = 0; i < NewPayloadHeaderCache.MAX_SIZE + 5; i++) {
      cache.put(headerAt(i));
    }
    assertThat(cache.size()).isEqualTo(NewPayloadHeaderCache.MAX_SIZE);
    assertThat(cache.getByNumber(0)).isEmpty();
    assertThat(cache.getByNumber(4)).isEmpty();
    assertThat(cache.oldest().orElseThrow().getNumber()).isEqualTo(5);
    assertThat(cache.newest().orElseThrow().getNumber())
        .isEqualTo(NewPayloadHeaderCache.MAX_SIZE + 4);
  }

  @Test
  void onNewPayloadDelegatesToPut() {
    final BlockHeader header = headerAt(42);
    cache.onNewPayload(header);
    assertThat(cache.getByHash(header.getHash())).contains(header);
  }

  @Test
  void oldestAndNewestEmptyOnFreshCache() {
    assertThat(cache.oldest()).isEmpty();
    assertThat(cache.newest()).isEmpty();
    assertThat(cache.size()).isZero();
  }

  private static BlockHeader headerAt(final long number) {
    return new BlockHeaderTestFixture().number(number).buildHeader();
  }

  private static BlockHeader headerAt(final long number, final String extraData) {
    return new BlockHeaderTestFixture()
        .number(number)
        .extraData(org.apache.tuweni.bytes.Bytes.fromHexString(extraData))
        .buildHeader();
  }
}
