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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;

import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class VersionedFlatDbCacheManagerTest {

  private VersionedFlatDbCacheManager cacheManager;

  @BeforeEach
  void setUp() {
    cacheManager = new VersionedFlatDbCacheManager(100, 100, new NoOpMetricsSystem());
  }

  @AfterEach
  void tearDown() throws Exception {
    cacheManager.close();
  }

  @Test
  void emptyFetcherResult_leavesMissesUnresolvedAndDoesNotCache() {
    final Bytes key = Bytes.of(1);
    final List<Optional<Bytes>> results =
        cacheManager.getMultipleFromCacheOrStorage(
            ACCOUNT_INFO_STATE, List.of(key), 0L, keys -> List.of());

    assertThat(results).hasSize(1);
    assertThat(results.get(0)).isNull();
    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, key)).isFalse();
  }

  @Test
  void sizeMismatchedFetcherResult_leavesMissesUnresolvedAndDoesNotCache() {
    final Bytes keyA = Bytes.of(1);
    final Bytes keyB = Bytes.of(2);
    final List<Optional<Bytes>> results =
        cacheManager.getMultipleFromCacheOrStorage(
            ACCOUNT_INFO_STATE, List.of(keyA, keyB), 0L, keys -> List.of(Optional.of(Bytes.of(9))));

    assertThat(results).containsExactly(null, null);
    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, keyA)).isFalse();
    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, keyB)).isFalse();
  }

  @Test
  void nullFetcherSlots_areSkippedAndNotCached() {
    final Bytes keyA = Bytes.of(1);
    final Bytes keyB = Bytes.of(2);
    final Bytes keyC = Bytes.of(3);
    final Bytes valueC = Bytes.of(30);

    final List<Optional<Bytes>> results =
        cacheManager.getMultipleFromCacheOrStorage(
            ACCOUNT_INFO_STATE,
            List.of(keyA, keyB, keyC),
            0L,
            keys -> {
              final List<Optional<Bytes>> fetched = new ArrayList<>(keys.size());
              fetched.add(null);
              fetched.add(Optional.empty());
              fetched.add(Optional.of(valueC));
              return fetched;
            });

    assertThat(results.get(0)).isNull();
    assertThat(results.get(1)).isEmpty();
    assertThat(results.get(2)).contains(valueC);

    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, keyA)).isFalse();
    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, keyB)).isTrue();
    assertThat(cacheManager.isCached(ACCOUNT_INFO_STATE, keyC)).isTrue();
    assertThat(cacheManager.getCachedValue(ACCOUNT_INFO_STATE, keyB))
        .hasValueSatisfying(cv -> assertThat(cv.isRemoval()).isTrue());
    assertThat(cacheManager.getCachedValue(ACCOUNT_INFO_STATE, keyC))
        .hasValueSatisfying(cv -> assertThat(cv.getValue()).isEqualTo(valueC));
  }
}
