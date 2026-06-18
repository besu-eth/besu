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
package org.hyperledger.besu.ethereum.trie.pathbased.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class PathBasedLazyTest {

  @Test
  void getPriorLoadsOnce() {
    final AtomicInteger priorLoads = new AtomicInteger();
    final PathBasedLazy<String> lazy =
        new PathBasedLazy<>(
            () -> {
              priorLoads.incrementAndGet();
              return "prior";
            },
            () -> "updated");

    assertThat(lazy.getPrior()).isEqualTo("prior");
    assertThat(lazy.getPrior()).isEqualTo("prior");
    assertThat(priorLoads).hasValue(1);
  }

  @Test
  void getUpdatedLoadsOnce() {
    final AtomicInteger updatedLoads = new AtomicInteger();
    final PathBasedLazy<String> lazy =
        new PathBasedLazy<>(
            () -> "prior",
            () -> {
              updatedLoads.incrementAndGet();
              return "updated";
            });

    assertThat(lazy.getUpdated()).isEqualTo("updated");
    assertThat(lazy.getUpdated()).isEqualTo("updated");
    assertThat(updatedLoads).hasValue(1);
  }

  @Test
  void priorKnownAbsentSkipsLoader() {
    final AtomicInteger priorLoads = new AtomicInteger();
    final PathBasedLazy<String> lazy =
        PathBasedLazy.withPriorKnownAbsent(
            () -> {
              priorLoads.incrementAndGet();
              return "updated";
            });

    assertThat(lazy.isPriorKnownAbsent()).isTrue();
    assertThat(lazy.getPrior()).isNull();
    assertThat(lazy.getPrior()).isNull();
    assertThat(priorLoads).hasValue(0);
    assertThat(lazy.getUpdated()).isEqualTo("updated");
  }

  @Test
  void materializeResolvesBothSides() {
    final PathBasedLazy<String> lazy = PathBasedLazy.withPriorKnownAbsent(() -> "updated");
    final PathBasedValue<String> materialized = lazy.materialize();

    assertThat(materialized.getPrior()).isNull();
    assertThat(materialized.getUpdated()).isEqualTo("updated");
    assertThat(materialized).isNotInstanceOf(PathBasedLazy.class);
  }
}
