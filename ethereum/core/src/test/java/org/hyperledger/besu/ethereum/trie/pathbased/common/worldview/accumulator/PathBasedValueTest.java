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
package org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class PathBasedValueTest {

  @Test
  void lazyPriorDefersLoadUntilAccessed() {
    final AtomicInteger loads = new AtomicInteger();
    final PathBasedValue<UInt256> value =
        PathBasedValue.withLazyPrior(
            () -> {
              loads.incrementAndGet();
              return UInt256.valueOf(42);
            });

    assertThat(loads).hasValue(0);
    assertThat(value.getUpdated()).isEqualTo(UInt256.valueOf(42));
    assertThat(loads).hasValue(1);
    assertThat(value.getUpdated()).isEqualTo(UInt256.valueOf(42));
    assertThat(loads).hasValue(1);
  }

  @Test
  void explicitUpdatedSkipsPriorLoadOnGetUpdated() {
    final AtomicInteger loads = new AtomicInteger();
    final PathBasedValue<UInt256> value =
        PathBasedValue.withLazyPrior(
            () -> {
              loads.incrementAndGet();
              return UInt256.valueOf(42);
            });

    value.setUpdated(UInt256.valueOf(99));

    assertThat(value.getUpdated()).isEqualTo(UInt256.valueOf(99));
    assertThat(loads).hasValue(0);
  }

  @Test
  void getPriorMaterializesLazyPrior() {
    final AtomicInteger loads = new AtomicInteger();
    final PathBasedValue<UInt256> value =
        PathBasedValue.withLazyPrior(
            () -> {
              loads.incrementAndGet();
              return UInt256.valueOf(42);
            });

    value.setUpdated(UInt256.valueOf(99));
    assertThat(loads).hasValue(0);

    assertThat(value.getPrior()).isEqualTo(UInt256.valueOf(42));
    assertThat(loads).hasValue(1);
  }
}
