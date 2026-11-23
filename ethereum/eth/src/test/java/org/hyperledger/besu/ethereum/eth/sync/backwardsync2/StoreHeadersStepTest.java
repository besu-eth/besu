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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class StoreHeadersStepTest {

  private final HeaderStore headerStore =
      new HeaderStore(new InMemoryKeyValueStorage(), new MainnetBlockHeaderFunctions());
  // chain.get(n) is block #n
  private final List<BlockHeader> chain = chain(10);

  @Test
  void storesEachBatchThatIsTheParentChainOfTheHeadersAbove() {
    final StoreHeadersStep step = step(Hash.ZERO);

    step.accept(descending(9, 5));
    step.accept(descending(4, 1));

    assertThat(headerStore.getRange(1, 9)).isEqualTo(chain.subList(1, 10));
    assertThat(step.lowest()).isEqualTo(chain.get(1));
  }

  @Test
  void rejectsABatchThatIsNotTheParentChain() {
    final StoreHeadersStep step = step(Hash.ZERO);
    step.accept(descending(9, 5));

    assertThatThrownBy(() -> step.accept(descending(3, 1)))
        .isInstanceOf(IllegalStateException.class);
    assertThat(headerStore.get(3)).isEmpty();
    assertThat(step.lowest()).isEqualTo(chain.get(5));
  }

  @Test
  void findsTheFinalizedBlockAmongTheHeaders() {
    final StoreHeadersStep step = step(chain.get(6).getHash());

    step.accept(descending(9, 5));
    step.accept(descending(4, 1));

    assertThat(step.finalizedNumber()).isEqualTo(6);
  }

  @Test
  void hasNoFinalizedBlockWhenItIsNotAmongTheHeaders() {
    final StoreHeadersStep step = step(chain.get(0).getHash());

    step.accept(descending(9, 1));

    assertThat(step.finalizedNumber()).isEqualTo(-1);
  }

  @Test
  void clearRemovesTheStoredHeaders() {
    headerStore.putAll(chain);

    headerStore.clear();

    assertThat(headerStore.get(5)).isEmpty();
  }

  private StoreHeadersStep step(final Hash finalizedBlockHash) {
    return new StoreHeadersStep(headerStore, chain.get(10), () -> finalizedBlockHash);
  }

  private List<BlockHeader> descending(final int from, final int to) {
    return new ArrayList<>(chain.subList(to, from + 1)).reversed();
  }

  private static List<BlockHeader> chain(final int head) {
    final List<BlockHeader> headers = new ArrayList<>();
    headers.add(new BlockHeaderTestFixture().number(0).buildHeader());
    for (int number = 1; number <= head; number++) {
      headers.add(
          new BlockHeaderTestFixture()
              .number(number)
              .parentHash(headers.getLast().getHash())
              .buildHeader());
    }
    return headers;
  }
}
