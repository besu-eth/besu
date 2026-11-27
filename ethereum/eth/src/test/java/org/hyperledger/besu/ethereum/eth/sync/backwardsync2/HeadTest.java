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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

class HeadTest {

  // block 0 the chain holds, blocks 1 to 4 above it
  private final List<Block> blocks = new BlockDataGenerator(1).blockSequence(5);
  private final Map<Hash, Head> payloads = new HashMap<>();

  @Test
  void findsTheParentsThatCameInNewPayloadsDownToTheChain() {
    payloads(1, 2, 3, 4);

    assertThat(payloadParents(head(4))).containsExactly(head(3), head(2), head(1));
  }

  @Test
  void stopsAtAParentThatDidNotComeInANewPayload() {
    payloads(1, 3, 4);

    assertThat(payloadParents(head(4))).containsExactly(head(3));
  }

  @Test
  void aHeadSentByHashHasNoKnownParent() {
    payloads(1, 2, 3, 4);

    assertThat(payloadParents(Head.ofHash(blocks.get(4).getHash()))).isEmpty();
  }

  private void payloads(final int... numbers) {
    for (final int number : numbers) {
      payloads.put(blocks.get(number).getHash(), head(number));
    }
  }

  private Head head(final int number) {
    return Head.ofPayload(blocks.get(number), Optional.empty());
  }

  private List<Head> payloadParents(final Head head) {
    return head.payloadParents(
        hash -> Optional.ofNullable(payloads.get(hash)),
        Set.of(blocks.getFirst().getHash())::contains);
  }
}
