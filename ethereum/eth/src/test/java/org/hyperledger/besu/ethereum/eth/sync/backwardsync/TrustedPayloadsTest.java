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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockOptions;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 60, unit = TimeUnit.SECONDS)
class TrustedPayloadsTest {
  private final BlockDataGenerator gen = new BlockDataGenerator();

  private Block block(final long number) {
    return gen.block(BlockOptions.create().setBlockNumber(number));
  }

  private static BlockAccessList bal() {
    return new BlockAccessList(List.of(), Bytes.of(1, 2, 3));
  }

  @Test
  void returnsAddedBlocksAndBals() {
    final TrustedPayloads payloads = new TrustedPayloads(Long.MAX_VALUE);
    final Block withBal = block(1);
    final Block withoutBal = block(2);
    final BlockAccessList bal = bal();

    payloads.add(withBal, Optional.of(bal));
    payloads.add(withoutBal, Optional.empty());

    assertThat(payloads.block(withBal.getHash())).contains(withBal);
    assertThat(payloads.blockAccessList(withBal.getHash())).contains(bal);
    assertThat(payloads.block(withoutBal.getHash())).contains(withoutBal);
    assertThat(payloads.blockAccessList(withoutBal.getHash())).isEmpty();
  }

  @Test
  void laterAddFillsInAMissingBal() {
    final TrustedPayloads payloads = new TrustedPayloads(Long.MAX_VALUE);
    final Block block = block(1);
    final BlockAccessList bal = bal();

    payloads.add(block, Optional.empty());
    payloads.add(block, Optional.of(bal));

    assertThat(payloads.blockAccessList(block.getHash())).contains(bal);
  }

  @Test
  void laterAddWithoutBalKeepsTheKnownBal() {
    final TrustedPayloads payloads = new TrustedPayloads(Long.MAX_VALUE);
    final Block block = block(1);
    final BlockAccessList bal = bal();

    payloads.add(block, Optional.of(bal));
    payloads.add(block, Optional.empty());

    assertThat(payloads.blockAccessList(block.getHash())).contains(bal);
  }

  @Test
  void evictsTheLowestNumbersFirstWhenOverTheByteBound() {
    final Block low = block(1);
    final Block middle = block(2);
    final Block high = block(3);
    final long all = (long) low.getSize() + middle.getSize() + high.getSize();
    final TrustedPayloads payloads = new TrustedPayloads(all - 1);

    payloads.add(high, Optional.empty());
    payloads.add(low, Optional.empty());
    payloads.add(middle, Optional.empty());

    assertThat(payloads.block(low.getHash())).isEmpty();
    assertThat(payloads.block(middle.getHash())).contains(middle);
    assertThat(payloads.block(high.getHash())).contains(high);
  }

  @Test
  void balBytesCountTowardsTheBound() {
    final Block block = block(1);
    final TrustedPayloads payloads = new TrustedPayloads(block.getSize());

    payloads.add(block, Optional.of(bal()));

    assertThat(payloads.block(block.getHash())).isEmpty();
  }

  @Test
  void removeUpToDropsBlocksAtOrBelowTheNumberAndFreesTheirBytes() {
    final Block one = block(1);
    final Block two = block(2);
    final Block three = block(3);
    final Block four = block(4);
    final TrustedPayloads payloads =
        new TrustedPayloads((long) two.getSize() + three.getSize() + four.getSize());
    payloads.add(one, Optional.empty());
    payloads.add(two, Optional.empty());

    payloads.removeUpTo(2);
    payloads.add(three, Optional.empty());
    payloads.add(four, Optional.empty());

    assertThat(payloads.block(one.getHash())).isEmpty();
    assertThat(payloads.block(two.getHash())).isEmpty();
    assertThat(payloads.block(three.getHash())).contains(three);
    assertThat(payloads.block(four.getHash())).contains(four);
  }

  @Test
  void clearDropsEverything() {
    final TrustedPayloads payloads = new TrustedPayloads(Long.MAX_VALUE);
    final Block block = block(1);
    payloads.add(block, Optional.of(bal()));

    payloads.clear();

    assertThat(payloads.block(block.getHash())).isEmpty();
    assertThat(payloads.blockAccessList(block.getHash())).isEmpty();
  }
}
