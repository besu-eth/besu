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

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.core.Utils;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class RecoverSendersStepTest {

  private final BlockDataGenerator generator = new BlockDataGenerator();

  @Test
  void recoversTheSenderOfEveryTransaction() {
    final Block generated = generator.nextBlock(generator.genesisBlock());
    final Block decoded = decode(generated);
    final List<Transaction> transactions = decoded.getBody().getTransactions();
    assertThat(transactions).isNotEmpty().allMatch(tx -> tx.getSenderIfKnown().isEmpty());

    final BlockBatch batch = new BlockBatch(List.of(decoded), Map.of(), Map.of());
    final BlockBatch recovered =
        new RecoverSendersStep(new DeterministicEthScheduler()).apply(batch).join();

    assertThat(recovered).isSameAs(batch);
    assertThat(transactions)
        .map(tx -> tx.getSenderIfKnown().orElseThrow())
        .containsExactlyElementsOf(
            generated.getBody().getTransactions().stream().map(Transaction::getSender).toList());
  }

  @Test
  void recoversTheSendersOfABlockImportedFromItsBal() {
    final Block decoded = decode(generator.nextBlock(generator.genesisBlock()));
    final BlockBatch batch =
        new BlockBatch(List.of(decoded), Map.of(), Map.of(decoded.getHash(), List.of()));

    new RecoverSendersStep(new DeterministicEthScheduler()).apply(batch).join();

    assertThat(decoded.getBody().getTransactions())
        .isNotEmpty()
        .allMatch(tx -> tx.getSenderIfKnown().isPresent());
  }

  // a decoded body has transactions whose senders are not recovered yet
  private static Block decode(final Block block) {
    final SyncBlock syncBlock = Utils.blockToSyncBlock(block);
    return new Block(block.getHeader(), syncBlock.getBody().getBodySupplier().get());
  }
}
