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

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Recovers the senders of the transactions of a batch on the computation threads, one block per
 * task, while the batch before is imported. The execution then finds them already known. A block
 * downloaded with its receipts is skipped: it is imported from its block access list, which needs
 * no sender.
 */
class RecoverSendersStep implements Function<BlockBatch, CompletableFuture<BlockBatch>> {

  private final EthScheduler scheduler;

  RecoverSendersStep(final EthScheduler scheduler) {
    this.scheduler = scheduler;
  }

  @Override
  public CompletableFuture<BlockBatch> apply(final BlockBatch batch) {
    final CompletableFuture<?>[] recoveries =
        batch.blocks().stream()
            .filter(block -> !batch.receipts().containsKey(block.getHash()))
            .map(block -> scheduler.scheduleComputationTask(() -> recoverSenders(block)))
            .toArray(CompletableFuture[]::new);
    return CompletableFuture.allOf(recoveries).thenApply(recovered -> batch);
  }

  private static Block recoverSenders(final Block block) {
    // the sender is kept by the transaction once recovered
    block.getBody().getTransactions().forEach(Transaction::getSender);
    return block;
  }
}
