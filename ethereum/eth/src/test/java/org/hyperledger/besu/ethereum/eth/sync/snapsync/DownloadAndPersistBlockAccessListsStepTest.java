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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.BlockchainStorage;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.SyncBlockAccessList;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DownloadAndPersistBlockAccessListsStepTest {

  @Mock private DefaultBlockchain blockchain;
  @Mock private BlockchainStorage blockchainStorage;
  @Mock private BlockchainStorage.Updater updater;

  private final List<List<BlockHeader>> requestedHeaders = new ArrayList<>();
  private DownloadAndPersistBlockAccessListsStep step;

  @BeforeEach
  void setUp() {
    when(blockchain.getBlockchainStorage()).thenReturn(blockchainStorage);
    step =
        new DownloadAndPersistBlockAccessListsStep(
            blockchain,
            Duration.ofSeconds(10),
            headers -> {
              requestedHeaders.add(headers);
              return CompletableFuture.completedFuture(
                  headers.stream()
                      .map(header -> new SyncBlockAccessList(Bytes.fromHexString("0xc0")))
                      .toList());
            });
  }

  @Test
  void downloadsOnlyTheBlockAccessListsThatAreNotStoredYet() {
    final BlockHeader stored = header(1);
    final BlockHeader missing = header(2);
    when(blockchainStorage.hasBlockAccessList(stored.getHash())).thenReturn(true);
    when(blockchainStorage.updater()).thenReturn(updater);

    final CompletableFuture<List<BlockHeader>> result = step.apply(List.of(stored, missing));

    assertThat(result).isCompletedWithValue(List.of(stored, missing));
    assertThat(requestedHeaders).containsExactly(List.of(missing));
    verify(updater).putSyncBlockAccessList(any(), any());
    verify(updater).putSyncBlockAccessList(missing.getHash(), syncBlockAccessList());
    verify(updater).commit();
  }

  @Test
  void downloadsNothingWhenAllBlockAccessListsAreStored() {
    final BlockHeader first = header(1);
    final BlockHeader second = header(2);
    when(blockchainStorage.hasBlockAccessList(any())).thenReturn(true);

    final CompletableFuture<List<BlockHeader>> result = step.apply(List.of(first, second));

    assertThat(result).isCompletedWithValue(List.of(first, second));
    assertThat(requestedHeaders).isEmpty();
    verify(blockchainStorage, never()).updater();
  }

  private static SyncBlockAccessList syncBlockAccessList() {
    return new SyncBlockAccessList(Bytes.fromHexString("0xc0"));
  }

  private static BlockHeader header(final long number) {
    return new BlockHeaderTestFixture().number(number).balHash(Hash.EMPTY).buildHeader();
  }
}
