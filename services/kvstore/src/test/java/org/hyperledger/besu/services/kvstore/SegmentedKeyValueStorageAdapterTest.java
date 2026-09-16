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
package org.hyperledger.besu.services.kvstore;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageAdapter.KeyValueStorageTransactionAdapter;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;

public class SegmentedKeyValueStorageAdapterTest {

  private static final byte[] KEY = {1};
  private static final byte[] VALUE = {2};

  private final SegmentedInMemoryKeyValueStorage sharedStorage =
      new SegmentedInMemoryKeyValueStorage(List.of(TestSegment.FIRST, TestSegment.SECOND));
  private final SegmentedKeyValueStorageAdapter first =
      new SegmentedKeyValueStorageAdapter(TestSegment.FIRST, sharedStorage);
  private final SegmentedKeyValueStorageAdapter second =
      new SegmentedKeyValueStorageAdapter(TestSegment.SECOND, sharedStorage);

  @Test
  public void joinedTransactionIsCommittedWithTheTransactionItJoined() {
    final KeyValueStorageTransactionAdapter transaction = startTransaction(first);
    transaction.put(KEY, VALUE);
    transaction.joinedTransactionFor(second).orElseThrow().put(KEY, VALUE);
    assertThat(second.containsKey(KEY)).isFalse();

    transaction.commit();

    assertThat(first.get(KEY).orElseThrow()).isEqualTo(VALUE);
    assertThat(second.get(KEY).orElseThrow()).isEqualTo(VALUE);
  }

  @Test
  public void joinedTransactionIsRolledBackWithTheTransactionItJoined() {
    final KeyValueStorageTransactionAdapter transaction = startTransaction(first);
    transaction.put(KEY, VALUE);
    transaction.joinedTransactionFor(second).orElseThrow().put(KEY, VALUE);

    transaction.rollback();

    assertThat(first.containsKey(KEY)).isFalse();
    assertThat(second.containsKey(KEY)).isFalse();
  }

  @Test
  public void noJoinedTransactionForAStorageOnAnotherSegmentedStorage() {
    final KeyValueStorageTransactionAdapter transaction = startTransaction(first);

    assertThat(transaction.joinedTransactionFor(new InMemoryKeyValueStorage())).isEmpty();
  }

  private static KeyValueStorageTransactionAdapter startTransaction(
      final SegmentedKeyValueStorageAdapter storage) {
    final KeyValueStorageTransaction transaction = storage.startTransaction();
    assertThat(transaction).isInstanceOf(KeyValueStorageTransactionAdapter.class);
    return (KeyValueStorageTransactionAdapter) transaction;
  }

  private enum TestSegment implements SegmentIdentifier {
    FIRST(new byte[] {1}),
    SECOND(new byte[] {2});

    private final byte[] id;

    TestSegment(final byte[] id) {
      this.id = id;
    }

    @Override
    public String getName() {
      return new String(id, StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getId() {
      return id;
    }

    @Override
    public boolean containsStaticData() {
      return false;
    }

    @Override
    public boolean isEligibleToHighSpecFlag() {
      return false;
    }
  }
}
