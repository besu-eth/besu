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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.context;

import org.hyperledger.besu.ethereum.eth.sync.backwardsync.GenericKeyValueStorageFacade;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.ValueConvertor;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.AccountRangeDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;

/**
 * Manages the persistence of the SnapSync state, allowing it to be saved and retrieved from the
 * database. The SnapSync state includes the current progress, downloaded data, and other relevant
 * information needed to resume SnapSync from where it left off after a client restart.
 */
public class SnapSyncStatePersistenceManager {

  private final byte[] SNAP_ACCOUNT_HEALING_LIST_INDEX =
      "snapInconsistentAccountsStorageIndex".getBytes(StandardCharsets.UTF_8);

  private final GenericKeyValueStorageFacade<BigInteger, AccountRangeDataRequest>
      accountRangeToDownload;
  private final GenericKeyValueStorageFacade<BigInteger, Bytes> healContext;

  public SnapSyncStatePersistenceManager(final StorageProvider storageProvider) {
    this.accountRangeToDownload =
        new GenericKeyValueStorageFacade<>(
            BigInteger::toByteArray,
            new ValueConvertor<>() {
              @Override
              public AccountRangeDataRequest fromBytes(final byte[] bytes) {
                return AccountRangeDataRequest.deserialize(
                    new BytesValueRLPInput(Bytes.of(bytes), false));
              }

              @Override
              public byte[] toBytes(final AccountRangeDataRequest value) {
                return value.serialize().toArrayUnsafe();
              }
            },
            storageProvider.getStorageBySegmentIdentifier(
                KeyValueSegmentIdentifier.SNAPSYNC_MISSING_ACCOUNT_RANGE));
    this.healContext =
        new GenericKeyValueStorageFacade<>(
            BigInteger::toByteArray,
            new ValueConvertor<>() {
              @Override
              public Bytes fromBytes(final byte[] bytes) {
                return Bytes.of(bytes);
              }

              @Override
              public byte[] toBytes(final Bytes value) {
                return value.toArrayUnsafe();
              }
            },
            storageProvider.getStorageBySegmentIdentifier(
                KeyValueSegmentIdentifier.SNAPSYNC_ACCOUNT_TO_FIX));
  }

  /**
   * Persists the current account range tasks to the database.
   *
   * @param accountRangeDataRequests The current account range tasks to persist.
   */
  public void updatePersistedTasks(final List<? extends SnapDataRequest> accountRangeDataRequests) {
    accountRangeToDownload.clear();
    accountRangeToDownload.putAll(
        keyValueStorageTransaction ->
            IntStream.range(0, accountRangeDataRequests.size())
                .forEach(
                    index ->
                        keyValueStorageTransaction.put(
                            BigInteger.valueOf(index).toByteArray(),
                            ((AccountRangeDataRequest) accountRangeDataRequests.get(index))
                                .serialize()
                                .toArrayUnsafe())));
  }

  /**
   * Persists the current accounts to heal in the database.
   *
   * @param accountsHealingList The current list of accounts to heal.
   */
  public void addAccountToHealingList(final Bytes accountsHealingList) {
    final BigInteger index =
        healContext
            .get(SNAP_ACCOUNT_HEALING_LIST_INDEX)
            .map(bytes -> new BigInteger(bytes.toArrayUnsafe()).add(BigInteger.ONE))
            .orElse(BigInteger.ZERO);
    healContext.putAll(
        keyValueStorageTransaction -> {
          keyValueStorageTransaction.put(SNAP_ACCOUNT_HEALING_LIST_INDEX, index.toByteArray());
          keyValueStorageTransaction.put(index.toByteArray(), accountsHealingList.toArrayUnsafe());
        });
  }

  /**
   * Returns the saved account range tasks, each range only once.
   *
   * @return the saved account range tasks without duplicate ranges
   */
  public List<AccountRangeDataRequest> getCurrentAccountRange() {
    final Set<Bytes> seenRanges = new HashSet<>();
    return accountRangeToDownload
        .streamValuesFromKeysThat(bytes -> true)
        .filter(
            request ->
                seenRanges.add(
                    Bytes.concatenate(request.getStartKeyHash(), request.getEndKeyHash())))
        .collect(Collectors.toList());
  }

  public Set<Bytes> getAccountsHealingList() {
    return healContext
        .streamValuesFromKeysThat(notEqualsTo(SNAP_ACCOUNT_HEALING_LIST_INDEX))
        .collect(Collectors.toCollection(HashSet::new));
  }

  public void clearAccountRangeTasks() {
    accountRangeToDownload.clear();
  }

  public void clear() {
    accountRangeToDownload.clear();
    healContext.clear();
  }

  public void close() throws IOException {
    accountRangeToDownload.close();
    healContext.close();
  }

  private Predicate<byte[]> notEqualsTo(final byte[] name) {
    return key -> !Arrays.equals(key, name);
  }
}
