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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2BytecodeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2StorageRangeRequest;
import org.hyperledger.besu.services.tasks.InMemoryTaskQueue;

import java.util.Set;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SnapV2WorldDownloadState#purgeChildRequestsForAccounts}: queued storage and code
 * requests of accounts deleted during reorg recovery are dropped and their child-request counts
 * settled, so the affected account ranges can still complete.
 */
class SnapV2ReorgQueuePurgeTest {

  private static final BlockHeader PIVOT = new BlockHeaderTestFixture().buildHeader();

  private static final Bytes32 RANGE_A_START = Bytes32.ZERO;
  private static final Bytes32 RANGE_A_END =
      Bytes32.fromHexString("0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
  private static final Bytes32 RANGE_B_START =
      Bytes32.fromHexString("0x8000000000000000000000000000000000000000000000000000000000000000");
  private static final Bytes32 RANGE_B_END =
      Bytes32.fromHexString("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

  private static final Bytes32 DELETED_ACCOUNT =
      Bytes32.fromHexString("0x1111111111111111111111111111111111111111111111111111111111111111");
  private static final Bytes32 SURVIVING_ACCOUNT =
      Bytes32.fromHexString("0x2222222222222222222222222222222222222222222222222222222222222222");
  private static final Bytes32 DELETED_ACCOUNT_B =
      Bytes32.fromHexString("0x9111111111111111111111111111111111111111111111111111111111111111");

  @Test
  void dropsQueuedStorageRequestsOfDeletedAccountsAndSettlesChildCounts() {
    final DownloadedAccountRangeTracker accountTracker = new DownloadedAccountRangeTracker();
    accountTracker.registerPending(RANGE_A_START, RANGE_A_END, 3);
    accountTracker.registerPending(RANGE_B_START, RANGE_B_END, 1);

    final InMemoryTaskQueue<SnapDataRequest> storageQueue = new InMemoryTaskQueue<>();
    final InMemoryTaskQueue<SnapDataRequest> largeStorageQueue = new InMemoryTaskQueue<>();
    final InMemoryTaskQueue<SnapDataRequest> codeQueue = new InMemoryTaskQueue<>();

    final SnapV2StorageRangeRequest deletedInRangeA =
        storageRequest(DELETED_ACCOUNT, RANGE_A_START);
    final SnapV2StorageRangeRequest survivingInRangeA =
        storageRequest(SURVIVING_ACCOUNT, RANGE_A_START);
    final SnapV2StorageRangeRequest deletedLargeInRangeA =
        storageRequest(DELETED_ACCOUNT, RANGE_A_START);
    final SnapV2StorageRangeRequest deletedInRangeB =
        storageRequest(DELETED_ACCOUNT_B, RANGE_B_START);

    storageQueue.add(deletedInRangeA);
    storageQueue.add(survivingInRangeA);
    largeStorageQueue.add(deletedLargeInRangeA);
    storageQueue.add(deletedInRangeB);

    final int purged =
        SnapV2WorldDownloadState.purgeChildRequestsForAccounts(
            storageQueue,
            largeStorageQueue,
            codeQueue,
            accountTracker,
            Set.of(Hash.wrap(DELETED_ACCOUNT), Hash.wrap(DELETED_ACCOUNT_B)));

    assertThat(purged).isEqualTo(3);
    assertThat(storageQueue.asList()).containsExactly(survivingInRangeA);
    assertThat(largeStorageQueue.asList()).isEmpty();
    assertThat(codeQueue.asList()).isEmpty();

    // Range A had 3 children: 2 purged. Range B had 1 child: purged.
    assertThat(accountTracker.isAccountHashPending(DELETED_ACCOUNT)).isTrue();
    assertThat(accountTracker.isAccountHashPending(RANGE_B_START)).isFalse();
    assertThat(accountTracker.pendingRangeCount()).isEqualTo(1);
    assertThat(accountTracker.completedRangeCount()).isEqualTo(1);

    // The surviving request still completes normally.
    accountTracker.onChildCompleted(RANGE_A_START);
    assertThat(accountTracker.pendingRangeCount()).isZero();
    assertThat(accountTracker.completedRangeCount()).isEqualTo(2);
  }

  @Test
  void noDeletedAccountsLeavesQueuesUntouched() {
    final DownloadedAccountRangeTracker accountTracker = new DownloadedAccountRangeTracker();
    accountTracker.registerPending(RANGE_A_START, RANGE_A_END, 1);

    final InMemoryTaskQueue<SnapDataRequest> storageQueue = new InMemoryTaskQueue<>();
    final SnapV2StorageRangeRequest request = storageRequest(SURVIVING_ACCOUNT, RANGE_A_START);
    storageQueue.add(request);

    final int purged =
        SnapV2WorldDownloadState.purgeChildRequestsForAccounts(
            storageQueue,
            new InMemoryTaskQueue<>(),
            new InMemoryTaskQueue<>(),
            accountTracker,
            Set.of());

    assertThat(purged).isZero();
    assertThat(storageQueue.asList()).containsExactly(request);
    assertThat(accountTracker.pendingRangeCount()).isEqualTo(1);
  }

  @Test
  void dropsQueuedCodeRequestsOfDeletedAccountsAndSettlesChildCounts() {
    final DownloadedAccountRangeTracker accountTracker = new DownloadedAccountRangeTracker();
    accountTracker.registerPending(RANGE_A_START, RANGE_A_END, 2);

    final InMemoryTaskQueue<SnapDataRequest> codeQueue = new InMemoryTaskQueue<>();
    final SnapV2BytecodeRequest deletedCodeInRangeA = codeRequest(DELETED_ACCOUNT, RANGE_A_START);
    final SnapV2BytecodeRequest survivingCodeInRangeA =
        codeRequest(SURVIVING_ACCOUNT, RANGE_A_START);
    codeQueue.add(deletedCodeInRangeA);
    codeQueue.add(survivingCodeInRangeA);

    final int purged =
        SnapV2WorldDownloadState.purgeChildRequestsForAccounts(
            new InMemoryTaskQueue<>(),
            new InMemoryTaskQueue<>(),
            codeQueue,
            accountTracker,
            Set.of(Hash.wrap(DELETED_ACCOUNT)));

    assertThat(purged).isEqualTo(1);
    assertThat(codeQueue.asList()).containsExactly(survivingCodeInRangeA);

    // Range A had 2 children: the deleted code request settled one, one remains.
    assertThat(accountTracker.pendingRangeCount()).isEqualTo(1);
    assertThat(accountTracker.completedRangeCount()).isZero();

    // The surviving code request still completes normally.
    accountTracker.onChildCompleted(RANGE_A_START);
    assertThat(accountTracker.pendingRangeCount()).isZero();
    assertThat(accountTracker.completedRangeCount()).isEqualTo(1);
  }

  private static SnapV2StorageRangeRequest storageRequest(
      final Bytes32 accountHash, final Bytes32 rangeStart) {
    return new SnapV2StorageRangeRequest(
        PIVOT, accountHash, Bytes32.ZERO, Bytes32.ZERO, Bytes32.ZERO, rangeStart);
  }

  private static SnapV2BytecodeRequest codeRequest(
      final Bytes32 accountHash, final Bytes32 rangeStart) {
    return new SnapV2BytecodeRequest(PIVOT, accountHash, Bytes32.ZERO, rangeStart);
  }
}
