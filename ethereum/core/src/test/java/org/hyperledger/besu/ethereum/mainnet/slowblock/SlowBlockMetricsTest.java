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
package org.hyperledger.besu.ethereum.mainnet.slowblock;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;

import java.util.List;

import org.junit.jupiter.api.Test;

class SlowBlockMetricsTest {

  private static final Address CONTRACT_A =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");
  private static final Address CONTRACT_B =
      Address.fromHexString("0x00000000000000000000000000000000000000bb");

  @Test
  void cacheHitsAreTotalReadsMinusBoundaryCrossingReads() {
    final SlowBlockMetrics metrics = new SlowBlockMetrics(0);
    metrics.addStateReads(100, 500, 7, 1_600);
    metrics.setCacheMisses(30, 200, 2);

    assertThat(metrics.hitAccounts()).isEqualTo(70);
    assertThat(metrics.hitStorageSlots()).isEqualTo(300);
    assertThat(metrics.hitCode()).isEqualTo(5);
  }

  @Test
  void derivedCacheHitsAreClampedAtZeroWhenInstrumentationDrifts() {
    final SlowBlockMetrics metrics = new SlowBlockMetrics(0);
    metrics.addStateReads(3, 0, 0, 0);
    metrics.setCacheMisses(10, 4, 1);

    assertThat(metrics.hitAccounts()).isZero();
    assertThat(metrics.hitStorageSlots()).isZero();
    assertThat(metrics.hitCode()).isZero();
  }

  @Test
  void thresholdOfZeroLogsEveryBlock() {
    final SlowBlockMetrics metrics = new SlowBlockMetrics(0);
    metrics.startBlock(1, "0xdeadbeef", 21_000, 1);
    metrics.endBlock(0L);

    assertThat(metrics.meetsThreshold()).isTrue();
  }

  @Test
  void blocksAtOrOverThePositiveThresholdAreLogged() {
    final SlowBlockMetrics metrics = new SlowBlockMetrics(500);
    metrics.startBlock(1, "0xdeadbeef", 21_000, 1);
    metrics.endBlock(500_000_000L);

    assertThat(metrics.meetsThreshold()).isTrue();
  }

  @Test
  void blocksUnderTheThresholdAreNotLogged() {
    final SlowBlockMetrics metrics = new SlowBlockMetrics(500);
    metrics.startBlock(1, "0xdeadbeef", 21_000, 1);
    metrics.endBlock(499_999_999L);

    assertThat(metrics.meetsThreshold()).isFalse();
  }

  @Test
  void mergingExecutionsAccumulatesCountersAndUnionsContracts() {
    final SlowBlockMetrics metrics = new SlowBlockMetrics(0);

    recordExecution(metrics, 1, 2, 3, 4, List.of(CONTRACT_A, CONTRACT_B));

    final SlowBlockReadCounts reads = new SlowBlockReadCounts();
    reads.onAccountRead();
    reads.onStorageRead();
    reads.onStorageRead();
    reads.onCodeRead(120);
    reads.addFallThroughReadNanos(5_000);
    metrics.mergeExecution(null, reads);

    // second execution touches a contract already seen, so uniques must not double count
    recordExecution(metrics, 10, 0, 0, 0, List.of(CONTRACT_B));

    assertThat(metrics.sload()).isEqualTo(11);
    assertThat(metrics.sstore()).isEqualTo(2);
    assertThat(metrics.calls()).isEqualTo(3);
    assertThat(metrics.creates()).isEqualTo(4);
    assertThat(metrics.uniqueContracts()).isEqualTo(2);
    assertThat(metrics.readAccounts()).isEqualTo(1);
    assertThat(metrics.readStorageSlots()).isEqualTo(2);
    assertThat(metrics.readCode()).isEqualTo(1);
    assertThat(metrics.readCodeBytes()).isEqualTo(120);
    assertThat(metrics.stateReadNanos()).isEqualTo(5_000);
  }

  @Test
  void executionTimeExcludesTheStateRootWaitAndTheCommit() {
    final SlowBlockMetrics metrics = new SlowBlockMetrics(0);
    metrics.endBlock(10_000_000L);
    metrics.setStateHashWaitNanos(3_000_000L);
    metrics.setCommitNanos(1_000_000L);

    assertThat(metrics.executionNanos()).isEqualTo(6_000_000L);
  }

  @Test
  void executionTimeClampsRatherThanGoingNegative() {
    final SlowBlockMetrics metrics = new SlowBlockMetrics(0);
    metrics.endBlock(1_000_000L);
    // under BAL the phases genuinely overlap, so the parts can exceed the wall-clock total
    metrics.setStateHashWaitNanos(900_000L);
    metrics.setCommitNanos(900_000L);

    assertThat(metrics.executionNanos()).isZero();
  }

  @Test
  void nullExecutionResultsAreIgnored() {
    final SlowBlockMetrics metrics = new SlowBlockMetrics(0);
    metrics.mergeExecution(null, null);

    assertThat(metrics.sload()).isZero();
    assertThat(metrics.readAccounts()).isZero();
  }

  private void recordExecution(
      final SlowBlockMetrics metrics,
      final long sload,
      final long sstore,
      final long calls,
      final long creates,
      final List<Address> contracts) {
    metrics.addEvmCounts(sload, sstore, calls, creates);
    metrics.addContractsExecuted(contracts);
  }
}
