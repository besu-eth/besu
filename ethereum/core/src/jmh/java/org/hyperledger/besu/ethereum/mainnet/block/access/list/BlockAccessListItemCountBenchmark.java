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
package org.hyperledger.besu.ethereum.mainnet.block.access.list;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;

import java.util.concurrent.TimeUnit;

import org.apache.tuweni.units.bigints.UInt256;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Cost of the per-transaction EIP-7928 item budget check during block building, as a function of
 * how many accounts the block access list already holds.
 */
@State(Scope.Thread)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class BlockAccessListItemCountBenchmark {

  private static final Address COINBASE = Address.fromHexString("0xc0");
  private static final Address TOKEN = Address.fromHexString("0x70");
  private static final StorageSlotKey TOTAL_SUPPLY = new StorageSlotKey(UInt256.ZERO);

  @Param({"5000", "28000", "58000"})
  public int accounts;

  private BlockAccessList.BlockAccessListBuilder builder;
  private PartialBlockAccessView candidate;

  @Setup
  public void setUp() {
    builder = BlockAccessList.builder();
    final int transactions = accounts / 2;
    for (int txIndex = 1; txIndex <= transactions; txIndex++) {
      builder.apply(transfer(txIndex, txIndex % 4 == 0));
    }
    candidate = transfer(transactions + 1, true);
  }

  @Benchmark
  public long itemCountIfApplied() {
    return builder.eip7928ItemCountIfApplied(candidate);
  }

  /** Reference point: the same count taken by applying the view to a full copy of the list. */
  @Benchmark
  public long itemCountOnCopy() {
    final BlockAccessList.BlockAccessListBuilder copy = BlockAccessList.builder();
    copy.mergeFrom(builder.build());
    copy.apply(candidate);
    return copy.eip7928ItemCount();
  }

  /**
   * A transfer between two fresh accounts that pays the coinbase; a token transfer also writes both
   * balance slots of one hot contract.
   */
  private static PartialBlockAccessView transfer(final int txIndex, final boolean token) {
    final Address sender = Address.fromHexString(String.format("0x%040x", 0x1000_0000L + txIndex));
    final Address recipient =
        Address.fromHexString(String.format("0x%040x", 0x2000_0000L + txIndex));
    final PartialBlockAccessView.PartialBlockAccessViewBuilder view =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder().withTxIndex(txIndex);
    view.getOrCreateAccountBuilder(sender).withPostBalance(Wei.of(txIndex)).withNonceChange(1L);
    view.getOrCreateAccountBuilder(recipient).withPostBalance(Wei.of(txIndex));
    view.getOrCreateAccountBuilder(COINBASE).withPostBalance(Wei.of(txIndex));
    if (token) {
      view.getOrCreateAccountBuilder(TOKEN)
          .addStorageRead(TOTAL_SUPPLY)
          .addStorageChange(balanceSlot(sender), UInt256.ONE, UInt256.ZERO)
          .addStorageChange(balanceSlot(recipient), UInt256.ZERO, UInt256.ONE);
    }
    return view.build();
  }

  private static StorageSlotKey balanceSlot(final Address holder) {
    return new StorageSlotKey(UInt256.fromBytes(holder.getBytes()));
  }
}
