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
package org.hyperledger.besu.consensus.merge.blockcreation;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Withdrawal;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt64;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Thread)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(value = TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class PayloadIdentifierBenchmark {

  private Hash parentHash;
  private Long timestamp;
  private Bytes32 prevRandao;
  private Address feeRecipient;
  private Optional<List<Withdrawal>> withdrawals;
  private Optional<Bytes32> parentBeaconBlockRoot;
  private Optional<Long> slotNumber;

  @Setup
  public void setUp() {
    final Random random = new Random(42);

    final byte[] hashBytes = new byte[32];
    random.nextBytes(hashBytes);
    parentHash = Hash.wrap(Bytes32.wrap(hashBytes));

    timestamp = 1_700_000_000L;

    final byte[] randaoBytes = new byte[32];
    random.nextBytes(randaoBytes);
    prevRandao = Bytes32.wrap(randaoBytes);

    feeRecipient = Address.fromHexString("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73");

    final byte[] beaconRootBytes = new byte[32];
    random.nextBytes(beaconRootBytes);
    parentBeaconBlockRoot = Optional.of(Bytes32.wrap(beaconRootBytes));

    slotNumber = Optional.of(12_345_678L);

    // 16 is the standard maximum number of withdrawals per block (EIP-4895)
    final List<Withdrawal> withdrawalList = new ArrayList<>(16);
    for (int i = 0; i < 16; i++) {
      final byte[] addrBytes = new byte[20];
      random.nextBytes(addrBytes);
      withdrawalList.add(
          new Withdrawal(
              UInt64.valueOf(i),
              UInt64.valueOf(100 + i),
              Address.wrap(Bytes.wrap(addrBytes)),
              GWei.of(1_000_000_000L + i)));
    }
    withdrawals = Optional.of(withdrawalList);
  }

  /**
   * Previous implementation: bitwise XOR of each field's hash code, shifted to separate bit
   * regions.
   */
  @Benchmark
  public PayloadIdentifier xorBased() {
    return new PayloadIdentifier(
        timestamp
            ^ ((long) parentHash.toHexString().hashCode()) << 8
            ^ ((long) prevRandao.toHexString().hashCode()) << 16
            ^ ((long) feeRecipient.toHexString().hashCode()) << 24
            ^ (long)
                withdrawals
                    .map(
                        ws ->
                            ws.stream()
                                .sorted(Comparator.comparing(Withdrawal::getIndex))
                                .map(Withdrawal::hashCode)
                                .reduce(1, (a, b) -> a ^ (b * 31)))
                    .orElse(0)
            ^ ((long) parentBeaconBlockRoot.hashCode()) << 32
            ^ slotNumber.orElse(0L) << 40);
  }

  @Benchmark
  public PayloadIdentifier currentImplementation() {
    return PayloadIdentifier.forPayloadParams(
        parentHash,
        timestamp,
        prevRandao,
        feeRecipient,
        withdrawals,
        parentBeaconBlockRoot,
        slotNumber);
  }
}
