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
package org.hyperledger.besu.ethereum.vm.operations.v2;

import static org.mockito.Mockito.mock;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.OsakaGasCalculator;
import org.hyperledger.besu.evm.operation.SStoreOperation;
import org.hyperledger.besu.evm.v2.operation.SLoadOperationV2;
import org.hyperledger.besu.evm.v2.operation.SStoreOperationV2;
import org.hyperledger.besu.evm.v2.worldstate.JournaledWorldUpdaterV2;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * SLOAD and SSTORE on the v2 stack, against storage that the block accumulator holds, under the
 * updaters of a transaction's call frames: stacked updaters, or the EVM v2 journal.
 */
@State(Scope.Thread)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class StorageOperationBenchmarkV2 {

  private static final int SLOTS = 1024;
  private static final int TOUCHED = 8;
  private static final Address CONTRACT =
      Address.fromHexString("0x00000000000000000000000000000000c0ffee01");

  /** Updaters above the block accumulator: two per transaction and one per call frame. */
  @Param({"3", "6"})
  public int levels;

  /** STACKED: an updater per level. JOURNAL: the transaction's journal and a mark per frame. */
  @Param({"STACKED", "JOURNAL"})
  public String mode;

  private final GasCalculator gasCalculator = new OsakaGasCalculator();
  private SLoadOperationV2 sload;
  private SStoreOperationV2 sstore;
  private WorldUpdater top;
  private long[][] keys;
  private MessageFrame warmFrame;
  private int index;

  @Setup(Level.Trial)
  public void setUp() {
    final ExecutionContextTestFixture fixture =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource("/genesis-jmh.json"))
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    final MutableWorldState worldState = fixture.getStateArchive().getWorldState();

    final WorldUpdater setup = worldState.updater();
    final MutableAccount contract = setup.createAccount(CONTRACT, 1, Wei.ZERO);
    contract.setCode(Bytes.fromHexString("0x6000"));
    keys = new long[SLOTS][];
    for (int i = 0; i < SLOTS; i++) {
      // most slots are mapping entries, whose keys are hashes
      final Bytes32 key =
          Bytes32.wrap(Hash.hash(Bytes.ofUnsignedInt(i)).getBytes().toArrayUnsafe());
      keys[i] = new long[] {key.getLong(0), key.getLong(8), key.getLong(16), key.getLong(24)};
      contract.setStorageValue(UInt256.fromBytes(key), UInt256.valueOf(i + 1L));
    }
    setup.commit();
    worldState.persist(null);

    final WorldUpdater transactionUpdater = worldState.updater().updater();
    WorldUpdater updater =
        mode.equals("JOURNAL")
            ? new JournaledWorldUpdaterV2(transactionUpdater)
            : transactionUpdater.updater();
    for (int i = 3; i < levels; i++) {
      updater = updater.updater();
    }
    top = updater;
    // a called contract is tracked by the updater of its frame
    top.getAccount(CONTRACT);

    sload = new SLoadOperationV2(gasCalculator);
    sstore = new SStoreOperationV2(gasCalculator, SStoreOperation.EIP_1706_MINIMUM);
    // the transaction of this frame has read every slot once
    warmFrame = newFrame(top.updater());
    for (final long[] key : keys) {
      load(warmFrame, key);
    }
  }

  private static MessageFrame newFrame(final WorldUpdater updater) {
    return MessageFrame.builder()
        .enableEvmV2(true)
        .worldUpdater(updater)
        .originator(Address.ZERO)
        .gasPrice(Wei.ONE)
        .blobGasPrice(Wei.ONE)
        .blockValues(mock(BlockValues.class))
        .miningBeneficiary(Address.ZERO)
        .blockHashLookup((__, ___) -> Hash.ZERO)
        .type(MessageFrame.Type.MESSAGE_CALL)
        .initialGas(Long.MAX_VALUE)
        .address(CONTRACT)
        .contract(CONTRACT)
        .inputData(Bytes32.ZERO)
        .sender(Address.ZERO)
        .value(Wei.ZERO)
        .apparentValue(Wei.ZERO)
        .code(Code.EMPTY_CODE)
        .completer(__ -> {})
        .build();
  }

  private static void push(
      final MessageFrame frame, final long w0, final long w1, final long w2, final long w3) {
    final long[] s = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int off = top << 2;
    s[off] = w0;
    s[off + 1] = w1;
    s[off + 2] = w2;
    s[off + 3] = w3;
    frame.setTopV2(top + 1);
  }

  private long load(final MessageFrame frame, final long[] key) {
    push(frame, key[0], key[1], key[2], key[3]);
    sload.execute(frame, null);
    final int top = frame.stackTopV2();
    final long low = frame.stackDataV2()[((top - 1) << 2) + 3];
    frame.setTopV2(top - 1);
    return low;
  }

  private void store(final MessageFrame frame, final long[] key, final long value) {
    push(frame, 0L, 0L, 0L, value);
    push(frame, key[0], key[1], key[2], key[3]);
    sstore.execute(frame, null);
  }

  /** A slot the transaction has already read. */
  @Benchmark
  public long sloadWarm() {
    return load(warmFrame, keys[index++ & (SLOTS - 1)]);
  }

  /** A call frame that reads a few slots, reads them again, updates them and reads them back. */
  @Benchmark
  public void callFrame(final Blackhole blackhole) {
    final WorldUpdater updater = top.updater();
    final MessageFrame frame = newFrame(updater);
    final int first = index;
    index += TOUCHED;
    for (int i = 0; i < TOUCHED; i++) {
      final long[] key = keys[(first + i) & (SLOTS - 1)];
      blackhole.consume(load(frame, key));
      blackhole.consume(load(frame, key));
      store(frame, key, first + i + 7L);
      blackhole.consume(load(frame, key));
    }
    updater.revert();
    updater.commit();
  }
}
