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
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.v2.operation.SelfBalanceOperationV2;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/** Benchmark for SELFBALANCE EVM v2 operation. */
@State(Scope.Thread)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(value = TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class SelfBalanceOperationBenchmarkV2 {

  protected static final int SAMPLE_SIZE = 30_000;

  private MessageFrame frame;

  @Setup
  public void setUp() {
    // Set up a mock world updater returning a non-null account with a balance
    final WorldUpdater worldUpdater = mock(WorldUpdater.class);
    final MutableAccount account = mock(MutableAccount.class);
    final byte[] balanceBytes = new byte[32];
    new Random(42).nextBytes(balanceBytes);
    final Wei balance = Wei.of(new java.math.BigInteger(1, balanceBytes));
    when(account.getBalance()).thenReturn(balance);

    frame =
        MessageFrame.builder()
            .enableEvmV2(true)
            .worldUpdater(worldUpdater)
            .originator(Address.ZERO)
            .gasPrice(Wei.ONE)
            .blobGasPrice(Wei.ONE)
            .blockValues(mock(org.hyperledger.besu.evm.frame.BlockValues.class))
            .miningBeneficiary(Address.ZERO)
            .blockHashLookup((__, ___) -> org.hyperledger.besu.datatypes.Hash.ZERO)
            .type(MessageFrame.Type.MESSAGE_CALL)
            .initialGas(Long.MAX_VALUE)
            .address(Address.ZERO)
            .contract(Address.ZERO)
            .inputData(org.apache.tuweni.bytes.Bytes32.ZERO)
            .sender(Address.ZERO)
            .value(Wei.ZERO)
            .apparentValue(Wei.ZERO)
            .code(org.hyperledger.besu.evm.Code.EMPTY_CODE)
            .completer(__ -> {})
            .build();

    // Configure world updater to return the account for Address.ZERO (frame's contract address)
    when(worldUpdater.getAccount(Address.ZERO)).thenReturn(account);
  }

  @Benchmark
  public void selfBalance(final Blackhole blackhole) {
    blackhole.consume(SelfBalanceOperationV2.staticOperation(frame, frame.stackDataV2()));
    // Pop the resulting balance
    frame.setTopV2(frame.stackTopV2() - 1);
  }
}
