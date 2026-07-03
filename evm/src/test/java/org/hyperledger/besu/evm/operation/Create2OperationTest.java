/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.evm.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.evm.MainnetEVMs.DEV_NET_CHAIN_ID;
import static org.hyperledger.besu.evm.frame.ExceptionalHaltReason.CODE_TOO_LARGE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.ConstantinopleGasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.processor.ContractCreationProcessor;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Deque;
import java.util.List;

import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class Create2OperationTest {

  private MessageFrame messageFrame;
  private final WorldUpdater worldUpdater = mock(WorldUpdater.class);
  private final MutableAccount account = mock(MutableAccount.class);
  private final EVM evm = MainnetEVMs.futureEips(EvmConfiguration.DEFAULT);
  private final MutableAccount newAccount = mock(MutableAccount.class);

  private final Create2Operation operation =
      new Create2Operation(new ConstantinopleGasCalculator());

  private static final String TOPIC =
      "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"; // 32 FFs
  private static final Bytes SIMPLE_CREATE =
      Bytes.fromHexString(
          "0x"
              + "7f" // push32
              + TOPIC
              + "6000" // PUSH1 0x00
              + "6000" // PUSH1 0x00
              + "A1" // LOG1
              + "6000" // PUSH1 0x00
              + "6000" // PUSH1 0x00
              + "F3" // RETURN
          );
  public static final String SENDER = "0xdeadc0de00000000000000000000000000000000";
  private static final int SHANGHAI_CREATE_GAS = 41240 + (0xc000 / 32) * 6;

  public static Object[][] params() {
    return new Object[][] {
      {
        "0x0000000000000000000000000000000000000000",
        "0x0000000000000000000000000000000000000000000000000000000000000000",
        "0x00",
        "0x4D1A2e2bB4F88F0250f26Ffff098B0b30B26BF38",
        32006
      },
      {
        "0xdeadbeef00000000000000000000000000000000",
        "0x0000000000000000000000000000000000000000000000000000000000000000",
        "0x00",
        "0xB928f69Bb1D91Cd65274e3c79d8986362984fDA3",
        32006
      },
      {
        "0xdeadbeef00000000000000000000000000000000",
        "0x000000000000000000000000feed000000000000000000000000000000000000",
        "0x00",
        "0xD04116cDd17beBE565EB2422F2497E06cC1C9833",
        32006
      },
      {
        "0x0000000000000000000000000000000000000000",
        "0x0000000000000000000000000000000000000000000000000000000000000000",
        "0xdeadbeef",
        "0x70f2b2914A2a4b783FaEFb75f459A580616Fcb5e",
        32006
      },
      {
        "0x00000000000000000000000000000000deadbeef",
        "0x00000000000000000000000000000000000000000000000000000000cafebabe",
        "0xdeadbeef",
        "0x60f3f640a8508fC6a86d45DF051962668E1e8AC7",
        32006
      },
      {
        "0x00000000000000000000000000000000deadbeef",
        "0x00000000000000000000000000000000000000000000000000000000cafebabe",
        "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        "0x1d8bfDC5D46DC4f61D6b6115972536eBE6A8854C",
        32012
      },
      {
        "0x0000000000000000000000000000000000000000",
        "0x0000000000000000000000000000000000000000000000000000000000000000",
        "0x",
        "0xE33C0C7F7df4809055C3ebA6c09CFe4BaF1BD9e0",
        32000
      }
    };
  }

  public void setUp(final String sender, final String salt, final String code) {

    final UInt256 memoryOffset = UInt256.fromHexString("0xFF");
    final Bytes codeBytes = Bytes.fromHexString(code);
    final UInt256 memoryLength = UInt256.valueOf(codeBytes.size());
    messageFrame =
        MessageFrame.builder()
            .type(MessageFrame.Type.CONTRACT_CREATION)
            .contract(Address.ZERO)
            .inputData(Bytes.EMPTY)
            .sender(Address.fromHexString(sender))
            .value(Wei.ZERO)
            .apparentValue(Wei.ZERO)
            .code(new Code(codeBytes))
            .completer(__ -> {})
            .address(Address.fromHexString(sender))
            .blockHashLookup((__, ___) -> Hash.ZERO)
            .blockValues(mock(BlockValues.class))
            .gasPrice(Wei.ZERO)
            .miningBeneficiary(Address.ZERO)
            .originator(Address.ZERO)
            .initialGas(100_000L)
            .worldUpdater(worldUpdater)
            .build();
    messageFrame.pushStackItem(UInt256.fromHexString(salt));
    messageFrame.pushStackItem(memoryLength);
    messageFrame.pushStackItem(memoryOffset);
    messageFrame.pushStackItem(UInt256.ZERO);
    messageFrame.expandMemory(0, 500);
    messageFrame.writeMemory(memoryOffset.trimLeadingZeros().toInt(), code.length(), codeBytes);

    when(account.getBalance()).thenReturn(Wei.ZERO);
    when(worldUpdater.getAccount(any())).thenReturn(account);
    when(worldUpdater.updater()).thenReturn(worldUpdater);
  }

  @ParameterizedTest
  @MethodSource("params")
  void shouldCalculateAddress(
      final String sender,
      final String salt,
      final String code,
      final String expectedAddress,
      final int ignoredExpectedGas) {
    setUp(sender, salt, code);
    final Address targetContractAddress =
        operation.generateTargetContractAddress(messageFrame, new Code(Bytes.fromHexString(code)));
    assertThat(targetContractAddress).isEqualTo(Address.fromHexString(expectedAddress));
  }

  @ParameterizedTest
  @MethodSource("params")
  void shouldCalculateGasPrice(
      final String sender,
      final String salt,
      final String code,
      final String ignoredExpectedAddress,
      final int expectedGas) {
    setUp(sender, salt, code);
    final OperationResult result = operation.execute(messageFrame, evm);
    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getGasCost()).isEqualTo(expectedGas);
  }

  @Test
  void shanghaiMaxInitCodeSizeCreate() {
    final UInt256 memoryOffset = UInt256.fromHexString("0xFF");
    final UInt256 memoryLength = UInt256.fromHexString("0xc000");
    final MessageFrame messageFrame = testMemoryFrame(memoryOffset, memoryLength);

    when(account.getNonce()).thenReturn(55L);
    when(account.getBalance()).thenReturn(Wei.ZERO);
    when(worldUpdater.getAccount(any())).thenReturn(account);
    when(worldUpdater.get(any())).thenReturn(account);
    when(worldUpdater.getSenderAccount(any())).thenReturn(account);
    when(worldUpdater.getOrCreate(any())).thenReturn(newAccount);
    when(newAccount.getCode()).thenReturn(Bytes.EMPTY);
    when(newAccount.isStorageEmpty()).thenReturn(true);
    when(worldUpdater.updater()).thenReturn(worldUpdater);

    final EVM evm = MainnetEVMs.shanghai(DEV_NET_CHAIN_ID, EvmConfiguration.DEFAULT);
    var result = operation.execute(messageFrame, evm);
    final MessageFrame createFrame = messageFrame.getMessageFrameStack().peek();
    final ContractCreationProcessor ccp = new ContractCreationProcessor(evm, false, List.of(), 0);
    ccp.process(createFrame, OperationTracer.NO_TRACING);

    final Log log = createFrame.getLogs().get(0);
    final String calculatedTopic = log.getTopics().get(0).getBytes().toUnprefixedHexString();
    assertThat(calculatedTopic).isEqualTo(TOPIC);
    assertThat(result.getGasCost()).isEqualTo(SHANGHAI_CREATE_GAS);
  }

  @Test
  void shanghaiMaxInitCodeSizePlus1Create() {
    final UInt256 memoryOffset = UInt256.fromHexString("0xFF");
    final UInt256 memoryLength = UInt256.fromHexString("0xc001");
    final MessageFrame messageFrame = testMemoryFrame(memoryOffset, memoryLength);

    when(account.getNonce()).thenReturn(55L);
    when(account.getBalance()).thenReturn(Wei.ZERO);
    when(worldUpdater.getAccount(any())).thenReturn(account);
    when(worldUpdater.get(any())).thenReturn(account);
    when(worldUpdater.getSenderAccount(any())).thenReturn(account);
    when(worldUpdater.getOrCreate(any())).thenReturn(newAccount);
    when(newAccount.getCode()).thenReturn(Bytes.EMPTY);
    when(newAccount.isStorageEmpty()).thenReturn(true);
    when(worldUpdater.updater()).thenReturn(worldUpdater);

    final EVM evm = MainnetEVMs.shanghai(DEV_NET_CHAIN_ID, EvmConfiguration.DEFAULT);
    var result = operation.execute(messageFrame, evm);
    assertThat(result.getHaltReason()).isEqualTo(CODE_TOO_LARGE);
  }

  @Test
  void amsterdamMaxInitCodeSizeCreate() {
    final UInt256 memoryOffset = UInt256.fromHexString("0xFF");
    final UInt256 memoryLength = UInt256.fromHexString("0x10000");
    final MessageFrame messageFrame = testMemoryFrame(memoryOffset, memoryLength);

    when(account.getNonce()).thenReturn(55L);
    when(account.getBalance()).thenReturn(Wei.ZERO);
    when(worldUpdater.getAccount(any())).thenReturn(account);
    when(worldUpdater.get(any())).thenReturn(account);
    when(worldUpdater.getSenderAccount(any())).thenReturn(account);
    when(worldUpdater.getOrCreate(any())).thenReturn(newAccount);
    when(newAccount.getCode()).thenReturn(Bytes.EMPTY);
    when(newAccount.isStorageEmpty()).thenReturn(true);
    when(worldUpdater.updater()).thenReturn(worldUpdater);

    final EVM evm = MainnetEVMs.amsterdam(DEV_NET_CHAIN_ID, EvmConfiguration.DEFAULT);
    var result = operation.execute(messageFrame, evm);
    assertThat(result.getHaltReason()).isNull();
  }

  @Test
  void amsterdamMaxInitCodeSizePlus1Create() {
    final UInt256 memoryOffset = UInt256.fromHexString("0xFF");
    // MAX_INITCODE_SIZE_AMSTERDAM (0x20000) + 1
    final UInt256 memoryLength = UInt256.fromHexString("0x20001");
    final MessageFrame messageFrame = testMemoryFrame(memoryOffset, memoryLength);

    when(account.getNonce()).thenReturn(55L);
    when(account.getBalance()).thenReturn(Wei.ZERO);
    when(worldUpdater.getAccount(any())).thenReturn(account);
    when(worldUpdater.get(any())).thenReturn(account);
    when(worldUpdater.getSenderAccount(any())).thenReturn(account);
    when(worldUpdater.getOrCreate(any())).thenReturn(newAccount);
    when(newAccount.getCode()).thenReturn(Bytes.EMPTY);
    when(newAccount.isStorageEmpty()).thenReturn(true);
    when(worldUpdater.updater()).thenReturn(worldUpdater);

    final EVM evm = MainnetEVMs.amsterdam(DEV_NET_CHAIN_ID, EvmConfiguration.DEFAULT);
    var result = operation.execute(messageFrame, evm);
    assertThat(result.getHaltReason()).isEqualTo(CODE_TOO_LARGE);
  }

  @Test
  void amsterdamBetweenOldAndNewInitCodeLimitCreate() {
    final UInt256 memoryOffset = UInt256.fromHexString("0xFF");
    final UInt256 memoryLength = UInt256.fromHexString("0xC001");
    final MessageFrame messageFrame = testMemoryFrame(memoryOffset, memoryLength);

    when(account.getNonce()).thenReturn(55L);
    when(account.getBalance()).thenReturn(Wei.ZERO);
    when(worldUpdater.getAccount(any())).thenReturn(account);
    when(worldUpdater.get(any())).thenReturn(account);
    when(worldUpdater.getSenderAccount(any())).thenReturn(account);
    when(worldUpdater.getOrCreate(any())).thenReturn(newAccount);
    when(newAccount.getCode()).thenReturn(Bytes.EMPTY);
    when(newAccount.isStorageEmpty()).thenReturn(true);
    when(worldUpdater.updater()).thenReturn(worldUpdater);

    final EVM evm = MainnetEVMs.amsterdam(DEV_NET_CHAIN_ID, EvmConfiguration.DEFAULT);
    var result = operation.execute(messageFrame, evm);
    assertThat(result.getHaltReason()).isNull();
  }

  @Test
  void oversizedInitCodeOnStackAbortsBeforeMemoryExpansion() {
    // EIP-3860: when the declared initcode size on the stack exceeds the limit, the
    // operation is an early exceptional abort and must not have memory side-effects.
    // CREATE2 carries an extra salt item on the stack, so this guards getInputSize against a
    // regression if the stack layout ever diverges from CREATE.
    final UInt256 memoryOffset = UInt256.ZERO;
    final UInt256 memoryLength =
        UInt256.fromHexString("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
    final MessageFrame messageFrame =
        MessageFrame.builder()
            .type(MessageFrame.Type.CONTRACT_CREATION)
            .contract(Address.ZERO)
            .inputData(Bytes.EMPTY)
            .sender(Address.fromHexString(SENDER))
            .value(Wei.ZERO)
            .apparentValue(Wei.ZERO)
            .code(new Code(SIMPLE_CREATE))
            .completer(__ -> {})
            .address(Address.fromHexString(SENDER))
            .blockHashLookup((__, ___) -> Hash.ZERO)
            .blockValues(mock(BlockValues.class))
            .gasPrice(Wei.ZERO)
            .miningBeneficiary(Address.ZERO)
            .originator(Address.ZERO)
            .initialGas(Long.MAX_VALUE)
            .worldUpdater(worldUpdater)
            .build();
    messageFrame.pushStackItem(Bytes.EMPTY); // salt
    messageFrame.pushStackItem(memoryLength);
    messageFrame.pushStackItem(memoryOffset);
    messageFrame.pushStackItem(UInt256.ZERO);

    final EVM evm = MainnetEVMs.shanghai(DEV_NET_CHAIN_ID, EvmConfiguration.DEFAULT);
    var result = operation.execute(messageFrame, evm);

    assertThat(result.getHaltReason()).isEqualTo(CODE_TOO_LARGE);
    assertThat(messageFrame.memoryWordSize()).isEqualTo(0);
  }

  @NotNull
  private MessageFrame testMemoryFrame(final UInt256 memoryOffset, final UInt256 memoryLength) {
    final MessageFrame messageFrame =
        MessageFrame.builder()
            .type(MessageFrame.Type.CONTRACT_CREATION)
            .contract(Address.ZERO)
            .inputData(Bytes.EMPTY)
            .sender(Address.fromHexString(SENDER))
            .value(Wei.ZERO)
            .apparentValue(Wei.ZERO)
            .code(new Code(SIMPLE_CREATE))
            .completer(__ -> {})
            .address(Address.fromHexString(SENDER))
            .blockHashLookup((__, ___) -> Hash.ZERO)
            .blockValues(mock(BlockValues.class))
            .gasPrice(Wei.ZERO)
            .miningBeneficiary(Address.ZERO)
            .originator(Address.ZERO)
            // Enough to cover memory expansion plus CREATE2 keccak hashing of a 128KiB+ initcode so
            // the MAX_INIT_CODE_SIZE check is reached rather than halting on gas first.
            .initialGas(1_000_000L)
            .worldUpdater(worldUpdater)
            .build();
    messageFrame.pushStackItem(Bytes.EMPTY);
    messageFrame.pushStackItem(memoryLength);
    messageFrame.pushStackItem(memoryOffset);
    messageFrame.pushStackItem(UInt256.ZERO);
    messageFrame.expandMemory(0, 500);
    messageFrame.writeMemory(
        memoryOffset.trimLeadingZeros().toInt(), SIMPLE_CREATE.size(), SIMPLE_CREATE);
    final Deque<MessageFrame> messageFrameStack = messageFrame.getMessageFrameStack();
    if (messageFrameStack.isEmpty()) {
      messageFrameStack.push(messageFrame);
    }
    return messageFrame;
  }
}
