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
package org.hyperledger.besu.evm.testutils;

import static org.hyperledger.besu.evm.frame.MessageFrame.DEFAULT_MAX_STACK_SIZE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.Words;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

public class TestMessageFrameBuilder {

  public static final Address DEFAULT_ADDRESS = Address.fromHexString("0xe8f1b89");
  private static final int maxStackSize = DEFAULT_MAX_STACK_SIZE;

  private Optional<BlockValues> blockValues = Optional.empty();
  private Optional<WorldUpdater> worldUpdater = Optional.empty();
  private long initialGas = Long.MAX_VALUE;
  private Address address = DEFAULT_ADDRESS;
  private Address sender = DEFAULT_ADDRESS;
  private Address originator = DEFAULT_ADDRESS;
  private Address contract = DEFAULT_ADDRESS;
  private Wei gasPrice = Wei.ZERO;
  private Wei blobGasPrice = Wei.ZERO;
  private Wei value = Wei.ZERO;
  private Bytes inputData = Bytes.EMPTY;
  private Code code = Code.EMPTY_CODE;
  private int pc = 0;
  private final List<Bytes> stackItems = new ArrayList<>();
  private Optional<BlockHashLookup> blockHashLookup = Optional.empty();
  private Bytes memory = Bytes.EMPTY;
  private boolean isStatic = false;

  public TestMessageFrameBuilder worldUpdater(final WorldUpdater worldUpdater) {
    this.worldUpdater = Optional.of(worldUpdater);
    return this;
  }

  public TestMessageFrameBuilder initialGas(final long initialGas) {
    this.initialGas = initialGas;
    return this;
  }

  public TestMessageFrameBuilder sender(final Address sender) {
    this.sender = sender;
    return this;
  }

  public TestMessageFrameBuilder address(final Address address) {
    this.address = address;
    return this;
  }

  TestMessageFrameBuilder originator(final Address originator) {
    this.originator = originator;
    return this;
  }

  public TestMessageFrameBuilder contract(final Address contract) {
    this.contract = contract;
    return this;
  }

  public TestMessageFrameBuilder gasPrice(final Wei gasPrice) {
    this.gasPrice = gasPrice;
    return this;
  }

  public TestMessageFrameBuilder blobGasPrice(final Wei blobGasPrice) {
    this.blobGasPrice = blobGasPrice;
    return this;
  }

  public TestMessageFrameBuilder value(final Wei value) {
    this.value = value;
    return this;
  }

  public TestMessageFrameBuilder inputData(final Bytes inputData) {
    this.inputData = inputData;
    return this;
  }

  public TestMessageFrameBuilder code(final Code code) {
    this.code = code;
    return this;
  }

  public TestMessageFrameBuilder pc(final int pc) {
    this.pc = pc;
    return this;
  }

  public TestMessageFrameBuilder blockValues(final BlockValues blockValues) {
    this.blockValues = Optional.of(blockValues);
    return this;
  }

  public TestMessageFrameBuilder pushStackItem(final Bytes item) {
    stackItems.add(item);
    return this;
  }

  public TestMessageFrameBuilder blockHashLookup(final BlockHashLookup blockHashLookup) {
    this.blockHashLookup = Optional.of(blockHashLookup);
    return this;
  }

  public TestMessageFrameBuilder memory(final Bytes memory) {
    this.memory = memory;
    return this;
  }

  public TestMessageFrameBuilder isStatic(final boolean isStatic) {
    this.isStatic = isStatic;
    return this;
  }

  public MessageFrame build() {
    final MessageFrame frame =
        MessageFrame.builder()
            // the stack items are pushed onto the EVM v1 stack
            .enableEvmV2(false)
            .type(MessageFrame.Type.MESSAGE_CALL)
            .worldUpdater(worldUpdater.orElseGet(this::createDefaultWorldUpdater))
            .initialGas(initialGas)
            .address(address)
            .originator(originator)
            .gasPrice(gasPrice)
            .blobGasPrice(blobGasPrice)
            .inputData(inputData)
            .sender(sender)
            .value(value)
            .apparentValue(value)
            .contract(contract)
            .code(code)
            .blockValues(blockValues.orElseGet(() -> new FakeBlockValues(1337)))
            .completer(c -> {})
            .miningBeneficiary(Address.ZERO)
            .blockHashLookup(
                blockHashLookup.orElse((__, number) -> Hash.hash(Words.longBytes(number))))
            .maxStackSize(maxStackSize)
            .isStatic(isStatic)
            .build();
    frame.setPC(pc);
    stackItems.forEach(frame::pushStackItem);
    frame.writeMemory(0, memory.size(), memory);
    return frame;
  }

  private WorldUpdater createDefaultWorldUpdater() {
    return new ToyWorld();
  }
}
