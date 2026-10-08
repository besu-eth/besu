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
package org.hyperledger.besu.evm.fluent;

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.processor.ContractCreationProcessor;
import org.hyperledger.besu.evm.processor.MessageCallProcessor;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.SelfDestructSettlement;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** The Evm executor. */
public class EVMExecutor {

  private boolean commitWorldState = false;
  private WorldUpdater worldUpdater = new SimpleWorld();
  private long gas = Long.MAX_VALUE;
  private Address receiver = Address.ZERO;
  private Address sender = Address.ZERO;
  private Address contract = Address.ZERO;
  private Address coinbase = Address.ZERO;
  private Wei gasPriceGWei = Wei.ZERO;
  private Wei blobGasPrice = Wei.ZERO;
  private Bytes callData = Bytes.EMPTY;
  private Wei ethValue = Wei.ZERO;
  private Code code = Code.EMPTY_CODE;
  private BlockValues blockValues = new SimpleBlockValues();
  private BlockHashLookup blockHashLookup = (__, ___) -> null;
  private Optional<List<VersionedHash>> versionedHashes = Optional.empty();
  private OperationTracer tracer = OperationTracer.NO_TRACING;
  private Set<Address> accessListWarmAddresses = new HashSet<>(Address.SIZE);
  private Multimap<Address, Bytes32> accessListWarmStorage = HashMultimap.create();
  private MessageCallProcessor messageCallProcessor = null;
  private ContractCreationProcessor contractCreationProcessor = null;
  private MessageFrame.Type messageFrameType = MessageFrame.Type.MESSAGE_CALL;
  private final EvmSpec evmSpec;

  /**
   * Constrycts an EVM executor
   *
   * @param evmSpec specification of the EVM
   */
  public EVMExecutor(final EvmSpec evmSpec) {
    checkNotNull(evmSpec, "evmSpec must not be null");
    this.evmSpec = evmSpec;
  }

  private MessageCallProcessor thisMessageCallProcessor() {
    return Objects.requireNonNullElseGet(
        messageCallProcessor,
        () -> new MessageCallProcessor(evmSpec.getEvm(), evmSpec.getPrecompileContractRegistry()));
  }

  private ContractCreationProcessor thisContractCreationProcessor() {
    return Objects.requireNonNullElseGet(
        contractCreationProcessor,
        () ->
            new ContractCreationProcessor(
                evmSpec.getEvm(),
                evmSpec.isRequireDeposit(),
                evmSpec.getContractValidationRules(),
                evmSpec.getInitialNonce()));
  }

  /**
   * Execute code.
   *
   * @param code the code
   * @param inputData the input data
   * @param value the value
   * @param receiver the receiver
   * @return the bytes
   */
  public Bytes execute(
      final Code code, final Bytes inputData, final Wei value, final Address receiver) {
    this.code = code;
    this.callData = inputData;
    this.ethValue = value;
    this.receiver = receiver;
    return execute();
  }

  /**
   * Execute code.
   *
   * @param codeBytes the code bytes
   * @param inputData the input data
   * @param value the value
   * @param receiver the receiver
   * @return the bytes
   */
  public Bytes execute(
      final Bytes codeBytes, final Bytes inputData, final Wei value, final Address receiver) {
    this.code = new Code(codeBytes);
    this.callData = inputData;
    this.ethValue = value;
    this.receiver = receiver;
    return execute();
  }

  /**
   * Execute.
   *
   * @return the bytes
   */
  public Bytes execute() {
    final MessageCallProcessor mcp = thisMessageCallProcessor();
    final ContractCreationProcessor ccp = thisContractCreationProcessor();
    final MessageFrame initialMessageFrame =
        MessageFrame.builder()
            .type(messageFrameType)
            .worldUpdater(worldUpdater.updater())
            .initialGas(gas)
            .contract(contract)
            .address(receiver)
            .originator(sender)
            .sender(sender)
            .gasPrice(gasPriceGWei)
            .blobGasPrice(blobGasPrice)
            .inputData(callData)
            .value(ethValue)
            .apparentValue(ethValue)
            .code(code)
            .blockValues(blockValues)
            .miningBeneficiary(coinbase)
            .blockHashLookup(blockHashLookup)
            .eip2930AccessListWarmAddresses(accessListWarmAddresses)
            .eip2930AccessListWarmStorage(accessListWarmStorage)
            .versionedHashes(versionedHashes)
            .completer(c -> {})
            .build();

    final Deque<MessageFrame> messageFrameStack = initialMessageFrame.getMessageFrameStack();
    while (!messageFrameStack.isEmpty()) {
      final MessageFrame messageFrame = messageFrameStack.peek();
      (switch (messageFrame.getType()) {
            case CONTRACT_CREATION -> ccp;
            case MESSAGE_CALL -> mcp;
          })
          .process(messageFrame, tracer);
    }
    SelfDestructSettlement.settle(
        worldUpdater, initialMessageFrame.getSelfDestructs(), evmSpec.getEvm().getGasCalculator());
    if (commitWorldState) {
      worldUpdater.commit();
    }
    return initialMessageFrame.getReturnData();
  }

  /**
   * MArk Commit world state to true.
   *
   * @return the evm executor
   */
  public EVMExecutor commitWorldState() {
    return commitWorldState(true);
  }

  /**
   * Sets Commit world state.
   *
   * @param commitWorldState the commit world state
   * @return the evm executor
   */
  public EVMExecutor commitWorldState(final boolean commitWorldState) {
    this.commitWorldState = commitWorldState;
    return this;
  }

  /**
   * Sets World updater.
   *
   * @param worldUpdater the world updater
   * @return the evm executor
   */
  public EVMExecutor worldUpdater(final WorldUpdater worldUpdater) {
    this.worldUpdater = worldUpdater;
    return this;
  }

  /**
   * Sets Gas.
   *
   * @param gas the gas
   * @return the evm executor
   */
  public EVMExecutor gas(final long gas) {
    this.gas = gas;
    return this;
  }

  /**
   * Sets Receiver address.
   *
   * @param receiver the receiver
   * @return the evm executor
   */
  public EVMExecutor receiver(final Address receiver) {
    this.receiver = receiver;
    return this;
  }

  /**
   * Sets Sender address.
   *
   * @param sender the sender
   * @return the evm executor
   */
  public EVMExecutor sender(final Address sender) {
    this.sender = sender;
    return this;
  }

  /**
   * Sets the address of the executing contract
   *
   * @param contract the contract
   * @return the evm executor
   */
  public EVMExecutor contract(final Address contract) {
    this.contract = contract;
    return this;
  }

  /**
   * Sets the address of the coinbase aka mining beneficiary
   *
   * @param coinbase the coinbase
   * @return the evm executor
   */
  public EVMExecutor coinbase(final Address coinbase) {
    this.coinbase = coinbase;
    // EIP-3651
    if (EvmSpecVersion.SHANGHAI.compareTo(evmSpec.getEvm().getEvmVersion()) <= 0) {
      this.warmAddress(coinbase);
    }
    return this;
  }

  /**
   * Sets Gas price GWei.
   *
   * @param gasPriceGWei the gas price g wei
   * @return the evm executor
   */
  public EVMExecutor gasPriceGWei(final Wei gasPriceGWei) {
    this.gasPriceGWei = gasPriceGWei;
    return this;
  }

  /**
   * Sets Blob Gas price.
   *
   * @param blobGasPrice the blob gas price g wei
   * @return the evm executor
   */
  public EVMExecutor blobGasPrice(final Wei blobGasPrice) {
    this.blobGasPrice = blobGasPrice;
    return this;
  }

  /**
   * Sets Call data.
   *
   * @param callData the call data
   * @return the evm executor
   */
  public EVMExecutor callData(final Bytes callData) {
    this.callData = callData;
    return this;
  }

  /**
   * Sets Eth value.
   *
   * @param ethValue the eth value
   * @return the evm executor
   */
  public EVMExecutor ethValue(final Wei ethValue) {
    this.ethValue = ethValue;
    return this;
  }

  /**
   * Sets Code.
   *
   * @param code the code
   * @return the evm executor
   */
  public EVMExecutor code(final Code code) {
    this.code = code;
    return this;
  }

  /**
   * Sets Code.
   *
   * @param codeBytes the code bytes
   * @return the evm executor
   */
  public EVMExecutor code(final Bytes codeBytes) {
    this.code = new Code(codeBytes);
    return this;
  }

  /**
   * Sets Block values.
   *
   * @param blockValues the block values
   * @return the evm executor
   */
  public EVMExecutor blockValues(final BlockValues blockValues) {
    this.blockValues = blockValues;
    return this;
  }

  /**
   * Sets the difficulty bytes on a SimpleBlockValues object
   *
   * @param difficulty the difficulty
   * @return the evm executor
   * @throws ClassCastException if the blockValues was set with a value that is not a {@link
   *     SimpleBlockValues}
   */
  public EVMExecutor difficulty(final Bytes difficulty) {
    ((SimpleBlockValues) this.blockValues).setDifficultyBytes(difficulty);
    return this;
  }

  /**
   * Sets the mix hash bytes on a SimpleBlockValues object
   *
   * @param mixHash the mix hash
   * @return the evm executor
   * @throws ClassCastException if the blockValues was set with a value that is not a {@link
   *     SimpleBlockValues}
   */
  public EVMExecutor mixHash(final Bytes32 mixHash) {
    ((SimpleBlockValues) this.blockValues).setMixHashOrPrevRandao(mixHash);
    return this;
  }

  /**
   * Sets the prev randao bytes on a SimpleBlockValues object
   *
   * @param prevRandao the prev randao
   * @return the evm executor
   * @throws ClassCastException if the blockValues was set with a value that is not a {@link
   *     SimpleBlockValues}
   */
  public EVMExecutor prevRandao(final Bytes32 prevRandao) {
    ((SimpleBlockValues) this.blockValues).setMixHashOrPrevRandao(prevRandao);
    return this;
  }

  /**
   * Sets the baseFee for the block, directly.
   *
   * @param baseFee the baseFee
   * @return the evm executor
   * @throws ClassCastException if the blockValues was set with a value that is not a {@link
   *     SimpleBlockValues}
   */
  public EVMExecutor baseFee(final Wei baseFee) {
    return baseFee(Optional.ofNullable(baseFee));
  }

  /**
   * Sets the baseFee for the block, as an Optional.
   *
   * @param baseFee the baseFee
   * @return the evm executor
   * @throws ClassCastException if the blockValues was set with a value that is not a {@link
   *     SimpleBlockValues}
   */
  public EVMExecutor baseFee(final Optional<Wei> baseFee) {
    ((SimpleBlockValues) this.blockValues).setBaseFee(baseFee);
    return this;
  }

  /**
   * Sets the block number for the block.
   *
   * @param number the block number
   * @return the evm executor
   * @throws ClassCastException if the blockValues was set with a value that is not a {@link
   *     SimpleBlockValues}
   */
  public EVMExecutor number(final long number) {
    ((SimpleBlockValues) this.blockValues).setNumber(number);
    return this;
  }

  /**
   * Sets the timestamp for the block.
   *
   * @param timestamp the block timestamp
   * @return the evm executor
   * @throws ClassCastException if the blockValues was set with a value that is not a {@link
   *     SimpleBlockValues}
   */
  public EVMExecutor timestamp(final long timestamp) {
    ((SimpleBlockValues) this.blockValues).setTimestamp(timestamp);
    return this;
  }

  /**
   * Sets the gas limit for the block.
   *
   * @param gasLimit the block gas limit
   * @return the evm executor
   * @throws ClassCastException if the blockValues was set with a value that is not a {@link
   *     SimpleBlockValues}
   */
  public EVMExecutor gasLimit(final long gasLimit) {
    ((SimpleBlockValues) this.blockValues).setGasLimit(gasLimit);
    return this;
  }

  /**
   * Sets the block hash lookup function
   *
   * @param blockHashLookup the block hash lookup function
   * @return the evm executor
   */
  public EVMExecutor blockHashLookup(final BlockHashLookup blockHashLookup) {
    this.blockHashLookup = blockHashLookup;
    return this;
  }

  /**
   * Sets Version Hashes for blobs. The blobs themselves are not accessible.
   *
   * @param versionedHashes the versioned hashes
   * @return the evm executor
   */
  public EVMExecutor versionedHashes(final Optional<List<VersionedHash>> versionedHashes) {
    this.versionedHashes = versionedHashes;
    return this;
  }

  /**
   * Sets Operation Tracer.
   *
   * @param tracer the tracer
   * @return the evm executor
   */
  public EVMExecutor tracer(final OperationTracer tracer) {
    this.tracer = tracer;
    return this;
  }

  /**
   * Sets Access list warm addresses.
   *
   * @param accessListWarmAddresses the access list warm addresses
   * @return the evm executor
   */
  public EVMExecutor accessListWarmAddresses(final Set<Address> accessListWarmAddresses) {
    this.accessListWarmAddresses = accessListWarmAddresses;
    return this;
  }

  /**
   * Sets Warm addresses.
   *
   * @param addresses the addresses
   * @return the evm executor
   */
  public EVMExecutor warmAddress(final Address... addresses) {
    this.accessListWarmAddresses.addAll(List.of(addresses));
    return this;
  }

  /**
   * Sets Access list warm storage map.
   *
   * @param accessListWarmStorage the access list warm storage
   * @return the evm executor
   */
  public EVMExecutor accessListWarmStorage(final Multimap<Address, Bytes32> accessListWarmStorage) {
    this.accessListWarmStorage = accessListWarmStorage;
    return this;
  }

  /**
   * Sets Access list warm storage.
   *
   * @param address the address
   * @param slots the slots
   * @return the evm executor
   */
  public EVMExecutor accessListWarmStorage(final Address address, final Bytes32... slots) {
    this.accessListWarmStorage.putAll(address, List.of(slots));
    return this;
  }

  /**
   * Sets Message call processor.
   *
   * @param messageCallProcessor the message call processor
   * @return the evm executor
   */
  public EVMExecutor messageCallProcessor(final MessageCallProcessor messageCallProcessor) {
    this.messageCallProcessor = messageCallProcessor;
    return this;
  }

  /**
   * Sets Contract call processor.
   *
   * @param contractCreationProcessor the contract creation processor
   * @return the evm executor
   */
  public EVMExecutor contractCallProcessor(
      final ContractCreationProcessor contractCreationProcessor) {
    this.contractCreationProcessor = contractCreationProcessor;
    return this;
  }

  /**
   * Sets the message frame type
   *
   * @param messageFrameType message frame type
   * @return the builder
   */
  public EVMExecutor messageFrameType(final MessageFrame.Type messageFrameType) {
    this.messageFrameType = messageFrameType;
    return this;
  }
}
