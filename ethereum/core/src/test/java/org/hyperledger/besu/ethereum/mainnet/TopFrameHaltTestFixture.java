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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.math.BigInteger;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Amsterdam transactions whose top frame runs out of gas during its EIP-2780 preparation charges,
 * before any code runs. Each one gets only its intrinsic gas, which excludes the state gas the
 * preparation charges.
 */
public class TopFrameHaltTestFixture {

  private static final String GENESIS_RESOURCE =
      "/org/hyperledger/besu/ethereum/mainnet/genesis-bp-it.json";
  // Funded in GENESIS_RESOURCE, which activates Amsterdam at genesis.
  private static final String SENDER_PRIVATE_KEY =
      "3a4ff6d22d7502ef2452368165422861c01a0f72f851793b372b87888dc3c453";
  private static final BigInteger CHAIN_ID = BigInteger.valueOf(42);
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();

  /** An account in GENESIS_RESOURCE with code, used as a call and delegation target. */
  public static final Address EXISTING_CONTRACT =
      Address.fromHexString("0x0000000000000000000000000000000000007700");

  /** Calldata of the message-call transactions: a function selector and one argument word. */
  public static final Bytes CALL_DATA =
      Bytes.fromHexString(
          "0xabcdef120000000000000000000000000000000000000000000000000000000000000001");

  /** An address that does not exist in GENESIS_RESOURCE. */
  public static final Address EMPTY_ACCOUNT =
      Address.fromHexString("0x00000000000000000000000000000000000dead0");

  /** The preparation charge that halts the top frame. */
  public enum Halt {
    /** The NEW_ACCOUNT state gas of the created contract. */
    CONTRACT_CREATION(MessageFrame.Type.CONTRACT_CREATION),
    /** The NEW_ACCOUNT state gas of an empty recipient that receives value. */
    VALUE_TO_EMPTY_RECIPIENT(MessageFrame.Type.MESSAGE_CALL),
    /** The EIP-7702 state gas of delegating an authority that does not exist yet. */
    DELEGATION_TO_NEW_AUTHORITY(MessageFrame.Type.MESSAGE_CALL);

    private final MessageFrame.Type frameType;

    Halt(final MessageFrame.Type frameType) {
      this.frameType = frameType;
    }

    public MessageFrame.Type frameType() {
      return frameType;
    }
  }

  private final ExecutionContextTestFixture fixture =
      ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS_RESOURCE))
          .dataStorageFormat(DataStorageFormat.BONSAI)
          .build();
  private final BlockHeader header = fixture.getBlockchain().getChainHeadHeader();
  private final ProtocolSpec spec = fixture.getProtocolSchedule().getByBlockHeader(header);
  private final KeyPair sender = keyPair(SENDER_PRIVATE_KEY);
  private final KeyPair authority = SIGNATURE_ALGORITHM.generateKeyPair();
  private final Map<Halt, Transaction> transactions = new EnumMap<>(Halt.class);

  /**
   * Builds the genesis state and signs one transaction per {@link Halt}. Each run gets its own
   * {@link #newWorldUpdater()}, so one instance can be shared by every test in a class.
   */
  public TopFrameHaltTestFixture() {
    for (final Halt halt : Halt.values()) {
      transactions.put(halt, buildTransaction(halt));
    }
  }

  /** The protocol spec the transactions are processed with. */
  public ProtocolSpec spec() {
    return spec;
  }

  /** The authority of the {@link Halt#DELEGATION_TO_NEW_AUTHORITY} transaction. */
  public Address authority() {
    return Address.extract(authority.getPublicKey());
  }

  /** The mining beneficiary the transactions are processed with. */
  public Address coinbase() {
    return header.getCoinbase();
  }

  /** A transaction that halts its top frame on the given preparation charge. */
  public Transaction transaction(final Halt halt) {
    return transactions.get(halt);
  }

  private Transaction buildTransaction(final Halt halt) {
    final Function<Long, Transaction> withGasLimit =
        switch (halt) {
          case CONTRACT_CREATION ->
              gasLimit -> template(gasLimit).payload(Bytes.of(0)).createTransaction(sender);
          case VALUE_TO_EMPTY_RECIPIENT ->
              gasLimit ->
                  template(gasLimit)
                      .to(Optional.of(EMPTY_ACCOUNT))
                      .value(Wei.ONE)
                      .payload(CALL_DATA)
                      .createTransaction(sender);
          case DELEGATION_TO_NEW_AUTHORITY -> {
            final List<CodeDelegation> delegations =
                List.of(
                    TransactionTestFixture.createSignedCodeDelegation(
                        CHAIN_ID, EXISTING_CONTRACT, 0, authority));
            yield gasLimit ->
                template(gasLimit)
                    .type(TransactionType.DELEGATE_CODE)
                    .to(Optional.of(EXISTING_CONTRACT))
                    .payload(CALL_DATA)
                    .codeDelegations(delegations)
                    .createTransaction(sender);
          }
        };
    // The gas limit is signed, so the intrinsic gas is measured on a draft first.
    final GasCalculator gasCalculator = spec.getGasCalculator();
    final Transaction draft = withGasLimit.apply(1_000_000L);
    return withGasLimit.apply(
        Math.max(
            gasCalculator.transactionIntrinsicExecutionGas(draft),
            gasCalculator.transactionFloorCost(draft)));
  }

  /** A world state to process one transaction on, isolated from the shared genesis state. */
  public WorldUpdater newWorldUpdater() {
    // The world state's updater is one shared accumulator, and the transaction processor writes
    // into the updater it is given. A child that nobody commits keeps those writes out of it.
    return fixture.getStateArchive().getWorldState().updater().updater();
  }

  public TransactionProcessingResult process(
      final WorldUpdater worldUpdater, final Transaction tx, final OperationTracer tracer) {
    return spec.getTransactionProcessor()
        .processTransaction(
            worldUpdater,
            header,
            tx,
            header.getCoinbase(),
            tracer,
            (frame, number) -> Hash.ZERO,
            Wei.ZERO);
  }

  public TransactionProcessingResult process(final Transaction tx, final OperationTracer tracer) {
    return process(newWorldUpdater(), tx, tracer);
  }

  private static TransactionTestFixture template(final long gasLimit) {
    return new TransactionTestFixture()
        .type(TransactionType.EIP1559)
        .chainId(Optional.of(CHAIN_ID))
        .maxPriorityFeePerGas(Optional.of(Wei.ZERO))
        .maxFeePerGas(Optional.of(Wei.of(1_000_000_000L)))
        .value(Wei.ZERO)
        .gasLimit(gasLimit);
  }

  private static KeyPair keyPair(final String privateKey) {
    return SIGNATURE_ALGORITHM.createKeyPair(
        SIGNATURE_ALGORITHM.createPrivateKey(Bytes32.fromHexString(privateKey)));
  }
}
