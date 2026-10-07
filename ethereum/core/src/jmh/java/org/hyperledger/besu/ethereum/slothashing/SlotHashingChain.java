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
package org.hyperledger.besu.ethereum.slothashing;

import static org.mockito.Mockito.mock;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.blockcreation.AbstractBlockCreator;
import org.hyperledger.besu.ethereum.blockcreation.BlockCreator.BlockCreationResult;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.MutableInitValues;
import org.hyperledger.besu.ethereum.core.SealableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * A Bonsai chain with a contract and funded senders, and a block whose transactions all read the
 * same hot slots of the contract and each write slots of their own.
 */
final class SlotHashingChain implements AutoCloseable {

  private static final String GENESIS =
      "/org/hyperledger/besu/ethereum/slothashing/slot-hashing-genesis.json";
  private static final Address CONTRACT =
      Address.fromHexString("0x00000000000000000000000000000000005107");
  private static final Address COINBASE =
      Address.fromHexString("0x000000000000000000000000000000000000c0b5");

  /** Reads slots [0, word 2) then sets slots [word 0, word 0 + word 1) to 1. */
  private static final String CONTRACT_CODE =
      "0x60005b8060403511156015578054506001016002565b5060005b80602035111560315760018160003501556001016019565b00";

  private static final long WRITE_SLOTS_BASE = 1_000_000L;

  private final ExecutionContextTestFixture context;
  private final List<Transaction> transactions;
  private final EthScheduler ethScheduler = new EthScheduler(1, 1, 1, new NoOpMetricsSystem());
  private final AbstractBlockCreator blockCreator;

  SlotHashingChain(
      final boolean amsterdam,
      final int transactionCount,
      final int sharedReadSlots,
      final int ownWriteSlots) {
    final List<KeyPair> senders = new ArrayList<>(transactionCount);
    for (int i = 0; i < transactionCount; i++) {
      senders.add(
          SignatureAlgorithmFactory.getInstance()
              .createKeyPair(
                  SECPPrivateKey.create(
                      UInt256.valueOf(i + 1L).toBytes(), SignatureAlgorithm.ALGORITHM)));
    }
    context =
        ExecutionContextTestFixture.builder(
                GenesisConfig.fromConfig(genesisJson(amsterdam, senders, sharedReadSlots)))
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();

    transactions = new ArrayList<>(transactionCount);
    for (int i = 0; i < transactionCount; i++) {
      final Bytes callData =
          Bytes.concatenate(
              UInt256.valueOf(WRITE_SLOTS_BASE + (long) i * ownWriteSlots),
              UInt256.valueOf(ownWriteSlots),
              UInt256.valueOf(sharedReadSlots));
      transactions.add(
          Transaction.builder()
              .type(TransactionType.EIP1559)
              .nonce(0)
              .maxPriorityFeePerGas(Wei.ZERO)
              .maxFeePerGas(Wei.of(10))
              .gasLimit(5_000_000L)
              .to(CONTRACT)
              .value(Wei.ZERO)
              .payload(callData)
              .chainId(BigInteger.valueOf(42))
              .signAndBuild(senders.get(i)));
    }
    blockCreator = new BlockCreatorForBenchmark(context, ethScheduler);
  }

  ExecutionContextTestFixture context() {
    return context;
  }

  /** Builds the next block with every transaction, as a block producer does. */
  BlockCreationResult createBlock() {
    final BlockHeader genesis = context.getBlockchain().getChainHeadHeader();
    final BlockCreationResult result =
        blockCreator.createBlock(
            Optional.of(transactions), Optional.empty(), genesis.getTimestamp() + 1, genesis);
    final int included = result.getBlock().getBody().getTransactions().size();
    if (included != transactions.size()) {
      throw new IllegalStateException(
          included + " transactions out of " + transactions.size() + " fit in the block");
    }
    return result;
  }

  /** A new world state at genesis, to process the next block on. */
  BonsaiWorldState genesisWorldState() {
    return (BonsaiWorldState)
        context
            .getStateArchive()
            .getWorldState(
                WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(
                    context.getBlockchain().getChainHeadHeader()))
            .orElseThrow();
  }

  @Override
  public void close() {
    ethScheduler.stop();
  }

  /** The base genesis plus the funded senders and the contract with its hot slots set. */
  private static String genesisJson(
      final boolean amsterdam, final List<KeyPair> senders, final int sharedReadSlots) {
    final StringBuilder alloc = new StringBuilder();
    for (final KeyPair sender : senders) {
      alloc
          .append('"')
          .append(Util.publicKeyToAddress(sender.getPublicKey()).toHexString())
          .append("\": {\"balance\": \"0xde0b6b3a7640000\"},");
    }
    final StringBuilder storage = new StringBuilder();
    for (int slot = 0; slot < sharedReadSlots; slot++) {
      storage
          .append(slot == 0 ? "" : ",")
          .append('"')
          .append(Bytes32.leftPad(Bytes.ofUnsignedLong(slot)).toHexString())
          .append("\": \"0x01\"");
    }
    alloc
        .append('"')
        .append(CONTRACT.toHexString())
        .append("\": {\"balance\": \"0x0\", \"code\": \"")
        .append(CONTRACT_CODE)
        .append("\", \"storage\": {")
        .append(storage)
        .append("}},");
    final String json = readGenesis().replace("\"alloc\": {", "\"alloc\": {" + alloc);
    return amsterdam ? json : json.replace("\"amsterdamTime\": 0,", "");
  }

  private static String readGenesis() {
    try (InputStream in = SlotHashingChain.class.getResourceAsStream(GENESIS)) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Builds blocks from the given transactions, the transaction pool is not used. */
  private static final class BlockCreatorForBenchmark extends AbstractBlockCreator {

    BlockCreatorForBenchmark(
        final ExecutionContextTestFixture context, final EthScheduler ethScheduler) {
      super(
          ImmutableMiningConfiguration.builder()
              .mutableInitValues(
                  MutableInitValues.builder()
                      .coinbase(COINBASE)
                      .minTransactionGasPrice(Wei.ZERO)
                      .build())
              .build(),
          (timestamp, header) -> COINBASE,
          parent -> Bytes.EMPTY,
          mock(TransactionPool.class),
          context.getProtocolContext(),
          context.getProtocolSchedule(),
          ethScheduler);
    }

    @Override
    protected BlockHeader createFinalBlockHeader(final SealableBlockHeader sealableBlockHeader) {
      return BlockHeaderBuilder.create()
          .difficulty(Difficulty.ZERO)
          .populateFrom(sealableBlockHeader)
          .mixHash(Hash.EMPTY)
          .nonce(0L)
          .blockHeaderFunctions(blockHeaderFunctions)
          .buildBlockHeader();
    }
  }
}
