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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionReceiptRootResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionReceiptStatusResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionReceiptWithMetadata;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.TransactionLocation;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.BlockGasAccountingStrategy;
import org.hyperledger.besu.ethereum.mainnet.BlockGasUsedValidator;
import org.hyperledger.besu.ethereum.mainnet.CancunTargetingGasLimitCalculator;
import org.hyperledger.besu.ethereum.mainnet.PoWHasher;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.blockhash.FrontierPreExecutionProcessor;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.DefaultStateRootCommitterFactory;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;

import java.math.BigInteger;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256s;
import org.junit.jupiter.api.Test;

public class EthGetTransactionReceiptTest {
  private enum TestHardforkId implements HardforkId {
    ROOT,
    STATUS;

    @Override
    public boolean finalized() {
      return true;
    }

    @Override
    public String description() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  private static final int MAX_BLOBS_PER_BLOCK = 6;
  private static final int TARGET_BLOBS_PER_BLOCK = 3;
  private final TransactionReceipt statusReceipt =
      new TransactionReceipt(1, 12, Collections.emptyList(), Optional.empty());
  private final Hash stateRoot =
      Hash.fromHexString("0000000000000000000000000000000000000000000000000000000000000000");
  private final TransactionReceipt rootReceipt =
      new TransactionReceipt(stateRoot, 12, Collections.emptyList(), Optional.empty());

  private final SECPSignature signature =
      SignatureAlgorithmFactory.getInstance()
          .createSignature(BigInteger.ONE, BigInteger.TEN, (byte) 1);
  private final Address sender =
      Address.fromHexString("0x0000000000000000000000000000000000000003");
  private final Transaction transaction =
      Transaction.builder()
          .nonce(1)
          .gasPrice(Wei.of(12))
          .gasLimit(43)
          .payload(Bytes.EMPTY)
          .value(Wei.ZERO)
          .signature(signature)
          .sender(sender)
          .guessType()
          .build();

  private final Hash hash =
      Hash.fromHexString("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
  private final Hash blockHash =
      Hash.fromHexString("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

  private final TransactionReceiptWithMetadata statusReceiptWithMetadata =
      TransactionReceiptWithMetadata.create(
          statusReceipt,
          transaction,
          hash,
          1,
          2,
          Optional.empty(),
          blockHash,
          1234,
          4,
          Optional.empty(),
          Optional.empty(),
          0);
  private final TransactionReceiptWithMetadata rootReceiptWithMetaData =
      TransactionReceiptWithMetadata.create(
          rootReceipt,
          transaction,
          hash,
          1,
          2,
          Optional.empty(),
          blockHash,
          1234,
          4,
          Optional.empty(),
          Optional.empty(),
          0);

  private final ProtocolSpec rootTransactionTypeSpec =
      new ProtocolSpec(
          TestHardforkId.ROOT,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          BlockHeader::getCoinbase,
          null,
          false,
          null,
          GasLimitCalculator.constant(),
          FeeMarket.legacy(),
          Optional.of(PoWHasher.ETHASH_LIGHT),
          null,
          Optional.empty(),
          null,
          Optional.empty(),
          new FrontierPreExecutionProcessor(),
          true,
          Duration.ofSeconds(12),
          true,
          Optional.empty(),
          Optional.empty(),
          new DefaultStateRootCommitterFactory(),
          BlockGasAccountingStrategy.FRONTIER,
          BlockGasUsedValidator.FRONTIER);
  private final ProtocolSpec statusTransactionTypeSpec =
      new ProtocolSpec(
          TestHardforkId.STATUS,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          BlockHeader::getCoinbase,
          null,
          false,
          null,
          GasLimitCalculator.constant(),
          FeeMarket.legacy(),
          Optional.of(PoWHasher.ETHASH_LIGHT),
          null,
          Optional.empty(),
          null,
          Optional.empty(),
          new FrontierPreExecutionProcessor(),
          true,
          Duration.ofSeconds(12),
          true,
          Optional.empty(),
          Optional.empty(),
          new DefaultStateRootCommitterFactory(),
          BlockGasAccountingStrategy.FRONTIER,
          BlockGasUsedValidator.FRONTIER);

  @SuppressWarnings("unchecked")
  private final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);

  private final Blockchain blockchain = mock(Blockchain.class);

  private final BlockchainQueries blockchainQueries =
      spy(
          new BlockchainQueries(
              protocolSchedule,
              blockchain,
              mock(WorldStateArchive.class),
              MiningConfiguration.newDefault()));
  private final EthGetTransactionReceipt ethGetTransactionReceipt =
      new EthGetTransactionReceipt(blockchainQueries, protocolSchedule);
  private final String receiptString =
      "0xcbef69eaf44af151aa66677ae4b8d8c343a09f667c873a3a6f4558fa4051fa5f";
  private final Hash receiptHash =
      Hash.fromHexString("cbef69eaf44af151aa66677ae4b8d8c343a09f667c873a3a6f4558fa4051fa5f");
  Object[] params = new Object[] {receiptString};
  private final JsonRpcRequestContext request =
      new JsonRpcRequestContext(new JsonRpcRequest("1", "eth_getTransactionReceipt", params));

  @Test
  public void shouldCreateAStatusTransactionReceiptWhenStatusTypeProtocol() {
    when(blockchainQueries.headBlockNumber()).thenReturn(1L);
    when(blockchainQueries.transactionReceiptByTransactionHash(receiptHash, protocolSchedule))
        .thenReturn(Optional.of(statusReceiptWithMetadata));
    when(protocolSchedule.getByBlockHeader(blockHeader(1))).thenReturn(statusTransactionTypeSpec);

    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse) ethGetTransactionReceipt.response(request);
    final TransactionReceiptStatusResult result =
        (TransactionReceiptStatusResult) response.getResult();

    assertThat(result.getType()).isEqualTo("0x0");
    assertThat(result.getStatus()).isEqualTo("0x1");
  }

  @Test
  public void shouldCreateARootTransactionReceiptWhenRootTypeProtocol() {
    when(blockchainQueries.headBlockNumber()).thenReturn(1L);
    when(blockchainQueries.transactionReceiptByTransactionHash(receiptHash, protocolSchedule))
        .thenReturn(Optional.of(rootReceiptWithMetaData));
    when(protocolSchedule.getByBlockHeader(blockHeader(1))).thenReturn(rootTransactionTypeSpec);

    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse) ethGetTransactionReceipt.response(request);
    final TransactionReceiptRootResult result = (TransactionReceiptRootResult) response.getResult();

    assertThat(result.getType()).isEqualTo("0x0");
    assertThat(result.getRoot()).isEqualTo(stateRoot.toString());
  }

  @Test
  public void shouldWorkFor1559Txs() {
    when(blockchainQueries.headBlockNumber()).thenReturn(1L);
    final Transaction transaction1559 =
        new BlockDataGenerator().transaction(TransactionType.EIP1559);
    final Wei baseFee = Wei.ONE;
    final TransactionReceiptWithMetadata transactionReceiptWithMetadata =
        TransactionReceiptWithMetadata.create(
            statusReceipt,
            transaction1559,
            hash,
            1,
            2,
            Optional.of(baseFee),
            blockHash,
            1234,
            4,
            Optional.empty(),
            Optional.empty(),
            0);
    when(blockchainQueries.transactionReceiptByTransactionHash(receiptHash, protocolSchedule))
        .thenReturn(Optional.of(transactionReceiptWithMetadata));
    when(protocolSchedule.getByBlockHeader(blockHeader(1))).thenReturn(rootTransactionTypeSpec);

    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse) ethGetTransactionReceipt.response(request);
    final TransactionReceiptStatusResult result =
        (TransactionReceiptStatusResult) response.getResult();

    assertThat(result.getStatus()).isEqualTo("0x1");
    assertThat(result.getType()).isEqualTo("0x2");
    assertThat(Wei.fromHexString(result.getEffectiveGasPrice()))
        .isEqualTo(
            UInt256s.min(
                baseFee.add(transaction1559.getMaxPriorityFeePerGas().get()),
                transaction1559.getMaxFeePerGas().get()));
  }

  /**
   * Test case to verify that the TransactionReceiptStatusResult contains blob gas used and blob gas
   * price when the transaction type is TransactionType#BLOB
   */
  @Test
  public void shouldContainBlobGasUsedAndBlobGasPriceWhenBlobTransaction() {

    var hash = Hash.wrap(Bytes32.random());
    mockBlockWithBlobTransaction(hash, 1L);
    when(blockchain.getTxReceipts(hash)).thenReturn(Optional.of(List.of(statusReceipt)));
    // Call the real method to get the transaction receipt by transaction hash
    when(blockchainQueries.transactionReceiptByTransactionHash(receiptHash, protocolSchedule))
        .thenCallRealMethod();

    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse) ethGetTransactionReceipt.response(request);
    final TransactionReceiptStatusResult result =
        (TransactionReceiptStatusResult) response.getResult();

    assertThat(result.getType()).isEqualTo("0x3");
    assertThat(result.getBlobGasUsed()).isEqualTo("0x20000");
    assertThat(result.getBlobGasPrice()).isEqualTo("0x1");
  }

  private void mockBlockWithBlobTransaction(final Hash blockHash, final long blockNumber) {
    Hash parentHash = Hash.wrap(Bytes32.random());
    TransactionLocation transactionLocation = mock(TransactionLocation.class);
    Block block = mock(Block.class);
    BlockBody body = mock(BlockBody.class);
    BlockHeader header = mock(BlockHeader.class);
    BlockHeader parentHeader = mock(BlockHeader.class);
    when(transactionLocation.getBlockHash()).thenReturn(blockHash);
    when(transactionLocation.getTransactionIndex()).thenReturn(0);
    when(header.getNumber()).thenReturn(blockNumber);
    when(header.getBlockHash()).thenReturn(blockHash);
    when(header.getParentHash()).thenReturn(parentHash);
    when(blockchain.getBlockHeader(parentHash)).thenReturn(Optional.of(parentHeader));
    when(block.getHeader()).thenReturn(header);
    when(block.getBody()).thenReturn(body);
    when(body.getTransactions())
        .thenReturn(List.of(new BlockDataGenerator().transaction(TransactionType.BLOB)));
    when(parentHeader.getExcessBlobGas()).thenReturn(Optional.of(BlobGas.of(1000)));
    when(blockchain.getBlockByHash(blockHash)).thenReturn(Optional.of(block));
    mockProtocolSpec(header);
    when(blockchain.getTransactionLocation(receiptHash))
        .thenReturn(Optional.of(transactionLocation));
  }

  private void mockProtocolSpec(final BlockHeader blockHeader) {
    FeeMarket feeMarket = FeeMarket.cancunDefault(0, Optional.empty());
    ProtocolSpec spec = mock(ProtocolSpec.class);
    when(spec.getFeeMarket()).thenReturn(feeMarket);
    when(spec.getGasCalculator()).thenReturn(new CancunGasCalculator());
    when(spec.getGasLimitCalculator())
        .thenReturn(
            new CancunTargetingGasLimitCalculator(
                0L,
                FeeMarket.cancunDefault(0L, Optional.empty()),
                new CancunGasCalculator(),
                MAX_BLOBS_PER_BLOCK,
                TARGET_BLOBS_PER_BLOCK));
    when(protocolSchedule.getByBlockHeader(blockHeader)).thenReturn(spec);
  }

  private BlockHeader blockHeader(final long number) {
    return new BlockHeaderTestFixture().number(number).buildHeader();
  }
}
