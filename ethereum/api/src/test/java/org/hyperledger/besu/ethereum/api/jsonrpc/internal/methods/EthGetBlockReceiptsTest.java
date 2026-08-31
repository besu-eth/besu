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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockReceiptsResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionReceiptLogResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionReceiptResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionReceiptWithMetadata;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EthGetBlockReceiptsTest {

  private static final int BLOCKCHAIN_LENGTH = 5;
  private static final String ZERO_HASH = String.valueOf(Hash.ZERO);
  private static final String HASH_63_CHARS_LONG =
      "0xd3d3d1340c085e1b14182e01fd0b7cc5b585dca77f809f78fcca3e1a165b189";
  private static final String ETH_METHOD = "eth_getBlockReceipts";
  private static final String JSON_RPC_VERSION = "2.0";

  @Mock private BlockchainQueries blockchainQueries;
  @Mock private WorldStateArchive worldStateArchive;
  private MutableBlockchain blockchain;
  private static final BlockDataGenerator blockDataGenerator = new BlockDataGenerator();
  private EthGetBlockReceipts method;
  private ProtocolSchedule protocolSchedule;
  final JsonRpcResponse blockNotFoundResponse = new JsonRpcSuccessResponse(null, null);

  @BeforeEach
  public void setUp() {
    blockchain = createInMemoryBlockchain(blockDataGenerator.genesisBlock());

    for (int i = 1; i < BLOCKCHAIN_LENGTH; i++) {
      final BlockDataGenerator.BlockOptions options =
          new BlockDataGenerator.BlockOptions()
              .setBlockNumber(i)
              .setParentHash(blockchain.getBlockHashByNumber(i - 1).orElseThrow());
      final Block block = blockDataGenerator.block(options);
      final List<TransactionReceipt> receipts = blockDataGenerator.receipts(block);

      blockchain.appendBlock(block, receipts);
    }

    blockchainQueries =
        spy(
            new BlockchainQueries(
                protocolSchedule, blockchain, worldStateArchive, MiningConfiguration.newDefault()));
    protocolSchedule = mock(ProtocolSchedule.class);
    method = new EthGetBlockReceipts(blockchainQueries, protocolSchedule);
  }

  @Test
  public void returnsCorrectMethodName() {
    assertThat(method.getName()).isEqualTo(ETH_METHOD);
  }

  @Test
  public void exceptionWhenNoParamsSupplied() {
    assertThatThrownBy(() -> method.response(requestWithParams()))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessage("Invalid block or block hash parameters (index 0)");
    verifyNoMoreInteractions(blockchainQueries);
  }

  @Test
  public void exceptionWhenBlockNumberTooLarge() {
    assertThatThrownBy(() -> method.response(requestWithParams("0x1212121212121212121212")))
        .isInstanceOf(InvalidJsonRpcParameters.class);
    verifyNoMoreInteractions(blockchainQueries);
  }

  @Test
  public void twoReceiptsForLatestBlock() {

    // Read expected transactions from the generated blockchain
    final Transaction expectedTx1 =
        blockchain.getBlockByNumber(BLOCKCHAIN_LENGTH - 1).get().getBody().getTransactions().get(0);
    final Transaction expectedTx2 =
        blockchain.getBlockByNumber(BLOCKCHAIN_LENGTH - 1).get().getBody().getTransactions().get(1);

    /* Block generator defaults to 2 transactions per mocked block */
    JsonRpcResponse actualResponse = method.response(requestWithParams("latest"));
    assertThat(actualResponse).isInstanceOf(JsonRpcSuccessResponse.class);
    final BlockReceiptsResult result =
        (BlockReceiptsResult) ((JsonRpcSuccessResponse) actualResponse).getResult();

    assertThat(result.getResults().size()).isEqualTo(2);

    // Check TX1 receipt is correct
    TransactionReceiptResult tx1 = result.getResults().get(0);
    assertThat(tx1.getBlockNumber()).isEqualTo("0x" + (BLOCKCHAIN_LENGTH - 1));
    assertThat(tx1.getEffectiveGasPrice()).isNotEmpty();
    assertThat(tx1.getTo()).isEqualTo(expectedTx1.getTo().get().toString());
    assertThat(tx1.getType())
        .isEqualTo(String.format("0x%X", expectedTx1.getType().getEthSerializedType()));

    // Check TX2 receipt is correct
    TransactionReceiptResult tx2 = result.getResults().get(1);
    assertThat(tx2.getBlockNumber()).isEqualTo("0x" + (BLOCKCHAIN_LENGTH - 1));
    assertThat(tx2.getEffectiveGasPrice()).isNotEmpty();
    assertThat(tx2.getTo()).isEqualTo(expectedTx2.getTo().get().toString());
    assertThat(tx2.getType())
        .isEqualTo(String.format("0x%X", expectedTx2.getType().getEthSerializedType()));
  }

  @Test
  public void twoReceiptsForBlockOne() {

    // Read expected transactions from the generated blockchain
    final Transaction expectedTx1 =
        blockchain.getBlockByNumber(1).get().getBody().getTransactions().get(0);
    final Transaction expectedTx2 =
        blockchain.getBlockByNumber(1).get().getBody().getTransactions().get(1);

    /* Block generator defaults to 2 transactions per block */
    JsonRpcResponse actualResponse = method.response(requestWithParams("0x01"));
    assertThat(actualResponse).isInstanceOf(JsonRpcSuccessResponse.class);
    final BlockReceiptsResult result =
        (BlockReceiptsResult) ((JsonRpcSuccessResponse) actualResponse).getResult();

    assertThat(result.getResults().size()).isEqualTo(2);

    // Check TX1 receipt is correct
    TransactionReceiptResult tx1 = result.getResults().get(0);
    assertThat(tx1.getBlockNumber()).isEqualTo("0x1");
    assertThat(tx1.getEffectiveGasPrice()).isNotEmpty();
    assertThat(tx1.getTo()).isEqualTo(expectedTx1.getTo().get().toString());
    assertThat(tx1.getType())
        .isEqualTo(String.format("0x%X", expectedTx1.getType().getEthSerializedType()));

    // Check TX2 receipt is correct
    TransactionReceiptResult tx2 = result.getResults().get(1);
    assertThat(tx2.getBlockNumber()).isEqualTo("0x1");
    assertThat(tx2.getEffectiveGasPrice()).isNotEmpty();
    assertThat(tx2.getTo()).isEqualTo(expectedTx2.getTo().get().toString());
    assertThat(tx2.getType())
        .isEqualTo(String.format("0x%X", expectedTx2.getType().getEthSerializedType()));
  }

  @Test
  public void blockNotFoundWhenHash63CharsLong() {
    /* Valid hash with 63 chars in - should result in block not found */
    JsonRpcResponse actualResponse = method.response(requestWithParams(HASH_63_CHARS_LONG));
    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(blockNotFoundResponse);
  }

  @Test
  public void blockNotFoundForZeroHash() {
    /* Zero hash - should result in block not found */
    JsonRpcResponse actualResponse = method.response(requestWithParams(ZERO_HASH));
    assertThat(actualResponse).usingRecursiveComparison().isEqualTo(blockNotFoundResponse);
  }

  @Test
  public void shouldReturnReceiptsWithRemovedTrueWhenBlockIsNotOnCanonicalChain() {
    final Hash blockHash = blockchain.getBlockHashByNumber(1).orElseThrow();

    // Instantiate a local queries object to bypass ErrorProne DirectInvocationOnMock check on the
    // field
    final BlockchainQueries localQueries =
        new BlockchainQueries(
            protocolSchedule, blockchain, worldStateArchive, MiningConfiguration.newDefault());

    // Get the real receipts first
    final List<TransactionReceiptWithMetadata> canonicalReceipts =
        localQueries.transactionReceiptsByBlockHash(blockHash, protocolSchedule).orElseThrow();

    // Map them to non-canonical (removed = true) receipts
    final List<TransactionReceiptWithMetadata> nonCanonicalReceipts =
        canonicalReceipts.stream()
            .map(
                receipt ->
                    TransactionReceiptWithMetadata.create(
                        receipt.getReceipt(),
                        receipt.getTransaction(),
                        receipt.getTransactionHash(),
                        receipt.getTransactionIndex(),
                        receipt.getGasUsed(),
                        receipt.getBaseFee(),
                        receipt.getBlockHash(),
                        receipt.getBlockTimestamp(),
                        receipt.getBlockNumber(),
                        receipt.getBlobGasUsed(),
                        receipt.getBlobGasPrice(),
                        receipt.getLogIndexOffset(),
                        true)) // removed = true
            .collect(java.util.stream.Collectors.toList());

    // Stub the spy to return the non-canonical receipts
    org.mockito.Mockito.doReturn(java.util.Optional.of(nonCanonicalReceipts))
        .when(blockchainQueries)
        .transactionReceiptsByBlockHash(blockHash, protocolSchedule);

    final JsonRpcResponse actualResponse = method.response(requestWithParams(blockHash.toString()));
    assertThat(actualResponse).isInstanceOf(JsonRpcSuccessResponse.class);
    final BlockReceiptsResult result =
        (BlockReceiptsResult) ((JsonRpcSuccessResponse) actualResponse).getResult();

    assertThat(result.getResults()).isNotEmpty();
    for (final TransactionReceiptResult receiptResult : result.getResults()) {
      assertThat(receiptResult.getLogs()).isNotEmpty();
      for (final TransactionReceiptLogResult logResult : receiptResult.getLogs()) {
        assertThat(logResult.isRemoved()).isTrue();
      }
    }
  }

  private JsonRpcRequestContext requestWithParams(final Object... params) {
    return new JsonRpcRequestContext(new JsonRpcRequest(JSON_RPC_VERSION, ETH_METHOD, params));
  }
}
