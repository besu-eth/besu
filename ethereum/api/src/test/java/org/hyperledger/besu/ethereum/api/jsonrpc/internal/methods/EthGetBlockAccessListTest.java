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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockAccessListResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.ChainHead;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotRead;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EthGetBlockAccessListTest {
  private final BlockDataGenerator blockDataGenerator = new BlockDataGenerator();
  private EthGetBlockAccessList method;
  private BlockchainQueries blockchainQueries;
  private Blockchain blockchain;

  @BeforeEach
  public void setUp() {
    blockchainQueries = mock(BlockchainQueries.class);
    blockchain = mock(Blockchain.class);

    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);

    method = new EthGetBlockAccessList(blockchainQueries);
  }

  @Test
  public void shouldReturnCorrectMethodName() {
    assertThat(method.getName()).isEqualTo("eth_getBlockAccessList");
  }

  @Test
  public void shouldReturnBlockAccessListForValidBlockNumber() {
    final long blockNumber = 5L;
    final BlockHeader header = blockDataGenerator.header(blockNumber);
    final Hash blockHash = header.getHash();

    when(blockchainQueries.headBlockNumber()).thenReturn(10L);
    when(blockchainQueries.getBlockHashByNumber(blockNumber)).thenReturn(Optional.of(blockHash));
    when(blockchainQueries.getBlockHeaderByHash(blockHash)).thenReturn(Optional.of(header));
    when(blockchainQueries.isBlockAccessListSupported(header)).thenReturn(true);

    final BlockAccessList blockAccessList = sampleBlockAccessList();
    when(blockchain.getBlockAccessList(blockHash)).thenReturn(Optional.of(blockAccessList));

    final JsonRpcResponse response = requestBlockAccessList(String.format("0x%X", blockNumber));

    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    final JsonRpcSuccessResponse successResponse = (JsonRpcSuccessResponse) response;
    assertThat(successResponse.getResult()).isInstanceOf(List.class);

    @SuppressWarnings("unchecked")
    final List<BlockAccessListResult.AccountChangesResult> result =
        (List<BlockAccessListResult.AccountChangesResult>) successResponse.getResult();
    assertThat(result).hasSize(2);
  }

  @Test
  public void shouldReturnBlockAccessListForValidBlockHash() {
    final BlockHeader header = blockDataGenerator.header(5L);
    final Hash blockHash = header.getHash();
    final String blockHashParam = blockHash.toHexString();

    when(blockchainQueries.getBlockHeaderByHash(blockHash)).thenReturn(Optional.of(header));
    when(blockchainQueries.isBlockAccessListSupported(header)).thenReturn(true);
    when(blockchain.getBlockAccessList(blockHash)).thenReturn(Optional.of(sampleBlockAccessList()));

    final JsonRpcResponse response = requestBlockAccessList(blockHashParam);

    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    final JsonRpcSuccessResponse successResponse = (JsonRpcSuccessResponse) response;
    assertThat(successResponse.getResult()).isInstanceOf(List.class);
  }

  @Test
  public void shouldReturnNullForUnknownBlockHash() {
    final Hash unknownHash =
        Hash.fromHexString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

    when(blockchainQueries.getBlockHeaderByHash(unknownHash)).thenReturn(Optional.empty());

    final JsonRpcResponse response = requestBlockAccessList(unknownHash.toHexString());

    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    assertThat(((JsonRpcSuccessResponse) response).getResult()).isNull();
  }

  @Test
  public void shouldReturnNullForUnknownBlockNumber() {
    final long blockNumber = 5L;

    when(blockchainQueries.headBlockNumber()).thenReturn(10L);
    when(blockchainQueries.getBlockHashByNumber(blockNumber)).thenReturn(Optional.empty());
    when(blockchainQueries.getBlockHeaderByHash(Hash.EMPTY)).thenReturn(Optional.empty());

    final JsonRpcResponse response = requestBlockAccessList(String.format("0x%X", blockNumber));

    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    assertThat(((JsonRpcSuccessResponse) response).getResult()).isNull();
  }

  @Test
  public void shouldReturnErrorForPreAmsterdamBlocks() {
    final long blockNumber = 5L;
    final BlockHeader header = blockDataGenerator.header(blockNumber);
    final Hash blockHash = header.getHash();

    when(blockchainQueries.headBlockNumber()).thenReturn(10L);
    when(blockchainQueries.getBlockHashByNumber(blockNumber)).thenReturn(Optional.of(blockHash));
    when(blockchainQueries.getBlockHeaderByHash(blockHash)).thenReturn(Optional.of(header));
    when(blockchainQueries.isBlockAccessListSupported(header)).thenReturn(false);

    final JsonRpcResponse response = requestBlockAccessList(String.format("0x%X", blockNumber));

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    final JsonRpcErrorResponse errorResponse = (JsonRpcErrorResponse) response;
    assertThat(errorResponse.getErrorType()).isEqualTo(RpcErrorType.RESOURCE_NOT_FOUND);
  }

  @Test
  public void shouldReturnErrorWhenAccessListIsPruned() {
    final long blockNumber = 5L;
    final BlockHeader header = blockDataGenerator.header(blockNumber);
    final Hash blockHash = header.getHash();

    when(blockchainQueries.headBlockNumber()).thenReturn(10L);
    when(blockchainQueries.getBlockHashByNumber(blockNumber)).thenReturn(Optional.of(blockHash));
    when(blockchainQueries.getBlockHeaderByHash(blockHash)).thenReturn(Optional.of(header));
    when(blockchainQueries.isBlockAccessListSupported(header)).thenReturn(true);
    when(blockchain.getBlockAccessList(blockHash)).thenReturn(Optional.empty());

    final JsonRpcResponse response = requestBlockAccessList(String.format("0x%X", blockNumber));

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    final JsonRpcErrorResponse errorResponse = (JsonRpcErrorResponse) response;
    assertThat(errorResponse.getErrorType()).isEqualTo(RpcErrorType.PRUNED_HISTORY_UNAVAILABLE);
  }

  @Test
  public void shouldHandleLatestBlockParameter() {
    final long blockNumber = 100L;
    final BlockHeader latestHeader = blockDataGenerator.header(blockNumber);
    final Hash blockHash = latestHeader.getHash();
    final ChainHead chainHead = mock(ChainHead.class);

    when(blockchain.getChainHead()).thenReturn(chainHead);
    when(chainHead.getBlockHeader()).thenReturn(latestHeader);
    when(blockchainQueries.getBlockHeaderByHash(blockHash)).thenReturn(Optional.of(latestHeader));
    when(blockchainQueries.isBlockAccessListSupported(latestHeader)).thenReturn(true);
    when(blockchain.getBlockAccessList(blockHash))
        .thenReturn(Optional.of(new BlockAccessList(Collections.emptyList())));

    final JsonRpcResponse response = requestBlockAccessList("latest");

    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
  }

  @Test
  public void shouldReturnErrorWhenAccessListIsNotAvailableForEarliest() {
    final BlockHeader genesisHeader = blockDataGenerator.header(0L);
    final Hash blockHash = genesisHeader.getHash();

    when(blockchainQueries.headBlockNumber()).thenReturn(10L);
    when(blockchainQueries.getBlockHashByNumber(0L)).thenReturn(Optional.of(blockHash));
    when(blockchainQueries.getBlockHeaderByHash(blockHash)).thenReturn(Optional.of(genesisHeader));
    when(blockchainQueries.isBlockAccessListSupported(genesisHeader)).thenReturn(false);

    final JsonRpcResponse response = requestBlockAccessList("earliest");

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    final JsonRpcErrorResponse errorResponse = (JsonRpcErrorResponse) response;
    assertThat(errorResponse.getErrorType()).isEqualTo(RpcErrorType.RESOURCE_NOT_FOUND);
  }

  @Test
  public void shouldThrowInvalidJsonRpcParametersForInvalidParameter() {
    final JsonRpcRequestContext requestContext =
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", method.getName(), new Object[] {"invalid"}));

    assertThatThrownBy(() -> method.response(requestContext))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessageContaining("Invalid block or block hash parameters (index 0)");
  }

  @Test
  public void shouldThrowInvalidJsonRpcParametersForMissingParameter() {
    final JsonRpcRequestContext requestContext =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", method.getName(), new Object[] {}));

    assertThatThrownBy(() -> method.response(requestContext))
        .isInstanceOf(InvalidJsonRpcParameters.class);
  }

  private JsonRpcResponse requestBlockAccessList(final String blockParameter) {
    return method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", method.getName(), new Object[] {blockParameter})));
  }

  private BlockAccessList sampleBlockAccessList() {
    final Address address1 = Address.fromHexString("0x1234567890123456789012345678901234567890");
    final Address address2 = Address.fromHexString("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

    final StorageSlotKey slot1 = new StorageSlotKey(UInt256.ONE);
    final StorageSlotKey slot2 = new StorageSlotKey(UInt256.valueOf(2));

    final StorageChange storageChange1 = new StorageChange(1, UInt256.valueOf(100));
    final StorageChange storageChange2 = new StorageChange(2, UInt256.valueOf(200));

    final SlotChanges slotChanges1 = new SlotChanges(slot1, List.of(storageChange1));
    final SlotChanges slotChanges2 = new SlotChanges(slot2, List.of(storageChange2));

    final AccountChanges accountChanges1 =
        new AccountChanges(
            address1,
            List.of(slotChanges1),
            Collections.emptyList(),
            List.of(new BalanceChange(1, Wei.of(1000))),
            List.of(new NonceChange(1, 5L)),
            Collections.emptyList());

    final AccountChanges accountChanges2 =
        new AccountChanges(
            address2,
            List.of(slotChanges2),
            List.of(new SlotRead(slot1)),
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(new CodeChange(1, Bytes.fromHexString("0x60806040"))));

    return new BlockAccessList(List.of(accountChanges1, accountChanges2));
  }
}
