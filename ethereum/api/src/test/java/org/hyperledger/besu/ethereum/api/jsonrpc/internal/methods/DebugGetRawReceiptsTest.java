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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class DebugGetRawReceiptsTest {

  private BlockchainQueries blockchainQueries;
  private Blockchain blockchain;
  private DebugGetRawReceipts method;

  @BeforeEach
  public void setUp() {
    blockchainQueries = mock(BlockchainQueries.class);
    blockchain = mock(Blockchain.class);
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    method = new DebugGetRawReceipts(blockchainQueries);
  }

  @Test
  public void returnsNullForMissingBlock() {
    final long missingBlockNumber = 999_999_999L;
    when(blockchainQueries.headBlockNumber()).thenReturn(missingBlockNumber);
    when(blockchainQueries.getBlockHashByNumber(missingBlockNumber)).thenReturn(Optional.empty());
    when(blockchain.getTxReceipts(Hash.EMPTY)).thenReturn(Optional.empty());

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0",
                "debug_getRawReceipts",
                new Object[] {"0x" + Long.toHexString(missingBlockNumber)}));

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);
    assertThat(response.getResult()).isNull();
  }

  @Test
  public void returnsNullForFutureBlock() {
    final long futureBlockNumber = 999_999_999_999L;
    when(blockchainQueries.headBlockNumber()).thenReturn(42L);

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0",
                "debug_getRawReceipts",
                new Object[] {"0x" + Long.toHexString(futureBlockNumber)}));

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);
    assertThat(response.getResult()).isNull();
  }

  @Test
  public void returnsEmptyArrayForBlockWithNoReceipts() {
    final long blockNumber = 42L;
    final Hash blockHash = Hash.fromHexStringLenient("0x1234");
    when(blockchainQueries.headBlockNumber()).thenReturn(blockNumber);
    when(blockchainQueries.getBlockHashByNumber(blockNumber)).thenReturn(Optional.of(blockHash));
    when(blockchain.getTxReceipts(blockHash)).thenReturn(Optional.of(java.util.List.of()));

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0",
                "debug_getRawReceipts",
                new Object[] {"0x" + Long.toHexString(blockNumber)}));

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);
    assertThat((String[]) response.getResult()).isEmpty();
  }

  @Test
  public void returnsReceiptsForBlockHash() {
    final Hash blockHash =
        Hash.fromHexString("0xd69523419c14a8c91d66477868b5ac215d792b4ea7047542fc6e20d48f50db75");
    final BlockHeader blockHeader = mock(BlockHeader.class);
    final TransactionReceipt receipt =
        new TransactionReceipt(
            TransactionType.EIP1559,
            1,
            100L,
            Collections.singletonList(new BlockDataGenerator().log()),
            Optional.empty());
    when(blockchainQueries.getBlockHeaderByHash(blockHash)).thenReturn(Optional.of(blockHeader));
    when(blockchain.getTxReceipts(blockHash)).thenReturn(Optional.of(List.of(receipt)));

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0", "debug_getRawReceipts", new Object[] {blockHash.getBytes().toHexString()}));

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);
    final String[] results = (String[]) response.getResult();
    assertThat(results).hasSize(1);
    final Bytes raw = Bytes.fromHexString(results[0]);
    assertThat(raw.get(0)).isEqualTo(TransactionType.EIP1559.getSerializedType());
  }

  @Test
  public void returnsNullForUnknownBlockHash() {
    final Hash blockHash =
        Hash.fromHexString("0xb8a651cb280e169015aef5235a141cb2d905058d1ff9bba788b7ad2c729c9837");
    when(blockchainQueries.getBlockHeaderByHash(blockHash)).thenReturn(Optional.empty());

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0", "debug_getRawReceipts", new Object[] {blockHash.getBytes().toHexString()}));

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);
    assertThat(response.getResult()).isNull();
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionType.class,
      names = {"ACCESS_LIST", "EIP1559", "BLOB", "DELEGATE_CODE"})
  public void typedReceiptRawStartsWithTypeByte(final TransactionType type) {
    final long blockNumber = 42L;
    final Hash blockHash = Hash.fromHexStringLenient("0x1234");
    final TransactionReceipt receipt =
        new TransactionReceipt(
            type,
            1,
            100L,
            Collections.singletonList(new BlockDataGenerator().log()),
            Optional.empty());

    when(blockchainQueries.headBlockNumber()).thenReturn(blockNumber);
    when(blockchainQueries.getBlockHashByNumber(blockNumber)).thenReturn(Optional.of(blockHash));
    when(blockchain.getTxReceipts(blockHash)).thenReturn(Optional.of(List.of(receipt)));

    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0",
                "debug_getRawReceipts",
                new Object[] {"0x" + Long.toHexString(blockNumber)}));

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);
    final String[] results = (String[]) response.getResult();
    assertThat(results).hasSize(1);

    final Bytes raw = Bytes.fromHexString(results[0]);
    assertThat(raw.get(0))
        .as(
            "first byte must be the EIP-2718 type byte for type %s — not an RLP byte-string prefix",
            type)
        .isEqualTo(type.getSerializedType());
  }
}
