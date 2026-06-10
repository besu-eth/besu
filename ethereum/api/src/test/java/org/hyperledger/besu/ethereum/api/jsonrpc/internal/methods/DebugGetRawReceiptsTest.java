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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    when(blockchainQueries.getBlockHashByNumber(missingBlockNumber)).thenReturn(Optional.empty());

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
    final long futureBlockNumber = Long.MAX_VALUE;
    when(blockchainQueries.getBlockHashByNumber(futureBlockNumber)).thenReturn(Optional.empty());

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
}
