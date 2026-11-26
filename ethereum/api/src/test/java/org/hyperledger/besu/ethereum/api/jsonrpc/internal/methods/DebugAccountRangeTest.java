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
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

public class DebugAccountRangeTest {

  private final Blockchain blockchain = mock(Blockchain.class);
  private final BlockchainQueries blockchainQueries = mock(BlockchainQueries.class);
  private final DebugAccountRange debugAccountRange = new DebugAccountRange(blockchainQueries);
  private final BonsaiWorldStateProvider bonsaiArchive =
      createBonsaiInMemoryWorldStateArchive(blockchain);
  private final Hash blockHash =
      Hash.fromHexString("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

  @Test
  public void nameShouldBeDebugAccountRange() {
    assertThat(debugAccountRange.getName()).isEqualTo("debug_accountRange");
  }

  @Test
  public void shouldReturnMethodNotEnabledOnBonsaiArchive() {
    when(blockchainQueries.getWorldStateArchive()).thenReturn(bonsaiArchive);

    final JsonRpcResponse response =
        debugAccountRange.response(
            new JsonRpcRequestContext(
                new JsonRpcRequest(
                    "2.0",
                    "debug_accountRange",
                    new Object[] {blockHash.toString(), "0x0", Bytes32.ZERO.toString(), 10})));

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.METHOD_NOT_ENABLED);
  }
}
