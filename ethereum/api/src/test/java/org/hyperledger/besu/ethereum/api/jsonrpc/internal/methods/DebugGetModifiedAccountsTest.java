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

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
public class DebugGetModifiedAccountsTest {

  private static final Address ACCOUNT_ADDRESS =
      Address.fromHexString("0x0000000000000000000000000000000000000001");
  private static final Address CODE_ADDRESS =
      Address.fromHexString("0x0000000000000000000000000000000000000002");
  private static final Address STORAGE_ADDRESS =
      Address.fromHexString("0x0000000000000000000000000000000000000003");

  private final BlockchainQueries blockchainQueries = mock(BlockchainQueries.class);
  private final Blockchain blockchain = mock(Blockchain.class);
  private final PathBasedWorldStateProvider worldStateArchive =
      mock(PathBasedWorldStateProvider.class);
  private final TrieLogManager trieLogManager = mock(TrieLogManager.class);

  private final DebugGetModifiedAccountsByNumber getModifiedAccountsByNumber =
      new DebugGetModifiedAccountsByNumber(blockchainQueries);
  private final DebugGetModifiedAccountsByHash getModifiedAccountsByHash =
      new DebugGetModifiedAccountsByHash(blockchainQueries);

  private final BlockHeader block0 = blockHeader(0);
  private final BlockHeader block1 = blockHeader(1);
  private final BlockHeader block2 = blockHeader(2);

  @BeforeEach
  public void setUp() {
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchainQueries.getWorldStateArchive()).thenReturn(worldStateArchive);
    when(worldStateArchive.getTrieLogManager()).thenReturn(trieLogManager);

    when(blockchain.getBlockHeader(0L)).thenReturn(Optional.of(block0));
    when(blockchain.getBlockHeader(1L)).thenReturn(Optional.of(block1));
    when(blockchain.getBlockHeader(2L)).thenReturn(Optional.of(block2));
    when(blockchain.getBlockHeader(block0.getBlockHash())).thenReturn(Optional.of(block0));
    when(blockchain.getBlockHeader(block1.getBlockHash())).thenReturn(Optional.of(block1));
    when(blockchain.getBlockHeader(block2.getBlockHash())).thenReturn(Optional.of(block2));
  }

  @Test
  public void nameShouldBeDebugGetModifiedAccountsByNumber() {
    assertThat(getModifiedAccountsByNumber.getName())
        .isEqualTo("debug_getModifiedAccountsByNumber");
  }

  @Test
  public void nameShouldBeDebugGetModifiedAccountsByHash() {
    assertThat(getModifiedAccountsByHash.getName()).isEqualTo("debug_getModifiedAccountsByHash");
  }

  @Test
  public void getModifiedAccountsByNumberShouldReturnSingleBlockChanges() {
    final TrieLog trieLog = trieLog();
    final Hash blockHash = block1.getBlockHash();
    when(trieLogManager.getTrieLogLayer(blockHash)).thenReturn(Optional.of(trieLog));

    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse) getModifiedAccountsByNumber.response(request("0x1"));

    assertThat((List<String>) response.getResult())
        .containsExactly(
            ACCOUNT_ADDRESS.toHexString(),
            CODE_ADDRESS.toHexString(),
            STORAGE_ADDRESS.toHexString());
  }

  @Test
  public void getModifiedAccountsByNumberShouldReturnRangedChanges() {
    final TrieLog block1TrieLog = trieLog();
    final TrieLog block2TrieLog = trieLog(Address.fromHexString("0x4"));
    final Hash block1Hash = block1.getBlockHash();
    final Hash block2Hash = block2.getBlockHash();
    when(trieLogManager.getTrieLogLayer(block1Hash)).thenReturn(Optional.of(block1TrieLog));
    when(trieLogManager.getTrieLogLayer(block2Hash)).thenReturn(Optional.of(block2TrieLog));

    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse) getModifiedAccountsByNumber.response(request("0x0", "0x2"));

    assertThat((List<String>) response.getResult())
        .containsExactly(
            ACCOUNT_ADDRESS.toHexString(),
            CODE_ADDRESS.toHexString(),
            STORAGE_ADDRESS.toHexString(),
            Address.fromHexString("0x4").toHexString());
  }

  @Test
  public void getModifiedAccountsByHashShouldReturnSingleBlockChanges() {
    final TrieLog trieLog = trieLog();
    final Hash parentHash = block1.getParentHash();
    final Hash blockHash = block1.getBlockHash();
    when(blockchain.getBlockHeader(parentHash)).thenReturn(Optional.of(block0));
    when(trieLogManager.getTrieLogLayer(blockHash)).thenReturn(Optional.of(trieLog));

    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse)
            getModifiedAccountsByHash.response(hashRequest(block1.getBlockHash().toHexString()));

    assertThat((List<String>) response.getResult())
        .containsExactly(
            ACCOUNT_ADDRESS.toHexString(),
            CODE_ADDRESS.toHexString(),
            STORAGE_ADDRESS.toHexString());
  }

  @Test
  public void getModifiedAccountsByHashShouldReturnRangedChanges() {
    final TrieLog block1TrieLog = trieLog();
    final TrieLog block2TrieLog = trieLog(Address.fromHexString("0x4"));
    final Hash block1ParentHash = block1.getParentHash();
    final Hash block2ParentHash = block2.getParentHash();
    final Hash block1Hash = block1.getBlockHash();
    final Hash block2Hash = block2.getBlockHash();
    when(blockchain.getBlockHeader(block1ParentHash)).thenReturn(Optional.of(block0));
    when(blockchain.getBlockHeader(block2ParentHash)).thenReturn(Optional.of(block1));
    when(trieLogManager.getTrieLogLayer(block1Hash)).thenReturn(Optional.of(block1TrieLog));
    when(trieLogManager.getTrieLogLayer(block2Hash)).thenReturn(Optional.of(block2TrieLog));

    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse)
            getModifiedAccountsByHash.response(
                hashRequest(
                    block0.getBlockHash().toHexString(), block2.getBlockHash().toHexString()));

    assertThat((List<String>) response.getResult())
        .containsExactly(
            ACCOUNT_ADDRESS.toHexString(),
            CODE_ADDRESS.toHexString(),
            STORAGE_ADDRESS.toHexString(),
            Address.fromHexString("0x4").toHexString());
  }

  @Test
  public void shouldReturnBlockNotFoundForUnknownBlock() {
    final JsonRpcResponse response = getModifiedAccountsByNumber.response(request("0x9"));

    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.BLOCK_NOT_FOUND);
  }

  @Test
  public void shouldReturnBlockNotFoundForUnknownEndBlock() {
    final JsonRpcResponse response = getModifiedAccountsByNumber.response(request("0x0", "0x9"));

    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.BLOCK_NOT_FOUND);
  }

  @Test
  public void shouldReturnInvalidBlockParamsWhenStartIsNotBeforeEnd() {
    final JsonRpcResponse response = getModifiedAccountsByNumber.response(request("0x2", "0x1"));

    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.INVALID_BLOCK_PARAMS);
  }

  @Test
  public void shouldReturnInvalidBlockParamsWhenSingleNumberParamIsGenesisBlock() {
    final JsonRpcResponse response = getModifiedAccountsByNumber.response(request("0x0"));

    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.INVALID_BLOCK_PARAMS);
  }

  @Test
  public void shouldReturnInvalidBlockParamsWhenHashRangeIsDisconnected() {
    final Hash orphanParentHash = Hash.hash(Bytes.ofUnsignedLong(10));
    final Hash orphanHash = Hash.hash(Bytes.ofUnsignedLong(11));
    final BlockHeader orphanParent = blockHeader(0, Hash.EMPTY, orphanParentHash);
    final BlockHeader orphan = blockHeader(1, orphanParentHash, orphanHash);
    final BlockHeader disconnectedEnd =
        blockHeader(2, orphanHash, Hash.hash(Bytes.ofUnsignedLong(12)));
    when(blockchain.getBlockHeader(disconnectedEnd.getBlockHash()))
        .thenReturn(Optional.of(disconnectedEnd));
    when(blockchain.getBlockHeader(disconnectedEnd.getParentHash()))
        .thenReturn(Optional.of(orphan));
    when(blockchain.getBlockHeader(orphan.getParentHash())).thenReturn(Optional.of(orphanParent));

    final JsonRpcResponse response =
        getModifiedAccountsByHash.response(
            hashRequest(
                block0.getBlockHash().toHexString(), disconnectedEnd.getBlockHash().toHexString()));

    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.INVALID_BLOCK_PARAMS);
  }

  @Test
  public void shouldReturnWorldStateUnavailableWhenTrieLogIsMissing() {
    final Hash blockHash = block1.getBlockHash();
    when(trieLogManager.getTrieLogLayer(blockHash)).thenReturn(Optional.empty());

    final JsonRpcResponse response = getModifiedAccountsByNumber.response(request("0x1"));

    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.WORLD_STATE_UNAVAILABLE);
  }

  @Test
  public void shouldReturnRangeErrorWhenNumberRangeIsTooLarge() {
    final BlockHeader distantBlock = blockHeader(1001);
    when(blockchain.getBlockHeader(1001L)).thenReturn(Optional.of(distantBlock));

    final JsonRpcResponse response = getModifiedAccountsByNumber.response(request("0x0", "0x3e9"));

    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.EXCEEDS_RPC_MAX_BLOCK_RANGE);
  }

  @Test
  public void shouldReturnRangeErrorWhenHashRangeIsTooLarge() {
    final BlockHeader distantBlock =
        blockHeader(1001, block0.getBlockHash(), Hash.hash(Bytes.ofUnsignedLong(1001)));
    when(blockchain.getBlockHeader(distantBlock.getBlockHash()))
        .thenReturn(Optional.of(distantBlock));

    final JsonRpcResponse response =
        getModifiedAccountsByHash.response(
            hashRequest(
                block0.getBlockHash().toHexString(), distantBlock.getBlockHash().toHexString()));

    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.EXCEEDS_RPC_MAX_BLOCK_RANGE);
  }

  @Test
  public void shouldReturnInvalidParamCountWhenNoBlockIsSupplied() {
    final JsonRpcResponse response = getModifiedAccountsByNumber.response(request());

    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.INVALID_PARAM_COUNT);
  }

  @Test
  public void shouldReturnMethodNotFoundForNonPathBasedWorldState() {
    final WorldStateArchive forestWorldStateArchive = mock(WorldStateArchive.class);
    when(blockchainQueries.getWorldStateArchive()).thenReturn(forestWorldStateArchive);

    final JsonRpcResponse response = getModifiedAccountsByNumber.response(request("0x0", "0x1"));

    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.METHOD_NOT_FOUND);
  }

  private JsonRpcRequestContext request(final Object... params) {
    return new JsonRpcRequestContext(
        new JsonRpcRequest("2.0", "debug_getModifiedAccountsByNumber", params));
  }

  private JsonRpcRequestContext hashRequest(final Object... params) {
    return new JsonRpcRequestContext(
        new JsonRpcRequest("2.0", "debug_getModifiedAccountsByHash", params));
  }

  private BlockHeader blockHeader(final long number) {
    return blockHeader(
        number,
        number == 0 ? Hash.EMPTY : Hash.hash(Bytes.ofUnsignedLong(number - 1)),
        Hash.hash(Bytes.ofUnsignedLong(number)));
  }

  private BlockHeader blockHeader(final long number, final Hash parentHash, final Hash blockHash) {
    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getNumber()).thenReturn(number);
    when(blockHeader.getBlockHash()).thenReturn(blockHash);
    when(blockHeader.getHash()).thenReturn(blockHash);
    when(blockHeader.getParentHash()).thenReturn(parentHash);
    return blockHeader;
  }

  private TrieLog trieLog() {
    final TrieLog trieLog = mock(TrieLog.class);
    final PathBasedValue<AccountValue> accountChange = mock(PathBasedValue.class);
    final PathBasedValue<Bytes> codeChange = mock(PathBasedValue.class);
    final PathBasedValue<UInt256> storageChange = mock(PathBasedValue.class);
    when(trieLog.getAccountChanges()).thenReturn(Map.of(ACCOUNT_ADDRESS, accountChange));
    when(trieLog.getCodeChanges()).thenReturn(Map.of(CODE_ADDRESS, codeChange));
    when(trieLog.getStorageChanges())
        .thenReturn(
            Map.of(STORAGE_ADDRESS, Map.of(new StorageSlotKey(UInt256.ONE), storageChange)));
    return trieLog;
  }

  private TrieLog trieLog(final Address accountAddress) {
    final TrieLog trieLog = mock(TrieLog.class);
    final PathBasedValue<AccountValue> accountChange = mock(PathBasedValue.class);
    when(trieLog.getAccountChanges()).thenReturn(Map.of(accountAddress, accountChange));
    when(trieLog.getCodeChanges()).thenReturn(Map.of());
    when(trieLog.getStorageChanges()).thenReturn(Map.of());
    return trieLog;
  }
}
