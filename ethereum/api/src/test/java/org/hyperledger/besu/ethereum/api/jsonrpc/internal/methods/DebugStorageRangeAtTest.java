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
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.BlockReplay;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.Tracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.DebugStorageRangeAtResult;
import org.hyperledger.besu.ethereum.api.query.BlockWithMetadata;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.AccountStorageEntry;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.invocation.InvocationOnMock;

public class DebugStorageRangeAtTest {

  private static final int TRANSACTION_INDEX = 2;
  private static final Bytes32 START_KEY_HASH = Bytes32.fromHexString("0x22");
  private final Blockchain blockchain = mock(Blockchain.class);
  private final BlockchainQueries blockchainQueries = mock(BlockchainQueries.class);
  private final BlockReplay blockReplay = mock(BlockReplay.class, Answers.RETURNS_DEEP_STUBS);
  private final DebugStorageRangeAt debugStorageRangeAt =
      new DebugStorageRangeAt(blockchainQueries, blockReplay);
  private final Tracer.TraceableState worldState = mock(Tracer.TraceableState.class);
  private final Account account = mock(Account.class);
  private final MainnetTransactionProcessor transactionProcessor =
      mock(MainnetTransactionProcessor.class);
  private final Transaction transaction = mock(Transaction.class);

  private final BlockHeader blockHeader = mock(BlockHeader.class, Answers.RETURNS_DEEP_STUBS);
  private final Hash blockHash =
      Hash.fromHexString("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
  private final Hash transactionHash =
      Hash.fromHexString("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
  private final Address accountAddress = Address.MODEXP;

  @BeforeEach
  public void setUp() {
    when(transaction.getHash()).thenReturn(transactionHash);
  }

  @Test
  public void nameShouldBeDebugStorageRangeAt() {
    assertThat(debugStorageRangeAt.getName()).isEqualTo("debug_storageRangeAt");
  }

  @Test
  public void shouldRetrieveStorageRange_fullValues() {
    final TransactionWithMetadata transactionWithMetadata =
        new TransactionWithMetadata(
            transaction, 12L, Optional.empty(), blockHash, TRANSACTION_INDEX, 0L);
    final BlockWithMetadata<TransactionWithMetadata, Hash> blockWithMetadata =
        new BlockWithMetadata<>(
            blockHeader,
            Collections.singletonList(transactionWithMetadata),
            Collections.emptyList(),
            Difficulty.ONE,
            1);
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0",
                "debug_storageRangeAt",
                new Object[] {
                  blockHash.toString(),
                  TRANSACTION_INDEX,
                  accountAddress,
                  START_KEY_HASH.toString(),
                  10
                }));

    when(blockchainQueries.blockByHash(blockHash)).thenReturn(Optional.of(blockWithMetadata));
    doAnswer(
            invocation ->
                invocation
                    .<Function<MutableWorldState, Optional<? extends JsonRpcResponse>>>getArgument(
                        1)
                    .apply(worldState))
        .when(blockchainQueries)
        .getAndMapWorldState(any(), any());
    when(blockchainQueries.transactionByBlockHashAndIndex(blockHash, TRANSACTION_INDEX))
        .thenReturn(Optional.of(transactionWithMetadata));
    when(worldState.get(accountAddress)).thenReturn(account);
    when(blockReplay.afterTransactionInBlock(
            any(Tracer.TraceableState.class), any(Hash.class), eq(transactionHash), any()))
        .thenAnswer(this::callAction);

    final List<AccountStorageEntry> entries = new ArrayList<>();
    entries.add(
        AccountStorageEntry.forKeyAndValue(UInt256.fromHexString("0x33"), UInt256.valueOf(6)));
    entries.add(
        AccountStorageEntry.forKeyAndValue(UInt256.fromHexString("0x44"), UInt256.valueOf(7)));
    entries.add(
        AccountStorageEntry.create(
            UInt256.valueOf(7),
            Bytes32.wrap(Hash.hash(Bytes32.fromHexString("0x45")).getBytes()),
            Optional.empty()));
    final NavigableMap<Bytes32, AccountStorageEntry> rawEntries = new TreeMap<>();
    entries.forEach(e -> rawEntries.put(e.getKeyHash(), e));

    when(account.storageEntriesFrom(START_KEY_HASH, 11)).thenReturn(rawEntries);
    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse) debugStorageRangeAt.response(request);
    final DebugStorageRangeAtResult result = (DebugStorageRangeAtResult) response.getResult();

    assertThat(result).isNotNull();
    assertThat(result.getNextKey()).isNull();

    entries.sort(Comparator.comparing(AccountStorageEntry::getKeyHash));
    assertThat(result.getStorage())
        .containsExactly(
            entry(
                entries.get(0).getKeyHash().toString(),
                new DebugStorageRangeAtResult.StorageEntry(entries.get(0), false)),
            entry(
                entries.get(1).getKeyHash().toString(),
                new DebugStorageRangeAtResult.StorageEntry(entries.get(1), false)),
            entry(
                entries.get(2).getKeyHash().toString(),
                new DebugStorageRangeAtResult.StorageEntry(entries.get(2), false)));
  }

  private Object callAction(final InvocationOnMock invocation) {
    //noinspection rawtypes
    return Optional.of(
        ((BlockReplay.TransactionAction) invocation.getArgument(3))
            .performAction(transaction, blockHeader, blockchain, transactionProcessor, Wei.ZERO));
  }
}
