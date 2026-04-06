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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotRead;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TestingBuildBlockV1Test {

  private static final long DEFAULT_TIMESTAMP = 100L;

  @Mock private ProtocolContext protocolContext;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private MiningConfiguration miningConfiguration;
  @Mock private TransactionPool transactionPool;
  @Mock private EthScheduler ethScheduler;
  @Mock private MutableBlockchain blockchain;

  private TestingBuildBlockV1 method;

  @BeforeEach
  void setUp() {
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    method =
        new TestingBuildBlockV1(
            protocolContext, protocolSchedule, miningConfiguration, transactionPool, ethScheduler);
  }

  @Test
  void shouldReturnCorrectMethodName() {
    assertThat(method.getName()).isEqualTo("testing_buildBlockV1");
  }

  @Test
  void shouldReturnErrorWhenParentBlockNotFound() {
    when(blockchain.getBlockHeader(any(Hash.class))).thenReturn(Optional.empty());

    final JsonRpcResponse response =
        method.response(requestWithParentHash(Hash.ZERO.toHexString()));

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    final JsonRpcErrorResponse errorResponse = (JsonRpcErrorResponse) response;
    assertThat(errorResponse.getError().getCode()).isEqualTo(RpcErrorType.INVALID_PARAMS.getCode());
  }

  @Test
  void shouldReturnErrorForInvalidTransactionRlp() {
    final BlockHeader parentHeader =
        new BlockHeaderTestFixture().timestamp(DEFAULT_TIMESTAMP).buildHeader();
    when(blockchain.getBlockHeader(any(Hash.class))).thenReturn(Optional.of(parentHeader));

    final JsonRpcResponse response =
        method.response(
            requestWithTransactions(
                parentHeader.getHash().toHexString(),
                new String[] {"0xINVALIDRLP"},
                DEFAULT_TIMESTAMP + 1));

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    final JsonRpcErrorResponse errorResponse = (JsonRpcErrorResponse) response;
    assertThat(errorResponse.getError().getCode())
        .isEqualTo(RpcErrorType.INVALID_TRANSACTION_PARAMS.getCode());
  }

  @Test
  void shouldReturnErrorWhenZeroTimestamp() {
    final BlockHeader parentHeader = new BlockHeaderTestFixture().buildHeader();
    when(blockchain.getBlockHeader(any(Hash.class))).thenReturn(Optional.of(parentHeader));

    final JsonRpcResponse response =
        method.response(requestWithTimestamp(parentHeader.getHash().toHexString(), 0L));

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    final JsonRpcErrorResponse errorResponse = (JsonRpcErrorResponse) response;
    assertThat(errorResponse.getError().getCode()).isEqualTo(RpcErrorType.INVALID_PARAMS.getCode());
  }

  @Test
  void shouldReturnErrorWhenMissingSuggestedFeeRecipient() {
    final BlockHeader parentHeader =
        new BlockHeaderTestFixture().timestamp(DEFAULT_TIMESTAMP).buildHeader();
    when(blockchain.getBlockHeader(any(Hash.class))).thenReturn(Optional.of(parentHeader));

    final JsonRpcResponse response =
        method.response(
            requestWithMissingSuggestedFeeRecipient(
                parentHeader.getHash().toHexString(), DEFAULT_TIMESTAMP + 1));

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    final JsonRpcErrorResponse errorResponse = (JsonRpcErrorResponse) response;
    assertThat(errorResponse.getError().getCode()).isEqualTo(RpcErrorType.INVALID_PARAMS.getCode());
  }

  @Test
  void verifyBlockAccessListEncodingFormat() {
    final BlockAccessList blockAccessList = createSampleBlockAccessList();
    final String encoded = encodeBlockAccessList(blockAccessList);

    assertThat(encoded).isNotNull();
    assertThat(encoded).startsWith("0x");
    assertThat(encoded.length()).isGreaterThan(2);
  }

  @Test
  void verifyBlockAccessListEncodingWithMultipleAccounts() {
    final BlockAccessList blockAccessList = createBlockAccessListWithMultipleAccounts();
    final String encoded = encodeBlockAccessList(blockAccessList);

    assertThat(encoded).isNotNull();
    assertThat(encoded).startsWith("0x");
  }

  @Test
  void verifyEmptyBlockAccessListEncoding() {
    final BlockAccessList emptyBlockAccessList = new BlockAccessList(List.of());
    final String encoded = encodeBlockAccessList(emptyBlockAccessList);

    assertThat(encoded).isNotNull();
    assertThat(encoded).startsWith("0x");
  }

  @Test
  void verifyBlockAccessListWithOnlyBalanceChanges() {
    final Address address = Address.fromHexString("0x0000000000000000000000000000000000000001");
    final BlockAccessList blockAccessList =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    address,
                    List.of(),
                    List.of(),
                    List.of(
                        new BalanceChange(0, Wei.fromEth(1)), new BalanceChange(1, Wei.fromEth(2))),
                    List.of(),
                    List.of())));

    final String encoded = encodeBlockAccessList(blockAccessList);

    assertThat(encoded).isNotNull();
    assertThat(encoded).startsWith("0x");
  }

  @Test
  void verifyBlockAccessListWithStorageChanges() {
    final Address address = Address.fromHexString("0x0000000000000000000000000000000000000001");
    final StorageSlotKey slotKey1 = new StorageSlotKey(UInt256.ONE);
    final StorageSlotKey slotKey2 = new StorageSlotKey(UInt256.valueOf(2));

    final BlockAccessList blockAccessList =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    address,
                    List.of(
                        new SlotChanges(
                            slotKey1,
                            List.of(
                                new StorageChange(0, UInt256.valueOf(100)),
                                new StorageChange(1, UInt256.valueOf(200)))),
                        new SlotChanges(
                            slotKey2, List.of(new StorageChange(0, UInt256.valueOf(300))))),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of())));

    final String encoded = encodeBlockAccessList(blockAccessList);

    assertThat(encoded).isNotNull();
    assertThat(encoded).startsWith("0x");
  }

  @Test
  void verifyBlockAccessListWithStorageReads() {
    final Address address = Address.fromHexString("0x0000000000000000000000000000000000000001");
    final StorageSlotKey slotKey1 = new StorageSlotKey(UInt256.ONE);
    final StorageSlotKey slotKey2 = new StorageSlotKey(UInt256.valueOf(2));

    final BlockAccessList blockAccessList =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    address,
                    List.of(),
                    List.of(new SlotRead(slotKey1), new SlotRead(slotKey2)),
                    List.of(),
                    List.of(),
                    List.of())));

    final String encoded = encodeBlockAccessList(blockAccessList);

    assertThat(encoded).isNotNull();
    assertThat(encoded).startsWith("0x");
  }

  @Test
  void verifyBlockAccessListWithNonceChanges() {
    final Address address = Address.fromHexString("0x0000000000000000000000000000000000000001");
    final BlockAccessList blockAccessList =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    address,
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(new NonceChange(0, 1L), new NonceChange(1, 2L)),
                    List.of())));

    final String encoded = encodeBlockAccessList(blockAccessList);

    assertThat(encoded).isNotNull();
    assertThat(encoded).startsWith("0x");
  }

  private static BlockAccessList createSampleBlockAccessList() {
    final Address address = Address.fromHexString("0x0000000000000000000000000000000000000001");
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.ONE);
    final SlotChanges slotChanges =
        new SlotChanges(slotKey, List.of(new StorageChange(0, UInt256.valueOf(2))));
    return new BlockAccessList(
        List.of(
            new AccountChanges(
                address,
                List.of(slotChanges),
                List.of(new SlotRead(slotKey)),
                List.of(new BalanceChange(0, Wei.ONE)),
                List.of(new NonceChange(0, 1L)),
                List.of())));
  }

  private static BlockAccessList createBlockAccessListWithMultipleAccounts() {
    final Address address1 = Address.fromHexString("0x0000000000000000000000000000000000000001");
    final Address address2 = Address.fromHexString("0x0000000000000000000000000000000000000002");
    final Address address3 = Address.fromHexString("0x0000000000000000000000000000000000000003");

    final StorageSlotKey slotKey1 = new StorageSlotKey(UInt256.ONE);
    final StorageSlotKey slotKey2 = new StorageSlotKey(UInt256.valueOf(2));

    return new BlockAccessList(
        List.of(
            new AccountChanges(
                address1,
                List.of(
                    new SlotChanges(slotKey1, List.of(new StorageChange(0, UInt256.valueOf(10))))),
                List.of(),
                List.of(new BalanceChange(0, Wei.fromEth(1))),
                List.of(new NonceChange(0, 1L)),
                List.of()),
            new AccountChanges(
                address2,
                List.of(),
                List.of(new SlotRead(slotKey2)),
                List.of(new BalanceChange(0, Wei.fromEth(2))),
                List.of(),
                List.of()),
            new AccountChanges(
                address3,
                List.of(),
                List.of(),
                List.of(new BalanceChange(0, Wei.fromEth(3))),
                List.of(),
                List.of())));
  }

  private static String encodeBlockAccessList(final BlockAccessList blockAccessList) {
    final BytesValueRLPOutput output = new BytesValueRLPOutput();
    blockAccessList.writeTo(output);
    return output.encoded().toHexString();
  }

  private JsonRpcRequestContext requestWithParentHash(final String parentHash) {
    return requestWithTransactions(parentHash, new String[0], DEFAULT_TIMESTAMP + 1);
  }

  private JsonRpcRequestContext requestWithTimestamp(
      final String parentHash, final long timestamp) {
    return requestWithTransactions(parentHash, new String[0], timestamp);
  }

  private JsonRpcRequestContext requestWithTransactions(
      final String parentHash, final String[] transactions, final long timestamp) {
    final Map<String, Object> payloadAttributes = new LinkedHashMap<>();
    payloadAttributes.put("timestamp", Bytes.ofUnsignedLong(timestamp).toQuantityHexString());
    payloadAttributes.put("prevRandao", Hash.ZERO.toHexString());
    payloadAttributes.put("suggestedFeeRecipient", "0x0000000000000000000000000000000000000000");
    payloadAttributes.put("withdrawals", Collections.emptyList());
    payloadAttributes.put("parentBeaconBlockRoot", Bytes32.ZERO.toHexString());

    return new JsonRpcRequestContext(
        new JsonRpcRequest(
            "2.0",
            "testing_buildBlockV1",
            new Object[] {parentHash, payloadAttributes, transactions, null}));
  }

  private JsonRpcRequestContext requestWithMissingSuggestedFeeRecipient(
      final String parentHash, final long timestamp) {
    final Map<String, Object> payloadAttributes = new LinkedHashMap<>();
    payloadAttributes.put("timestamp", Bytes.ofUnsignedLong(timestamp).toQuantityHexString());
    payloadAttributes.put("prevRandao", Hash.ZERO.toHexString());
    payloadAttributes.put("withdrawals", Collections.emptyList());
    payloadAttributes.put("parentBeaconBlockRoot", Bytes32.ZERO.toHexString());

    return new JsonRpcRequestContext(
        new JsonRpcRequest(
            "2.0",
            "testing_buildBlockV1",
            new Object[] {parentHash, payloadAttributes, new String[0], null}));
  }
}
