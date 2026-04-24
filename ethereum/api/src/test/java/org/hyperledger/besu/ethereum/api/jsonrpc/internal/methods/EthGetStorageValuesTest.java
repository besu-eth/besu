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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.ChainHead;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EthGetStorageValuesTest {

  private static final Address ADDRESS_ONE =
      Address.fromHexString("0x7dcd17433742f4c0ca53122ab541d0ba67fc27df");
  private static final Address ADDRESS_TWO =
      Address.fromHexString("0xc1cadaffffffffffffffffffffffffffffffffff");
  private static final String SLOT_ZERO =
      "0x0000000000000000000000000000000000000000000000000000000000000000";
  private static final String SLOT_HIGH =
      "0x0100000000000000000000000000000000000000000000000000000000000000";
  private static final String VALUE_FORTY_TWO =
      "0x000000000000000000000000000000000000000000000000000000000000002a";
  private static final String VALUE_ZERO =
      "0x0000000000000000000000000000000000000000000000000000000000000000";

  private final BlockchainQueries blockchainQueries = mock(BlockchainQueries.class);
  private final Blockchain blockchain = mock(Blockchain.class);
  private final ChainHead chainHead = mock(ChainHead.class);
  private final BlockHeader chainHeadHeader = mock(BlockHeader.class);

  private EthGetStorageValues method;

  @BeforeEach
  public void setup() {
    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHead()).thenReturn(chainHead);
    when(chainHead.getBlockHeader()).thenReturn(chainHeadHeader);
    when(chainHeadHeader.getBlockHash()).thenReturn(Hash.ZERO);
    method = new EthGetStorageValues(blockchainQueries);
  }

  @Test
  public void shouldReturnCorrectMethodName() {
    assertThat(method.getName()).isEqualTo("eth_getStorageValues");
  }

  @Test
  public void shouldReturnEmptyRequestErrorWhenMapIsEmpty() {
    final JsonRpcRequestContext request = requestWithParams(Map.of(), "latest");

    final JsonRpcErrorResponse response = (JsonRpcErrorResponse) method.response(request);

    assertThat(response.getError().getCode()).isEqualTo(-32602);
    assertThat(response.getError().getMessage()).isEqualTo("empty request");
  }

  @Test
  public void shouldReturnStorageValueForSingleAddress() {
    when(blockchainQueries.storageAt(ADDRESS_ONE, UInt256.fromHexString(SLOT_ZERO), Hash.ZERO))
        .thenReturn(Optional.of(UInt256.valueOf(42)));

    final JsonRpcRequestContext request =
        requestWithParams(Map.of(ADDRESS_ONE.toHexString(), List.of(SLOT_ZERO)), "latest");

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);

    @SuppressWarnings("unchecked")
    final Map<String, List<String>> result = (Map<String, List<String>>) response.getResult();
    assertThat(result).containsEntry(ADDRESS_ONE.toHexString(), List.of(VALUE_FORTY_TWO));
  }

  @Test
  public void shouldReturnStorageValuesForMultipleAddresses() {
    when(blockchainQueries.storageAt(ADDRESS_ONE, UInt256.fromHexString(SLOT_ZERO), Hash.ZERO))
        .thenReturn(Optional.of(UInt256.valueOf(42)));
    when(blockchainQueries.storageAt(ADDRESS_TWO, UInt256.fromHexString(SLOT_HIGH), Hash.ZERO))
        .thenReturn(Optional.of(UInt256.ZERO));

    final JsonRpcRequestContext request =
        requestWithParams(
            Map.of(
                ADDRESS_ONE.toHexString(), List.of(SLOT_ZERO),
                ADDRESS_TWO.toHexString(), List.of(SLOT_HIGH)),
            "latest");

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);

    @SuppressWarnings("unchecked")
    final Map<String, List<String>> result = (Map<String, List<String>>) response.getResult();
    assertThat(result)
        .containsEntry(ADDRESS_ONE.toHexString(), List.of(VALUE_FORTY_TWO))
        .containsEntry(ADDRESS_TWO.toHexString(), List.of(VALUE_ZERO));
  }

  @Test
  public void shouldReturnZeroForUnknownAccount() {
    when(blockchainQueries.storageAt(ADDRESS_TWO, UInt256.fromHexString(SLOT_HIGH), Hash.ZERO))
        .thenReturn(Optional.empty());

    final JsonRpcRequestContext request =
        requestWithParams(Map.of(ADDRESS_TWO.toHexString(), List.of(SLOT_HIGH)), "latest");

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);

    @SuppressWarnings("unchecked")
    final Map<String, List<String>> result = (Map<String, List<String>>) response.getResult();
    assertThat(result).containsEntry(ADDRESS_TWO.toHexString(), List.of(VALUE_ZERO));
  }

  @Test
  public void shouldReturnTooManySlotsErrorWhenOverLimit() {
    final String[] keys = new String[EthGetStorageValues.MAX_STORAGE_SLOTS + 1];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = SLOT_ZERO;
    }

    final JsonRpcRequestContext request =
        requestWithParams(Map.of(ADDRESS_ONE.toHexString(), List.of(keys)), "latest");

    final JsonRpcErrorResponse response = (JsonRpcErrorResponse) method.response(request);

    assertThat(response.getError().getCode()).isEqualTo(-32602);
    assertThat(response.getError().getMessage()).isEqualTo("too many slots (max 1024)");
  }

  private JsonRpcRequestContext requestWithParams(
      final Map<String, List<String>> requests, final String block) {
    return new JsonRpcRequestContext(
        new JsonRpcRequest("2.0", "eth_getStorageValues", new Object[] {requests, block}));
  }

  @Test
  public void shouldThrowInvalidJsonRpcParametersForMalformedSlotHex() {
    final JsonRpcRequestContext request =
        requestWithParams(Map.of(ADDRESS_ONE.toHexString(), List.of("0xzz")), "latest");

    assertThatThrownBy(() -> method.response(request))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessageContaining("Invalid storage key parameter");
  }

  @Test
  public void shouldReturnInvalidParamsWhenSlotListIsNull() {
    final Map<String, List<String>> params = new HashMap<>();
    params.put(ADDRESS_ONE.toHexString(), null);
    final JsonRpcRequestContext request = requestWithParams(params, "latest");

    final JsonRpcErrorResponse response = (JsonRpcErrorResponse) method.response(request);

    assertThat(response.getError().getCode()).isEqualTo(-32602);
    assertThat(response.getError().getMessage()).isEqualTo("null slot list");
  }

  @Test
  public void shouldReturnStorageValueByBlockHash() {
    final Hash specificBlockHash =
        Hash.fromHexString("0xb73cee02246c6f32ac0f459934e89102c2ce904d966a4fc2ab6309c3e104b9cb");
    final BlockHeader specificHeader = mock(BlockHeader.class);
    when(specificHeader.getBlockHash()).thenReturn(specificBlockHash);
    when(blockchainQueries.getBlockHeaderByHash(specificBlockHash))
        .thenReturn(Optional.of(specificHeader));
    when(blockchainQueries.storageAt(
            ADDRESS_ONE, UInt256.fromHexString(SLOT_ZERO), specificBlockHash))
        .thenReturn(Optional.of(UInt256.valueOf(42)));

    final JsonRpcRequestContext request =
        requestWithParams(
            Map.of(ADDRESS_ONE.toHexString(), List.of(SLOT_ZERO)), specificBlockHash.toHexString());

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);

    @SuppressWarnings("unchecked")
    final Map<String, List<String>> result = (Map<String, List<String>>) response.getResult();
    assertThat(result).containsEntry(ADDRESS_ONE.toHexString(), List.of(VALUE_FORTY_TWO));
  }

  @Test
  public void shouldThrowInvalidJsonRpcParametersWhenSlotKeyIsNull() {
    final JsonRpcRequestContext request =
        requestWithParams(Map.of(ADDRESS_ONE.toHexString(), Arrays.asList("0x0", null)), "latest");

    assertThatThrownBy(() -> method.response(request))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessageContaining("Invalid storage key parameter");
  }
}
