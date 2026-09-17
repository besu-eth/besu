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
package org.hyperledger.besu.ethereum.api.jsonrpc.methods.fork.london;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.BlockchainImporter;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcTestMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.ImmutableCallParameter;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.testutil.BlockTestUtil;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import com.google.common.io.Resources;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EthEstimateGasIntegrationTest {

  private final String METHOD_NAME = "eth_estimateGas";

  private static JsonRpcTestMethodsFactory BLOCKCHAIN;

  private JsonRpcMethod method;

  @BeforeAll
  public static void setUpOnce() throws Exception {
    final String genesisJson =
        Resources.toString(BlockTestUtil.getTestLondonGenesisUrl(), StandardCharsets.UTF_8);

    BLOCKCHAIN =
        new JsonRpcTestMethodsFactory(
            new BlockchainImporter(BlockTestUtil.getTestLondonBlockchainUrl(), genesisJson));
  }

  @BeforeEach
  public void setUp() {
    final Map<String, JsonRpcMethod> methods = BLOCKCHAIN.methods();
    method = methods.get(METHOD_NAME);
  }

  @Test
  public void shouldReturnExpectedValueForTransfer() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .sender(Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b"))
            .to(Address.fromHexString("0x8888f1f195afa192cfee860698584c030f4c9db1"))
            .value(Wei.ONE)
            .build();

    final JsonRpcResponse response = method.response(requestWithParams(callParameter));
    final JsonRpcResponse expectedResponse = new JsonRpcSuccessResponse(null, "0x5208");
    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnExpectedValueForTransfer_WithAccessList() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .sender(Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b"))
            .to(Address.fromHexString("0x8888f1f195afa192cfee860698584c030f4c9db1"))
            .value(Wei.ONE)
            .accessList(createAccessList())
            .build();

    final JsonRpcResponse response = method.response(requestWithParams(callParameter));
    final JsonRpcResponse expectedResponse = new JsonRpcSuccessResponse(null, "0x62d4");
    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnExpectedValueForContractDeploy() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .sender(Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b"))
            .input(
                Bytes.fromHexString(
                    "0x608060405234801561001057600080fd5b50610157806100206000396000f30060806040526004361061004c576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff1680633bdab8bf146100515780639ae97baa14610068575b600080fd5b34801561005d57600080fd5b5061006661007f565b005b34801561007457600080fd5b5061007d6100b9565b005b7fa53887c1eed04528e23301f55ad49a91634ef5021aa83a97d07fd16ed71c039a60016040518082815260200191505060405180910390a1565b7fa53887c1eed04528e23301f55ad49a91634ef5021aa83a97d07fd16ed71c039a60026040518082815260200191505060405180910390a17fa53887c1eed04528e23301f55ad49a91634ef5021aa83a97d07fd16ed71c039a60036040518082815260200191505060405180910390a15600a165627a7a7230582010ddaa52e73a98c06dbcd22b234b97206c1d7ed64a7c048e10c2043a3d2309cb0029"))
            .build();

    final JsonRpcResponse response = method.response(requestWithParams(callParameter));
    final JsonRpcResponse expectedResponse = new JsonRpcSuccessResponse(null, "0x1f4c0");
    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnExpectedValueForContractDeploy_WithAccessList() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .chainId(BLOCKCHAIN.getChainId())
            .sender(Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b"))
            .input(
                Bytes.fromHexString(
                    "0x608060405234801561001057600080fd5b50610157806100206000396000f30060806040526004361061004c576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff1680633bdab8bf146100515780639ae97baa14610068575b600080fd5b34801561005d57600080fd5b5061006661007f565b005b34801561007457600080fd5b5061007d6100b9565b005b7fa53887c1eed04528e23301f55ad49a91634ef5021aa83a97d07fd16ed71c039a60016040518082815260200191505060405180910390a1565b7fa53887c1eed04528e23301f55ad49a91634ef5021aa83a97d07fd16ed71c039a60026040518082815260200191505060405180910390a17fa53887c1eed04528e23301f55ad49a91634ef5021aa83a97d07fd16ed71c039a60036040518082815260200191505060405180910390a15600a165627a7a7230582010ddaa52e73a98c06dbcd22b234b97206c1d7ed64a7c048e10c2043a3d2309cb0029"))
            .accessList(createAccessList())
            .build();

    final JsonRpcResponse response = method.response(requestWithParams(callParameter));
    final JsonRpcResponse expectedResponse = new JsonRpcSuccessResponse(null, "0x2059d");
    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnErrorWithInvalidChainId() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .chainId(BLOCKCHAIN.getChainId().add(BigInteger.ONE))
            .sender(Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b"))
            .to(Address.fromHexString("0x8888f1f195afa192cfee860698584c030f4c9db1"))
            .maxFeePerGas(Wei.ONE)
            .value(Wei.ONE)
            .build();

    final JsonRpcRequestContext request = requestWithParams(callParameter, "latest");
    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(
            null,
            JsonRpcError.from(
                ValidationResult.invalid(
                    TransactionInvalidReason.WRONG_CHAIN_ID,
                    "transaction was meant for chain id 1983 and not this chain id 1982")));

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  private List<AccessListEntry> createAccessList() {
    return List.of(
        new AccessListEntry(
            Address.fromHexString("0x8888f1f195afa192cfee860698584c030f4c9db1"),
            List.of(Bytes32.ZERO)));
  }

  private JsonRpcRequestContext requestWithParams(final Object... params) {
    return new JsonRpcRequestContext(new JsonRpcRequest("2.0", METHOD_NAME, params));
  }
}
