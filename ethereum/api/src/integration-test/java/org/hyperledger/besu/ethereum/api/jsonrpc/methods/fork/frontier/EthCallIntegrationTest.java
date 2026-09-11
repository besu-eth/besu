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
package org.hyperledger.besu.ethereum.api.jsonrpc.methods.fork.frontier;

import static org.assertj.core.api.Assertions.assertThat;

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

import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.google.common.io.Resources;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EthCallIntegrationTest {

  private static JsonRpcTestMethodsFactory BLOCKCHAIN;

  private JsonRpcMethod method;

  @BeforeAll
  public static void setUpOnce() throws Exception {
    final String genesisJson =
        Resources.toString(BlockTestUtil.getTestGenesisUrl(), StandardCharsets.UTF_8);

    BLOCKCHAIN =
        new JsonRpcTestMethodsFactory(
            new BlockchainImporter(BlockTestUtil.getTestBlockchainUrl(), genesisJson));
  }

  @BeforeEach
  public void setUp() {
    final Map<String, JsonRpcMethod> methods = BLOCKCHAIN.methods();
    method = methods.get("eth_call");
  }

  @Test
  public void shouldReturnExpectedResultForCallAtLatestBlock() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .sender(Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b"))
            .to(Address.fromHexString("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"))
            .input(Bytes.fromHexString("0x12a7b914"))
            .build();

    final JsonRpcRequestContext request = requestWithParams(callParameter, "latest");
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(
            null, "0x0000000000000000000000000000000000000000000000000000000000000001");

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnExpectedResultForCallAtSpecificBlock() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .sender(Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b"))
            .to(Address.fromHexString("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"))
            .input(Bytes.fromHexString("0x12a7b914"))
            .build();

    final JsonRpcRequestContext request = requestWithParams(callParameter, "0x8");
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(
            null, "0x0000000000000000000000000000000000000000000000000000000000000000");

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnSuccessWhenCreatingContract() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .sender(Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b"))
            .input(
                Bytes.fromHexString(
                    "0x608060405234801561001057600080fd5b50610157806100206000396000f30060806040526004361061004c576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff1680633bdab8bf146100515780639ae97baa14610068575b600080fd5b34801561005d57600080fd5b5061006661007f565b005b34801561007457600080fd5b5061007d6100b9565b005b7fa53887c1eed04528e23301f55ad49a91634ef5021aa83a97d07fd16ed71c039a60016040518082815260200191505060405180910390a1565b7fa53887c1eed04528e23301f55ad49a91634ef5021aa83a97d07fd16ed71c039a60026040518082815260200191505060405180910390a17fa53887c1eed04528e23301f55ad49a91634ef5021aa83a97d07fd16ed71c039a60036040518082815260200191505060405180910390a15600a165627a7a7230582010ddaa52e73a98c06dbcd22b234b97206c1d7ed64a7c048e10c2043a3d2309cb0029"))
            .build();

    final JsonRpcRequestContext request = requestWithParams(callParameter, "latest");
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(
            null,
            "0x60806040526004361061004c576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff1680633bdab8bf146100515780639ae97baa14610068575b600080fd5b34801561005d57600080fd5b5061006661007f565b005b34801561007457600080fd5b5061007d6100b9565b005b7fa53887c1eed04528e23301f55ad49a91634ef5021aa83a97d07fd16ed71c039a60016040518082815260200191505060405180910390a1565b7fa53887c1eed04528e23301f55ad49a91634ef5021aa83a97d07fd16ed71c039a60026040518082815260200191505060405180910390a17fa53887c1eed04528e23301f55ad49a91634ef5021aa83a97d07fd16ed71c039a60036040518082815260200191505060405180910390a15600a165627a7a7230582010ddaa52e73a98c06dbcd22b234b97206c1d7ed64a7c048e10c2043a3d2309cb0029");

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnErrorWithGasLimitTooLow() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .sender(Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b"))
            .to(Address.fromHexString("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"))
            .gas(0L)
            .input(Bytes.fromHexString("0x12a7b914"))
            .build();

    final JsonRpcRequestContext request = requestWithParams(callParameter, "latest");
    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(
            null,
            JsonRpcError.from(
                ValidationResult.invalid(
                    TransactionInvalidReason.INTRINSIC_GAS_EXCEEDS_GAS_LIMIT,
                    "intrinsic gas cost 21272 exceeds gas limit 0")));

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnErrorWithGasPriceTooHighAndStrict() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .sender(Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b"))
            .to(Address.fromHexString("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"))
            .gasPrice(Wei.fromHexString("0x10000000000000"))
            .input(Bytes.fromHexString("0x12a7b914"))
            .strict(true)
            .build();

    final JsonRpcRequestContext request = requestWithParams(callParameter, "latest");
    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(
            null,
            JsonRpcError.from(
                ValidationResult.invalid(
                    TransactionInvalidReason.UPFRONT_GAS_COST_EXCEEDS_BALANCE,
                    "transaction up-front gas cost 0x2fefd80000000000000 exceeds transaction sender account balance 0x340ab63a0215af0d for sender 0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b")));

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnSuccessWithGasPriceTooHighNotStrict() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .sender(Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b"))
            .to(Address.fromHexString("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"))
            .gasPrice(Wei.fromHexString("0x10000000000000"))
            .input(Bytes.fromHexString("0x12a7b914"))
            .strict(false)
            .build();

    final JsonRpcRequestContext request = requestWithParams(callParameter, "latest");
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(
            null, "0x0000000000000000000000000000000000000000000000000000000000000001");

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnErrorWithGasPriceTooHigh() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .sender(Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b"))
            .to(Address.fromHexString("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"))
            .gasPrice(Wei.fromHexString("0x10000000000000"))
            .input(Bytes.fromHexString("0x12a7b914"))
            .build();

    final JsonRpcRequestContext request = requestWithParams(callParameter, "latest");
    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(
            null,
            JsonRpcError.from(
                ValidationResult.invalid(
                    TransactionInvalidReason.UPFRONT_GAS_COST_EXCEEDS_BALANCE,
                    "transaction up-front gas cost 0x2fefd80000000000000 exceeds transaction sender account balance 0x340ab63a0215af0d for sender 0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b")));

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnSuccessWithValidGasPrice() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .sender(Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b"))
            .to(Address.fromHexString("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"))
            .gasPrice(Wei.fromHexString("0x10"))
            .input(Bytes.fromHexString("0x12a7b914"))
            .build();

    final JsonRpcRequestContext request = requestWithParams(callParameter, "latest");
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(
            null, "0x0000000000000000000000000000000000000000000000000000000000000001");

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnErrorWithGasPriceAndEmptyBalance() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .sender(Address.fromHexString("0xdeadbeef00000000000000000000000000000000"))
            .to(Address.fromHexString("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"))
            .gasPrice(Wei.fromHexString("0x10"))
            .input(Bytes.fromHexString("0x12a7b914"))
            .build();

    final JsonRpcRequestContext request = requestWithParams(callParameter, "latest");
    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(
            null,
            JsonRpcError.from(
                ValidationResult.invalid(
                    TransactionInvalidReason.UPFRONT_GAS_COST_EXCEEDS_BALANCE,
                    "transaction up-front gas cost 0x2fefd80 exceeds transaction sender account balance 0x0 for sender 0xdeadbeef00000000000000000000000000000000")));

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnSuccessWithZeroGasPriceAndEmptyBalance() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .sender(Address.fromHexString("0xdeadbeef00000000000000000000000000000000"))
            .to(Address.fromHexString("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"))
            .gasPrice(Wei.fromHexString("0x0"))
            .input(Bytes.fromHexString("0x12a7b914"))
            .build();

    final JsonRpcRequestContext request = requestWithParams(callParameter, "latest");
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(
            null, "0x0000000000000000000000000000000000000000000000000000000000000001");

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnSuccessWithoutGasPriceAndEmptyBalance() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .sender(Address.fromHexString("0xdeadbeef00000000000000000000000000000000"))
            .to(Address.fromHexString("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"))
            .input(Bytes.fromHexString("0x12a7b914"))
            .build();

    final JsonRpcRequestContext request = requestWithParams(callParameter, "latest");
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(
            null, "0x0000000000000000000000000000000000000000000000000000000000000001");

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnSuccessWithInvalidGasPricingAndEmptyBalance() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .sender(Address.fromHexString("0xdeadbeef00000000000000000000000000000000"))
            .to(Address.fromHexString("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"))
            .maxPriorityFeePerGas(Wei.fromHexString("0x0A"))
            .input(Bytes.fromHexString("0x12a7b914"))
            .build();

    final JsonRpcRequestContext request = requestWithParams(callParameter, "latest");
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(
            null, "0x0000000000000000000000000000000000000000000000000000000000000001");

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnEmptyHashResultForCallWithOnlyToField() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .to(Address.fromHexString("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"))
            .build();

    final JsonRpcRequestContext request = requestWithParams(callParameter, "latest");
    final JsonRpcResponse expectedResponse = new JsonRpcSuccessResponse(null, "0x");

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnSuccessWithInputAndDataFieldSetToSameValue() {
    final CallParameter callParameter =
        ImmutableCallParameter.builder()
            .sender(Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b"))
            .to(Address.fromHexString("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"))
            .input(Bytes.fromHexString("0x12a7b914"))
            .build();

    final JsonRpcRequestContext request = requestWithParams(callParameter, "latest");
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(
            null, "0x0000000000000000000000000000000000000000000000000000000000000001");

    final JsonRpcResponse response = method.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  private JsonRpcRequestContext requestWithParams(final Object... params) {
    return new JsonRpcRequestContext(new JsonRpcRequest("2.0", "eth_call", params));
  }
}
