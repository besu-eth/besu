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
package org.hyperledger.besu.consensus.ibft.jsonrpc.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalMatchers.lt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.BlockInterface;
import org.hyperledger.besu.consensus.common.validator.ValidatorProvider;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.SignerMetricResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.LongStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IbftGetSignerMetricsTest {

  private static final Address[] VALIDATORS = {
    Address.fromHexString("0x1"), Address.fromHexString("0x2"), Address.fromHexString("0x3"),
  };

  private final String IBFT_METHOD = "ibft_getSignerMetrics";
  private final String JSON_RPC_VERSION = "2.0";
  private IbftGetSignerMetrics method;

  private ValidatorProvider validatorProvider;
  private BlockchainQueries blockchainQueries;
  private BlockInterface blockInterface;

  @BeforeEach
  public void setup() {
    validatorProvider = mock(ValidatorProvider.class);
    blockchainQueries = mock(BlockchainQueries.class);
    blockInterface = mock(BlockInterface.class);
    method = new IbftGetSignerMetrics(validatorProvider, blockInterface, blockchainQueries);
  }

  @Test
  public void returnsCorrectMethodName() {
    assertThat(method.getName()).isEqualTo(IBFT_METHOD);
  }

  @Test
  public void exceptionWhenInvalidStartBlockSupplied() {
    assertThatThrownBy(() -> method.response(requestWithParams("INVALID")))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessageContaining("Invalid start block parameter (index 0)");
  }

  @Test
  public void exceptionWhenInvalidEndBlockSupplied() {
    assertThatThrownBy(() -> method.response(requestWithParams("0x1", "INVALID")))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessageContaining("Invalid end block parameter (index 1)");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getSignerMetricsWhenNoParams() {

    final long startBlock = 1L;
    final long endBlock = 3L;

    when(blockchainQueries.headBlockNumber()).thenReturn(endBlock);

    final List<SignerMetricResult> signerMetricResultList = new ArrayList<>();

    LongStream.range(startBlock, endBlock)
        .forEach(value -> signerMetricResultList.add(generateBlock(value)));

    signerMetricResultList.add(new SignerMetricResult(VALIDATORS[0])); // missing validator

    final JsonRpcRequestContext request = requestWithParams();

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);

    assertThat((Collection<SignerMetricResult>) response.getResult())
        .containsExactlyInAnyOrderElementsOf(signerMetricResultList);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getSignerMetrics() {

    final long startBlock = 1L;
    final long endBlock = 5L;

    when(blockchainQueries.headBlockNumber()).thenReturn(endBlock);

    final List<SignerMetricResult> signerMetricResultList = new ArrayList<>();

    // sign a first block with nodekey number 1
    final SignerMetricResult signerMetricResultFirstNodeKeys = generateBlock(startBlock);
    signerMetricResultList.add(signerMetricResultFirstNodeKeys);
    // sign a second block with nodekey number 2
    final SignerMetricResult signerMetricResultSecondNodeKeys = generateBlock(startBlock + 1);
    signerMetricResultList.add(signerMetricResultSecondNodeKeys);
    // sign a third block with nodekey number 3
    final SignerMetricResult signerMetricResultThirdNodeKeys = generateBlock(startBlock + 2);
    signerMetricResultList.add(signerMetricResultThirdNodeKeys);
    // sign the last block with the nodekey number 1
    generateBlock(startBlock + 3);
    signerMetricResultFirstNodeKeys.setLastProposedBlockNumber(startBlock + 3);
    signerMetricResultFirstNodeKeys.incrementeNbBlock();

    final JsonRpcRequestContext request = requestWithParams();

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);

    assertThat((Collection<SignerMetricResult>) response.getResult())
        .containsExactlyInAnyOrderElementsOf(signerMetricResultList);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getSignerMetricsWhenThereAreFewerBlocksThanTheDefaultRange() {
    final long startBlock = 0L;
    final long headBlock = 2L;

    final List<SignerMetricResult> signerMetricResultList = new ArrayList<>();

    when(blockchainQueries.headBlockNumber()).thenReturn(headBlock);

    LongStream.range(startBlock, headBlock)
        .forEach(value -> signerMetricResultList.add(generateBlock(value)));

    signerMetricResultList.add(new SignerMetricResult(VALIDATORS[2])); // missing validator

    final JsonRpcRequestContext request = requestWithParams();

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);

    // verify getBlockHeaderByNumber is not called with negative values
    verify(blockchainQueries, never()).getBlockHeaderByNumber(lt(0L));

    assertThat((Collection<SignerMetricResult>) response.getResult())
        .containsExactlyInAnyOrderElementsOf(signerMetricResultList);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getSignerMetricsWithLatest() {

    final long startBlock = 1L;
    final long endBlock = 3L;

    final List<SignerMetricResult> signerMetricResultList = new ArrayList<>();

    when(blockchainQueries.headBlockNumber()).thenReturn(endBlock);

    LongStream.range(startBlock, endBlock)
        .forEach(value -> signerMetricResultList.add(generateBlock(value)));

    signerMetricResultList.add(new SignerMetricResult(VALIDATORS[0])); // missing validator

    final JsonRpcRequestContext request =
        requestWithParams("0x" + Long.toHexString(startBlock), "latest");

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);

    assertThat((Collection<SignerMetricResult>) response.getResult())
        .containsExactlyInAnyOrderElementsOf(signerMetricResultList);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getSignerMetricsWithPending() {

    final long startBlock = 1L;
    final long endBlock = 3L;

    final List<SignerMetricResult> signerMetricResultList = new ArrayList<>();

    when(blockchainQueries.headBlockNumber()).thenReturn(endBlock);

    LongStream.range(startBlock, endBlock)
        .forEach(value -> signerMetricResultList.add(generateBlock(value)));

    signerMetricResultList.add(new SignerMetricResult(VALIDATORS[0])); // missing validator

    final JsonRpcRequestContext request =
        requestWithParams("0x" + Long.toHexString(startBlock), "pending");

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);

    assertThat((Collection<SignerMetricResult>) response.getResult())
        .containsExactlyInAnyOrderElementsOf(signerMetricResultList);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getSignerMetricsWithEarliest() {

    final long startBlock = 0L;
    final long endBlock = 3L;

    final List<SignerMetricResult> signerMetricResultList = new ArrayList<>();

    when(blockchainQueries.headBlockNumber()).thenReturn(endBlock);

    LongStream.range(startBlock, endBlock)
        .forEach(value -> signerMetricResultList.add(generateBlock(value)));

    final JsonRpcRequestContext request =
        requestWithParams("earliest", "0x" + Long.toHexString(endBlock));

    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) method.response(request);

    assertThat((Collection<SignerMetricResult>) response.getResult())
        .containsExactlyInAnyOrderElementsOf(signerMetricResultList);
  }

  private JsonRpcRequestContext requestWithParams(final Object... params) {
    return new JsonRpcRequestContext(new JsonRpcRequest(JSON_RPC_VERSION, IBFT_METHOD, params));
  }

  private SignerMetricResult generateBlock(final long number) {
    final Address proposerAddressBlock = VALIDATORS[(int) (number % VALIDATORS.length)];

    final BlockHeader header = new BlockHeaderTestFixture().number(number).buildHeader();

    when(blockchainQueries.getBlockHeaderByNumber(number)).thenReturn(Optional.of(header));
    when(blockInterface.getProposerOfBlock(header)).thenReturn(proposerAddressBlock);
    when(validatorProvider.getValidatorsAfterBlock(header)).thenReturn(Arrays.asList(VALIDATORS));

    final SignerMetricResult signerMetricResult = new SignerMetricResult(proposerAddressBlock);
    signerMetricResult.incrementeNbBlock();
    signerMetricResult.setLastProposedBlockNumber(number);

    return signerMetricResult;
  }
}
