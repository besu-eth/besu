/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.evm.precompile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.evm.frame.MessageFrame;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class KZGPointEvalPrecompileContractTest {
  private static KZGPointEvalPrecompiledContract contract;
  private final MessageFrame toRun = mock(MessageFrame.class);

  @BeforeAll
  public static void init() {
    KZGPointEvalPrecompiledContract.init();
    contract = new KZGPointEvalPrecompiledContract();
  }

  @AfterAll
  public static void tearDown() {
    KZGPointEvalPrecompiledContract.tearDown();
  }

  @ParameterizedTest(name = "{index}")
  @MethodSource("getPointEvaluationPrecompileTestVectors")
  void testComputePrecompile(final PrecompileTestParameters parameters) {
    when(toRun.getVersionedHashes()).thenReturn(Optional.of(List.of(parameters.versionedHash)));
    PrecompiledContract.PrecompileContractResult result =
        contract.computePrecompile(parameters.input, toRun);
    if (parameters.valid) {
      assertThat(result.state()).isEqualTo(MessageFrame.State.COMPLETED_SUCCESS);
      assertThat(result.output()).isEqualTo(parameters.returnValue);
    } else {
      assertThat(result.state()).isNotEqualTo(MessageFrame.State.COMPLETED_SUCCESS);
    }
  }

  public static List<PrecompileTestParameters> getPointEvaluationPrecompileTestVectors()
      throws IOException {
    final JsonNode jsonNode;
    try (final InputStream testVectors =
        KZGPointEvalPrecompileContractTest.class.getResourceAsStream(
            "pointEvaluationPrecompile.json")) {
      jsonNode = new ObjectMapper().readTree(testVectors);
    }
    final ArrayNode testCases = (ArrayNode) jsonNode.get("TestCases");
    final Bytes returnValue = Bytes.fromHexString(jsonNode.get("PrecompileReturnValue").asText());
    return IntStream.range(0, testCases.size())
        .mapToObj(
            i -> {
              final JsonNode testCase = testCases.get(i);
              final Bytes input = Bytes.fromHexString(testCase.get("Input").asText());
              final boolean valid = testCase.get("Valid").asBoolean();
              return new PrecompileTestParameters(
                  input, valid, returnValue, new VersionedHash(Bytes32.wrap(input.slice(0, 32))));
            })
        .collect(Collectors.toList());
  }

  record PrecompileTestParameters(
      Bytes input, boolean valid, Bytes returnValue, VersionedHash versionedHash) {}
}
