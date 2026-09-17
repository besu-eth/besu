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
package org.hyperledger.besu.ethereum.core.feemarket;

import static com.google.common.base.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class BaseFeeMarketBaseFeeTest {

  private final BaseFeeMarket baseFeeMarket = FeeMarket.london(0);

  public static Stream<Arguments> data() {
    try {
      final List<Object[]> data = new ArrayList<>();
      final String testFilePath = "basefee-test.json";
      final URL testFileUrl = BaseFeeMarketBaseFeeTest.class.getResource(testFilePath);
      checkState(testFileUrl != null, "Cannot find test file " + testFilePath);
      final String testSuiteJson = Resources.toString(testFileUrl, StandardCharsets.UTF_8);
      final ObjectMapper objectMapper = new ObjectMapper();
      final BaseFeeMarketBaseFeeTestCase[] testCases =
          objectMapper.readValue(testSuiteJson, BaseFeeMarketBaseFeeTestCase[].class);
      for (final BaseFeeMarketBaseFeeTestCase testCase : testCases) {
        data.add(
            new Object[] {
              testCase.parentBaseFee,
              testCase.parentGasUsed,
              testCase.parentTargetGasUsed,
              testCase.expectedBaseFee
            });
      }
      return data.stream().map(Arguments::of);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  @Disabled("Need to have spec frozen to define correct values")
  public void assertThatBaseFeeIsCorrect(
      final Wei parentBaseFee,
      final long parentGasUsed,
      final long parentTargetGasUsed,
      final Wei expectedBaseFee) {
    assertThat(baseFeeMarket.computeBaseFee(0L, parentBaseFee, parentGasUsed, parentTargetGasUsed))
        .isEqualTo(expectedBaseFee);
  }

  @SuppressWarnings("unused")
  private static class BaseFeeMarketBaseFeeTestCase {

    private Wei parentBaseFee;
    private long parentGasUsed;
    private long parentTargetGasUsed;
    private Wei expectedBaseFee;

    public void setParentBaseFee(final Wei parentBaseFee) {
      this.parentBaseFee = parentBaseFee;
    }

    public void setParentGasUsed(final long parentGasUsed) {
      this.parentGasUsed = parentGasUsed;
    }

    public void setParentTargetGasUsed(final long parentTargetGasUsed) {
      this.parentTargetGasUsed = parentTargetGasUsed;
    }

    public void setExpectedBaseFee(final Wei expectedBaseFee) {
      this.expectedBaseFee = expectedBaseFee;
    }
  }
}
