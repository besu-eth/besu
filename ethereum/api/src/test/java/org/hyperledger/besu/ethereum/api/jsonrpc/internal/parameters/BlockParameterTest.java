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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class BlockParameterTest {
  @Test
  public void earliestShouldReturnZeroNumberValue() {
    final BlockParameter blockParameter = new BlockParameter("earliest");
    assertThat(blockParameter.getNumber()).isPresent();
    assertThat(blockParameter.getNumber().get()).isEqualTo(0);

    assertThat(blockParameter.isEarliest()).isTrue();
    assertThat(blockParameter.isFinalized()).isFalse();
    assertThat(blockParameter.isLatest()).isFalse();
    assertThat(blockParameter.isNumeric()).isFalse();
    assertThat(blockParameter.isPending()).isFalse();
    assertThat(blockParameter.isSafe()).isFalse();
  }

  @Test
  public void latestShouldReturnEmptyNumberValue() {
    final BlockParameter blockParameter = new BlockParameter("latest");
    assertThat(blockParameter.getNumber()).isEmpty();

    assertThat(blockParameter.isLatest()).isTrue();
    assertThat(blockParameter.isEarliest()).isFalse();
    assertThat(blockParameter.isFinalized()).isFalse();
    assertThat(blockParameter.isNumeric()).isFalse();
    assertThat(blockParameter.isPending()).isFalse();
    assertThat(blockParameter.isSafe()).isFalse();
  }

  @Test
  public void pendingShouldReturnEmptyNumberValue() {
    final BlockParameter blockParameter = new BlockParameter("pending");
    assertThat(blockParameter.getNumber()).isEmpty();

    assertThat(blockParameter.isPending()).isTrue();
    assertThat(blockParameter.isEarliest()).isFalse();
    assertThat(blockParameter.isFinalized()).isFalse();
    assertThat(blockParameter.isLatest()).isFalse();
    assertThat(blockParameter.isNumeric()).isFalse();
    assertThat(blockParameter.isSafe()).isFalse();
  }

  @Test
  public void finalizedShouldReturnEmptyNumberValue() {
    final BlockParameter blockParameter = new BlockParameter("finalized");
    assertThat(blockParameter.getNumber()).isEmpty();

    assertThat(blockParameter.isFinalized()).isTrue();
    assertThat(blockParameter.isEarliest()).isFalse();
    assertThat(blockParameter.isLatest()).isFalse();
    assertThat(blockParameter.isNumeric()).isFalse();
    assertThat(blockParameter.isPending()).isFalse();
    assertThat(blockParameter.isSafe()).isFalse();
  }

  @Test
  public void safeShouldReturnEmptyNumberValue() {
    final BlockParameter blockParameter = new BlockParameter("safe");
    assertThat(blockParameter.getNumber()).isEmpty();

    assertThat(blockParameter.isSafe()).isTrue();
    assertThat(blockParameter.isEarliest()).isFalse();
    assertThat(blockParameter.isFinalized()).isFalse();
    assertThat(blockParameter.isLatest()).isFalse();
    assertThat(blockParameter.isNumeric()).isFalse();
    assertThat(blockParameter.isPending()).isFalse();
  }

  @Test
  public void decimalStringShouldThrowException() {
    assertThatThrownBy(() -> new BlockParameter("7"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be a hex string with 0x prefix");
  }

  @Test
  public void longShouldReturnLongNumberValue() {
    final BlockParameter blockParameter = new BlockParameter(5);
    assertThat(blockParameter.getNumber()).isPresent();
    assertThat(blockParameter.getNumber().get()).isEqualTo(5L);

    assertThat(blockParameter.isNumeric()).isTrue();
    assertThat(blockParameter.isEarliest()).isFalse();
    assertThat(blockParameter.isFinalized()).isFalse();
    assertThat(blockParameter.isLatest()).isFalse();
    assertThat(blockParameter.isPending()).isFalse();
    assertThat(blockParameter.isSafe()).isFalse();
  }

  @Test
  public void multiDigitDecimalStringShouldThrowException() {
    assertThatThrownBy(() -> new BlockParameter("55"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be a hex string with 0x prefix");
  }

  @Test
  public void hexShouldReturnLongNumberValue() {
    final BlockParameter blockParameter = new BlockParameter("0x55");
    assertThat(blockParameter.getNumber()).isPresent();
    assertThat(blockParameter.getNumber().get()).isEqualTo(85L);

    assertThat(blockParameter.isNumeric()).isTrue();
    assertThat(blockParameter.isEarliest()).isFalse();
    assertThat(blockParameter.isFinalized()).isFalse();
    assertThat(blockParameter.isLatest()).isFalse();
    assertThat(blockParameter.isPending()).isFalse();
    assertThat(blockParameter.isSafe()).isFalse();
  }

  @Test
  public void upperCaseStringShouldBeHandled() {
    final BlockParameter blockParameter = new BlockParameter("LATEST");
    assertThat(blockParameter.getNumber()).isEmpty();
    assertThat(blockParameter.isLatest()).isTrue();
    assertThat(blockParameter.isEarliest()).isFalse();
    assertThat(blockParameter.isFinalized()).isFalse();
    assertThat(blockParameter.isNumeric()).isFalse();
    assertThat(blockParameter.isPending()).isFalse();
    assertThat(blockParameter.isSafe()).isFalse();
  }

  @Test
  public void mixedCaseStringShouldBeHandled() {
    final BlockParameter blockParameter = new BlockParameter("lATest");
    assertThat(blockParameter.getNumber()).isEmpty();
    assertThat(blockParameter.isLatest()).isTrue();
    assertThat(blockParameter.isEarliest()).isFalse();
    assertThat(blockParameter.isFinalized()).isFalse();
    assertThat(blockParameter.isNumeric()).isFalse();
    assertThat(blockParameter.isPending()).isFalse();
    assertThat(blockParameter.isSafe()).isFalse();
  }

  @Test
  public void invalidValueShouldThrowException() {
    assertThatThrownBy(() -> new BlockParameter("invalid"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be a hex string with 0x prefix");
  }
}
