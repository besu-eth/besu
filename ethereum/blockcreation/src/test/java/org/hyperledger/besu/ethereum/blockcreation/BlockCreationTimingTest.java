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
package org.hyperledger.besu.ethereum.blockcreation;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;

public class BlockCreationTimingTest {

  @Test
  public void registerValueIsPrintedAsIsAndDoesNotAffectDeltaChain() {
    final BlockCreationTiming timing = new BlockCreationTiming();

    timing.register("preTxsSelection");
    timing.register("txsSelection");
    timing.registerValue("txsSelectionHighScore", Duration.ofMillis(42));
    timing.register("postTxsSelection");

    final String rendered = timing.toString();

    assertThat(rendered).contains("txsSelectionHighScore=42ms");
    assertThat(rendered).contains("preTxsSelection=");
    assertThat(rendered).contains("txsSelection=");
    assertThat(rendered).contains("postTxsSelection=");
    assertThat(rendered.indexOf("txsSelection="))
        .isLessThan(rendered.indexOf("txsSelectionHighScore="));
    assertThat(rendered.indexOf("txsSelectionHighScore="))
        .isLessThan(rendered.indexOf("postTxsSelection="));
  }

  @Test
  public void registerValueOfZeroIsStillPrinted() {
    final BlockCreationTiming timing = new BlockCreationTiming();
    timing.register("start");
    timing.registerValue("txsSelectionHighScore", Duration.ZERO);

    assertThat(timing.toString()).contains("txsSelectionHighScore=0ms");
  }
}
