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
    timing.registerValue("selectedTxsEvaluation", Duration.ofMillis(42));
    timing.register("postTxsSelection");

    final String rendered = timing.toString();

    assertThat(rendered).contains("selectedTxsEvaluation=42ms");
    assertThat(rendered).contains("preTxsSelection=");
    assertThat(rendered).contains("txsSelection=");
    assertThat(rendered).contains("postTxsSelection=");
    assertThat(rendered.indexOf("txsSelection="))
        .isLessThan(rendered.indexOf("selectedTxsEvaluation="));
    assertThat(rendered.indexOf("selectedTxsEvaluation="))
        .isLessThan(rendered.indexOf("postTxsSelection="));
  }

  @Test
  public void registerValueOfZeroIsStillPrinted() {
    final BlockCreationTiming timing = new BlockCreationTiming();
    timing.register("start");
    timing.registerValue("selectedTxsEvaluation", Duration.ZERO);

    assertThat(timing.toString()).contains("selectedTxsEvaluation=0ms");
  }

  @Test
  public void registerAllPreservesStandaloneValuesIntoOuterTiming() {
    // Mirrors what BlockMiner.mineBlock() does: an inner timing built by
    // AbstractBlockCreator.createBlock(...) is merged into an outer timing via registerAll,
    // and the outer's toString() is what BlockMiner.logProducedBlock prints.
    // The standalone selectedTxsEvaluation value must survive the merge as-is, not be
    // treated as a stopwatch delta.
    final BlockCreationTiming inner = new BlockCreationTiming();
    inner.register("duplicateWorldState");
    inner.register("preTxsSelection");
    inner.register("txsSelection");
    inner.registerValue("selectedTxsEvaluation", Duration.ofMillis(42));
    inner.register("blockAssembled");

    final BlockCreationTiming outer = new BlockCreationTiming();
    outer.register("protocolWait");
    outer.registerAll(inner);
    outer.register("importingBlock");

    final String rendered = outer.toString();

    assertThat(rendered).contains("selectedTxsEvaluation=42ms");
    assertThat(rendered.indexOf("txsSelection="))
        .isLessThan(rendered.indexOf("selectedTxsEvaluation="));
    assertThat(rendered.indexOf("selectedTxsEvaluation="))
        .isLessThan(rendered.indexOf("blockAssembled="));
  }
}
