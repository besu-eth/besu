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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleActivation.blockNumber;
import static org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleActivation.timestamp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.Test;

class ProtocolScheduleCustomizationTest {

  @Test
  void activationsRetainTheirDomainEvenWhenTheirValuesMatch() {
    final ProtocolScheduleCustomization customization =
        new ProtocolScheduleCustomization(
            "test",
            List.of(
                new ProtocolSpecModification(blockNumber(10), builder -> builder),
                new ProtocolSpecModification(timestamp(10), builder -> builder)));

    assertThat(customization.toForkIdActivations().blockNumbers()).containsExactly(10L);
    assertThat(customization.toForkIdActivations().timestamps()).containsExactly(10L);
  }

  @Test
  void forkIdActivationsAreDerivedFromRules() {
    final ProtocolScheduleCustomization customization =
        new ProtocolScheduleCustomization(
            "test", List.of(new ProtocolSpecModification(blockNumber(10), builder -> builder)));

    assertThat(customization.toForkIdActivations().blockNumbers()).containsExactly(10L);
  }

  @Test
  void duplicateRulesInTheSameDomainAreRejected() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new ProtocolScheduleCustomization(
                    "test",
                    List.of(
                        new ProtocolSpecModification(blockNumber(10), builder -> builder),
                        new ProtocolSpecModification(blockNumber(10), builder -> builder))))
        .withMessageContaining("More than one");
  }

  @Test
  void structuralModifierRunsBeforePluginModifierAtTheSameBlock() {
    final List<String> trace = new ArrayList<>();
    final ProtocolScheduleCustomization customization =
        customization(
            new ProtocolSpecModification(blockNumber(0), recordingModifier(trace, "plugin")));
    final ProtocolSpecAdapters adapters =
        ProtocolSpecAdapters.compose(
            Map.of(0L, recordingModifier(trace, "structural")), customization);

    final ProtocolSpecBuilder builder = new ProtocolSpecBuilder();
    assertThat(adapters.getModifierForBlock(0).apply(builder)).isSameAs(builder);

    assertThat(trace).containsExactly("structural", "plugin");
  }

  @Test
  void structuralAndPluginModifiersRemainEffectiveAcrossEachOthersLaterBoundaries() {
    final List<String> trace = new ArrayList<>();
    final ProtocolScheduleCustomization customization =
        customization(
            new ProtocolSpecModification(blockNumber(10), recordingModifier(trace, "plugin-10")));
    final ProtocolSpecAdapters adapters =
        ProtocolSpecAdapters.compose(
            Map.of(
                0L,
                recordingModifier(trace, "structural-0"),
                20L,
                recordingModifier(trace, "structural-20")),
            customization);
    final ProtocolSpecBuilder builder = new ProtocolSpecBuilder();

    assertThat(adapters.getModifierForBlock(10).apply(builder)).isSameAs(builder);
    assertThat(trace).containsExactly("structural-0", "plugin-10");

    trace.clear();
    assertThat(adapters.getModifierForBlock(20).apply(builder)).isSameAs(builder);
    assertThat(trace).containsExactly("structural-20", "plugin-10");
  }

  @Test
  void timestampModifierComposesAfterTheStructuralModifier() {
    final List<String> trace = new ArrayList<>();
    final ProtocolScheduleCustomization customization =
        customization(
            new ProtocolSpecModification(timestamp(100), recordingModifier(trace, "timestamp")));
    final ProtocolSpecAdapters adapters =
        ProtocolSpecAdapters.compose(Map.of(0L, recordingModifier(trace, "block")), customization);

    final ProtocolSpecBuilder builder = new ProtocolSpecBuilder();
    assertThat(adapters.getModifierForTimestamp(100).apply(builder)).isSameAs(builder);

    assertThat(trace).containsExactly("block", "timestamp");
  }

  @Test
  void aContributedBlockModifierDoesNotReachTheTimestampEra() {
    final List<String> trace = new ArrayList<>();
    final ProtocolScheduleCustomization customization =
        new ProtocolScheduleCustomization(
            "test",
            List.of(
                new ProtocolSpecModification(blockNumber(100), recordingModifier(trace, "block")),
                new ProtocolSpecModification(
                    timestamp(1000), recordingModifier(trace, "timestamp"))));
    final ProtocolSpecAdapters adapters = ProtocolSpecAdapters.compose(Map.of(), customization);

    final ProtocolSpecBuilder builder = new ProtocolSpecBuilder();
    assertThat(adapters.getModifierForTimestamp(1000).apply(builder)).isSameAs(builder);

    // block 100 may not have been reached when timestamp 1000 is, and the two cannot be ordered
    // against each other, so the block-era overlay must not be applied here
    assertThat(trace).containsExactly("timestamp");
  }

  private static ProtocolScheduleCustomization customization(
      final ProtocolSpecModification modification) {
    return new ProtocolScheduleCustomization("test", List.of(modification));
  }

  private static UnaryOperator<ProtocolSpecBuilder> recordingModifier(
      final List<String> trace, final String label) {
    return builder -> {
      trace.add(label);
      return builder;
    };
  }
}
