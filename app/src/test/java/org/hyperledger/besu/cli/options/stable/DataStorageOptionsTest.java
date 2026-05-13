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
package org.hyperledger.besu.cli.options.stable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration.MINIMUM_TRIE_LOG_RETENTION_LIMIT;

import org.hyperledger.besu.cli.options.AbstractCLIOptionsTest;
import org.hyperledger.besu.cli.options.storage.DataStorageOptions;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import org.junit.jupiter.api.Test;

public class DataStorageOptionsTest
    extends AbstractCLIOptionsTest<DataStorageConfiguration, DataStorageOptions> {

  @Test
  public void bonsaiTrieLogPruningLimitOption() {
    internalTestSuccess(
        dataStorageConfiguration ->
            assertThat(
                    dataStorageConfiguration
                        .getPathBasedExtraStorageConfiguration()
                        .getTrieLogPruningWindowSize())
                .isEqualTo(600),
        "--bonsai-limit-trie-logs-enabled",
        "--bonsai-trie-logs-pruning-window-size",
        "600");
  }

  @Test
  public void bonsaiTrieLogsEnabled_explicitlySetToFalse() {
    internalTestSuccess(
        dataStorageConfiguration ->
            assertThat(
                    dataStorageConfiguration
                        .getPathBasedExtraStorageConfiguration()
                        .getLimitTrieLogsEnabled())
                .isEqualTo(false),
        "--bonsai-limit-trie-logs-enabled=false");
  }

  @Test
  public void pathbasedTrieLogPruningWindowSizeShouldBePositive() {
    internalTestFailure(
        "--bonsai-trie-logs-pruning-window-size=0 must be greater than 0",
        "--bonsai-limit-trie-logs-enabled",
        "--bonsai-trie-logs-pruning-window-size",
        "0");
  }

  @Test
  public void pathbasedTrieLogPruningWindowSizeShouldBeAboveRetentionLimit() {
    internalTestFailure(
        "--bonsai-trie-logs-pruning-window-size=512 must be greater than --bonsai-historical-block-limit=512",
        "--bonsai-limit-trie-logs-enabled",
        "--bonsai-trie-logs-pruning-window-size",
        "512");
  }

  @Test
  public void bonsaiTrieLogRetentionLimitOption() {
    internalTestSuccess(
        dataStorageConfiguration ->
            assertThat(
                    dataStorageConfiguration
                        .getPathBasedExtraStorageConfiguration()
                        .getMaxLayersToLoad())
                .isEqualTo(MINIMUM_TRIE_LOG_RETENTION_LIMIT + 1),
        "--bonsai-limit-trie-logs-enabled",
        "--bonsai-historical-block-limit",
        "513");
  }

  @Test
  public void bonsaiTrieLogRetentionLimitOption_boundaryTest() {
    internalTestSuccess(
        dataStorageConfiguration ->
            assertThat(
                    dataStorageConfiguration
                        .getPathBasedExtraStorageConfiguration()
                        .getMaxLayersToLoad())
                .isEqualTo(MINIMUM_TRIE_LOG_RETENTION_LIMIT),
        "--bonsai-limit-trie-logs-enabled",
        "--bonsai-historical-block-limit",
        "512");
  }

  @Test
  public void bonsaiTrieLogRetentionLimitShouldBeAboveMinimum() {
    internalTestFailure(
        "--bonsai-historical-block-limit minimum value is 512",
        "--bonsai-limit-trie-logs-enabled",
        "--bonsai-historical-block-limit",
        "511");
  }

  @Test
  public void pathbasedCodeUsingCodeHashEnabledCanBeEnabled() {
    internalTestSuccess(
        dataStorageConfiguration ->
            assertThat(
                    dataStorageConfiguration
                        .getPathBasedExtraStorageConfiguration()
                        .getUnstable()
                        .getCodeStoredByCodeHashEnabled())
                .isEqualTo(true),
        "--Xbonsai-code-using-code-hash-enabled",
        "true");
  }

  @Test
  public void pathbasedCodeUsingCodeHashEnabledCanBeDisabled() {
    internalTestSuccess(
        dataStorageConfiguration ->
            assertThat(
                    dataStorageConfiguration
                        .getPathBasedExtraStorageConfiguration()
                        .getUnstable()
                        .getCodeStoredByCodeHashEnabled())
                .isEqualTo(false),
        "--Xbonsai-code-using-code-hash-enabled",
        "false");
  }

  @Test
  public void parallelTxProcessingEnabledByDefault() {
    internalTestSuccess(
        dataStorageConfiguration ->
            assertThat(
                    dataStorageConfiguration
                        .getPathBasedExtraStorageConfiguration()
                        .getParallelTxProcessingEnabled())
                .isEqualTo(true));
  }

  @Test
  public void parallelTxProcessingCanBeEnabled() {
    internalTestSuccess(
        dataStorageConfiguration ->
            assertThat(
                    dataStorageConfiguration
                        .getPathBasedExtraStorageConfiguration()
                        .getParallelTxProcessingEnabled())
                .isEqualTo(true),
        "--bonsai-parallel-tx-processing-enabled=true");
  }

  @Test
  public void parallelTxProcessingCanBeDisabled() {
    internalTestSuccess(
        dataStorageConfiguration ->
            assertThat(
                    dataStorageConfiguration
                        .getPathBasedExtraStorageConfiguration()
                        .getParallelTxProcessingEnabled())
                .isEqualTo(false),
        "--bonsai-parallel-tx-processing-enabled=false");
  }

  @Test
  public void parallelStateRootComputationEnabledByDefault() {
    internalTestSuccess(
        dataStorageConfiguration ->
            assertThat(
                    dataStorageConfiguration
                        .getPathBasedExtraStorageConfiguration()
                        .getParallelStateRootComputationEnabled())
                .isEqualTo(true));
  }

  @Test
  public void parallelStateRootComputationCanBeEnabled() {
    internalTestSuccess(
        dataStorageConfiguration ->
            assertThat(
                    dataStorageConfiguration
                        .getPathBasedExtraStorageConfiguration()
                        .getParallelStateRootComputationEnabled())
                .isEqualTo(true),
        "--bonsai-parallel-state-root-computation-enabled=true");
  }

  @Test
  public void parallelStateRootComputationCanBeDisabled() {
    internalTestSuccess(
        dataStorageConfiguration ->
            assertThat(
                    dataStorageConfiguration
                        .getPathBasedExtraStorageConfiguration()
                        .getParallelStateRootComputationEnabled())
                .isEqualTo(false),
        "--bonsai-parallel-state-root-computation-enabled=false");
  }

  @Test
  public void receiptCompactionCanBeEnabledWithImplicitTrueValue() {
    internalTestSuccess(
        dataStorageConfiguration ->
            assertThat(dataStorageConfiguration.getReceiptCompactionEnabled()).isEqualTo(true),
        "--receipt-compaction-enabled");
  }

  @Test
  public void receiptCompactionCanBeEnabled() {
    internalTestSuccess(
        dataStorageConfiguration ->
            assertThat(dataStorageConfiguration.getReceiptCompactionEnabled()).isEqualTo(true),
        "--receipt-compaction-enabled=true");
  }

  @Test
  public void receiptCompactionCanBeDisabled() {
    internalTestSuccess(
        dataStorageConfiguration ->
            assertThat(dataStorageConfiguration.getReceiptCompactionEnabled()).isEqualTo(false),
        "--receipt-compaction-enabled=false");
  }

  @Test
  public void revertReasonCanBeEnabled() {
    internalTestSuccess(
        dataStorageConfiguration ->
            assertThat(dataStorageConfiguration.getRevertReasonEnabled()).isTrue(),
        "--revert-reason-enabled=true");
  }

  @Test
  public void revertReasonCanBeDisabled() {
    internalTestSuccess(
        dataStorageConfiguration ->
            assertThat(dataStorageConfiguration.getRevertReasonEnabled()).isFalse(),
        "--revert-reason-enabled=false");
  }

  @Override
  protected DataStorageConfiguration createDefaultDomainObject() {
    return DataStorageConfiguration.DEFAULT_CONFIG;
  }

  @Override
  protected DataStorageConfiguration createCustomizedDomainObject() {
    return ImmutableDataStorageConfiguration.builder()
        .dataStorageFormat(DataStorageFormat.BONSAI)
        .pathBasedExtraStorageConfiguration(
            ImmutablePathBasedExtraStorageConfiguration.builder()
                .maxLayersToLoad(513L)
                .limitTrieLogsEnabled(true)
                .trieLogPruningWindowSize(514)
                .parallelTxProcessingEnabled(true)
                .parallelStateRootComputationEnabled(true)
                .build())
        .revertReasonEnabled(true)
        .build();
  }

  @Override
  protected DataStorageOptions optionsFromDomainObject(
      final DataStorageConfiguration domainObject) {
    return DataStorageOptions.fromConfig(domainObject);
  }

  @Override
  protected DataStorageOptions getOptionsFromBesuCommand(final TestBesuCommand besuCommand) {
    return besuCommand.getDataStorageOptions();
  }
}
