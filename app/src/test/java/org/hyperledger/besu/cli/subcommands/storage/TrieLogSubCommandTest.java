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
package org.hyperledger.besu.cli.subcommands.storage;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.cli.CommandTestAbstract;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;

import java.util.List;

import org.junit.jupiter.api.Test;

class TrieLogSubCommandTest extends CommandTestAbstract {

  @Test
  void limitTrieLogsDefaultDisabledForAllSubcommands() {
    assertTrieLogSubcommand("prune");
    assertTrieLogSubcommand("count");
    assertTrieLogSubcommand("import");
    assertTrieLogSubcommand("export");
  }

  @Test
  void limitTrieLogsDisabledForAllSubcommands() {
    assertTrieLogSubcommandWithExplicitLimitEnabled("prune");
    assertTrieLogSubcommandWithExplicitLimitEnabled("count");
    assertTrieLogSubcommandWithExplicitLimitEnabled("import");
    assertTrieLogSubcommandWithExplicitLimitEnabled("export");
  }

  private void assertTrieLogSubcommand(final String trieLogSubcommand) {
    parseCommand("storage", "trie-log", trieLogSubcommand);
    assertConfigurationIsDisabledBySubcommand();
  }

  private void assertTrieLogSubcommandWithExplicitLimitEnabled(final String trieLogSubcommand) {
    parseCommand("--bonsai-limit-trie-logs-enabled=true", "storage", "trie-log", trieLogSubcommand);
    assertConfigurationIsDisabledBySubcommand();
  }

  private void assertConfigurationIsDisabledBySubcommand() {
    verify(mockControllerBuilder, atLeastOnce())
        .dataStorageConfiguration(dataStorageConfigurationArgumentCaptor.capture());
    final List<DataStorageConfiguration> configs =
        dataStorageConfigurationArgumentCaptor.getAllValues();
    assertThat(configs.get(0).getExtraStorageConfiguration().getLimitTrieLogsEnabled()).isTrue();
    assertThat(configs.get(1).getExtraStorageConfiguration().getLimitTrieLogsEnabled()).isFalse();
  }

  @Test
  void limitTrieLogsDefaultEnabledForBesuMainCommand() {
    parseCommand();
    verify(mockControllerBuilder, atLeastOnce())
        .dataStorageConfiguration(dataStorageConfigurationArgumentCaptor.capture());
    final List<DataStorageConfiguration> configs =
        dataStorageConfigurationArgumentCaptor.getAllValues();
    assertThat(configs)
        .allMatch(
            dataStorageConfiguration ->
                dataStorageConfiguration.getExtraStorageConfiguration().getLimitTrieLogsEnabled());
  }
}
