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
package org.hyperledger.besu.cli;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration;
import org.hyperledger.besu.ethereum.core.plugins.PluginsVerificationMode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

public class PluginsOptionsTest extends CommandTestAbstract {

  @Captor protected ArgumentCaptor<PluginConfiguration> pluginConfigurationArgumentCaptor;

  @Test
  public void shouldParsePluginOptionForSinglePlugin() {
    parseCommand("--plugins", "pluginA");
    verify(getBesuPluginContext()).initialize(pluginConfigurationArgumentCaptor.capture());
    assertThat(pluginConfigurationArgumentCaptor.getValue().getRequestedPlugins())
        .isEqualTo(List.of("pluginA"));
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void shouldParsePluginOptionForMultiplePlugins() {
    parseCommand("--plugins", "pluginA,pluginB");
    verify(getBesuPluginContext()).initialize(pluginConfigurationArgumentCaptor.capture());
    assertThat(pluginConfigurationArgumentCaptor.getValue().getRequestedPlugins())
        .isEqualTo(List.of("pluginA", "pluginB"));

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void shouldNotUsePluginOptionWhenNoPluginsSpecified() {
    parseCommand();
    verify(getBesuPluginContext()).initialize(pluginConfigurationArgumentCaptor.capture());
    assertThat(pluginConfigurationArgumentCaptor.getValue().getRequestedPlugins())
        .isEqualTo(List.of());
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void shouldNotParseAnyPluginsWhenPluginOptionIsEmpty() {
    parseCommand("--plugins", "");
    verify(getBesuPluginContext()).initialize(pluginConfigurationArgumentCaptor.capture());
    assertThat(pluginConfigurationArgumentCaptor.getValue().getRequestedPlugins())
        .isEqualTo(List.of());
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void shouldParsePluginsExternalEnabledOptionWhenFalse() {
    parseCommand("--Xplugins-external-enabled=false");
    verify(getBesuPluginContext()).initialize(pluginConfigurationArgumentCaptor.capture());

    assertThat(pluginConfigurationArgumentCaptor.getValue().isExternalPluginsEnabled())
        .isEqualTo(false);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void shouldParsePluginsExternalEnabledOptionWhenTrue() {
    parseCommand("--Xplugins-external-enabled=true");
    verify(getBesuPluginContext()).initialize(pluginConfigurationArgumentCaptor.capture());

    assertThat(pluginConfigurationArgumentCaptor.getValue().isExternalPluginsEnabled())
        .isEqualTo(true);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void shouldEnablePluginsExternalByDefault() {
    parseCommand();
    verify(getBesuPluginContext()).initialize(pluginConfigurationArgumentCaptor.capture());
    assertThat(pluginConfigurationArgumentCaptor.getValue().isExternalPluginsEnabled())
        .isEqualTo(true);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void shouldFailWhenPluginsIsDisabledAndPluginsExplicitlyRequested() {
    parseCommand("--Xplugins-external-enabled=false", "--plugins", "pluginA");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "--plugins-dir, --plugins, --plugin-continue-on-error and --plugins-verification-mode options can only be used when --Xplugins-external-enabled is true");
  }

  @Test
  public void shouldHaveContinueOnErrorFalseByDefault() {
    parseCommand();
    verify(getBesuPluginContext()).initialize(pluginConfigurationArgumentCaptor.capture());
    assertThat(pluginConfigurationArgumentCaptor.getValue().isContinueOnPluginError())
        .isEqualTo(false);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void shouldUseContinueOnErrorWhenTrue() {
    parseCommand("--plugin-continue-on-error=true");
    verify(getBesuPluginContext()).initialize(pluginConfigurationArgumentCaptor.capture());

    assertThat(pluginConfigurationArgumentCaptor.getValue().isContinueOnPluginError())
        .isEqualTo(true);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void shouldFailWhenPluginsIsDisabledAnHaltOnErrorTrue() {
    parseCommand("--Xplugins-external-enabled=false", "--plugin-continue-on-error=true");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "--plugins-dir, --plugins, --plugin-continue-on-error and --plugins-verification-mode options can only be used when --Xplugins-external-enabled is true");
  }

  @Test
  public void shouldHaveVerificationModeNoneByDefault() {
    parseCommand();
    verify(getBesuPluginContext()).initialize(pluginConfigurationArgumentCaptor.capture());
    assertThat(pluginConfigurationArgumentCaptor.getValue().getPluginsVerificationMode())
        .isEqualTo(PluginsVerificationMode.NONE);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void shouldUseVerificationModeWhenSet() {
    parseCommand("--plugins-verification-mode=FULL");
    verify(getBesuPluginContext()).initialize(pluginConfigurationArgumentCaptor.capture());
    assertThat(pluginConfigurationArgumentCaptor.getValue().getPluginsVerificationMode())
        .isEqualTo(PluginsVerificationMode.FULL);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void shouldFailWhenPluginsIsDisabledAndPluginsDirExplicitlySet() {
    parseCommand("--Xplugins-external-enabled=false", "--plugins-dir", "/some/plugins");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "--plugins-dir, --plugins, --plugin-continue-on-error and --plugins-verification-mode options can only be used when --Xplugins-external-enabled is true");
  }

  @Test
  public void shouldDefaultThePluginsDirToThePluginsDirectoryUnderBesuHome() throws IOException {
    // besu.home is also the default data path, which the command creates, so it must be writable
    final Path home = Files.createTempDirectory("besu-home-");
    home.toFile().deleteOnExit();
    final String previousPluginsDir = System.clearProperty("besu.plugins.dir");
    final String previousHome = System.setProperty("besu.home", home.toString());
    try {
      parseCommand();
      verify(getBesuPluginContext()).initialize(pluginConfigurationArgumentCaptor.capture());
      assertThat(pluginConfigurationArgumentCaptor.getValue().getPluginsDir())
          .isEqualTo(home.resolve("plugins"));
    } finally {
      restoreSystemProperty("besu.plugins.dir", previousPluginsDir);
      restoreSystemProperty("besu.home", previousHome);
    }

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void shouldDefaultThePluginsDirToTheSystemPropertyWhenSet() {
    final String previous = System.setProperty("besu.plugins.dir", "/prop/plugins");
    try {
      parseCommand();
      verify(getBesuPluginContext()).initialize(pluginConfigurationArgumentCaptor.capture());
      assertThat(pluginConfigurationArgumentCaptor.getValue().getPluginsDir())
          .isEqualTo(Path.of("/prop/plugins"));
    } finally {
      restoreSystemProperty("besu.plugins.dir", previous);
    }

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void shouldPreferThePluginsDirOptionOverTheSystemProperty() {
    final String previous = System.setProperty("besu.plugins.dir", "/prop/plugins");
    try {
      parseCommand("--plugins-dir", "/some/plugins");
      verify(getBesuPluginContext()).initialize(pluginConfigurationArgumentCaptor.capture());
      assertThat(pluginConfigurationArgumentCaptor.getValue().getPluginsDir())
          .isEqualTo(Path.of("/some/plugins"));
    } finally {
      restoreSystemProperty("besu.plugins.dir", previous);
    }

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void shouldReadThePluginsDirFromTheCommandLine() {
    parseCommand("--plugins-dir", "/some/plugins");
    verify(getBesuPluginContext()).initialize(pluginConfigurationArgumentCaptor.capture());
    assertThat(pluginConfigurationArgumentCaptor.getValue().getPluginsDir())
        .isEqualTo(Path.of("/some/plugins"));

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void shouldReadThePluginsDirFromTheEnvironment() {
    setEnvironmentVariable("BESU_PLUGINS_DIR", "/env/plugins");
    parseCommand();
    verify(getBesuPluginContext()).initialize(pluginConfigurationArgumentCaptor.capture());
    assertThat(pluginConfigurationArgumentCaptor.getValue().getPluginsDir())
        .isEqualTo(Path.of("/env/plugins"));

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void shouldReadThePluginsDirFromTheConfigFile() throws IOException {
    final Path toml = createTempFile("toml", "plugins-dir=\"/toml/plugins\"\n");
    parseCommand("--config-file", toml.toString());
    verify(getBesuPluginContext()).initialize(pluginConfigurationArgumentCaptor.capture());
    assertThat(pluginConfigurationArgumentCaptor.getValue().getPluginsDir())
        .isEqualTo(Path.of("/toml/plugins"));

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  private static void restoreSystemProperty(final String name, final String previous) {
    if (previous == null) {
      System.clearProperty(name);
    } else {
      System.setProperty(name, previous);
    }
  }
}
