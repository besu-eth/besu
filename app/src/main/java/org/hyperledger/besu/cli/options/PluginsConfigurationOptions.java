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
package org.hyperledger.besu.cli.options;

import static org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration.DEFAULT_CONTINUE_ON_PLUGIN_ERROR;
import static org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration.DEFAULT_EXTERNAL_PLUGINS_ENABLED;
import static org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration.DEFAULT_PLUGINS_VERIFICATION_MODE;
import static org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration.DEFAULT_REQUESTED_PLUGINS_INFO;

import org.hyperledger.besu.cli.converter.PluginInfoConverter;
import org.hyperledger.besu.cli.util.CommandLineUtils;
import org.hyperledger.besu.ethereum.core.plugins.ImmutablePluginConfiguration;
import org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration;
import org.hyperledger.besu.ethereum.core.plugins.PluginInfo;
import org.hyperledger.besu.ethereum.core.plugins.PluginsVerificationMode;

import java.util.List;

import picocli.CommandLine;

/** The Plugins options. */
public class PluginsConfigurationOptions implements CLIOptions<PluginConfiguration> {

  private static final String PLUGINS_OPTION_NAME = "--plugins";
  private static final String CONTINUE_ON_PLUGIN_ERROR_OPTION_NAME = "--plugin-continue-on-error";
  private static final String PLUGINS_EXTERNAL_ENABLED_OPTION_NAME = "--Xplugins-external-enabled";
  private static final String PLUGINS_VERIFICATION_MODE_OPTION_NAME = "--plugins-verification-mode";

  @CommandLine.Option(
      names = {PLUGINS_EXTERNAL_ENABLED_OPTION_NAME},
      description = "Enables external plugins (default: ${DEFAULT-VALUE})",
      hidden = true)
  private boolean externalPluginsEnabled = DEFAULT_EXTERNAL_PLUGINS_ENABLED;

  @CommandLine.Option(
      names = {PLUGINS_OPTION_NAME},
      description = "Comma-separated list of plugin names to load",
      split = ",",
      converter = PluginInfoConverter.class)
  private List<PluginInfo> plugins = DEFAULT_REQUESTED_PLUGINS_INFO;

  @CommandLine.Option(
      names = {CONTINUE_ON_PLUGIN_ERROR_OPTION_NAME},
      description =
          "Allow Besu startup even if any plugins fail to initialize correctly (default: ${DEFAULT-VALUE})")
  private boolean continueOnPluginError = DEFAULT_CONTINUE_ON_PLUGIN_ERROR;

  @CommandLine.Option(
      names = {PLUGINS_VERIFICATION_MODE_OPTION_NAME},
      description = "How to verify plugins during startup phase (default: ${DEFAULT-VALUE})")
  private final PluginsVerificationMode pluginsVerificationMode = DEFAULT_PLUGINS_VERIFICATION_MODE;

  /** Default Constructor. */
  public PluginsConfigurationOptions() {}

  @Override
  public PluginConfiguration toDomainObject() {
    return ImmutablePluginConfiguration.builder()
        .externalPluginsEnabled(externalPluginsEnabled)
        .requestedPluginsInfo(plugins)
        .continueOnPluginError(continueOnPluginError)
        .pluginsVerificationMode(pluginsVerificationMode)
        .build();
  }

  /**
   * Validate that there are no inconsistencies in the specified options.
   *
   * @param commandLine the full commandLine to check all the options specified by the user
   */
  public void validate(final CommandLine commandLine) {
    String errorMessage =
        String.format(
            "%s, %s and %s options can only be used when %s is true",
            PLUGINS_OPTION_NAME,
            CONTINUE_ON_PLUGIN_ERROR_OPTION_NAME,
            PLUGINS_VERIFICATION_MODE_OPTION_NAME,
            PLUGINS_EXTERNAL_ENABLED_OPTION_NAME);
    CommandLineUtils.failIfOptionDoesntMeetRequirement(
        commandLine,
        errorMessage,
        externalPluginsEnabled,
        List.of(
            PLUGINS_OPTION_NAME,
            CONTINUE_ON_PLUGIN_ERROR_OPTION_NAME,
            PLUGINS_VERIFICATION_MODE_OPTION_NAME));
  }

  @Override
  public List<String> getCLIOptions() {
    return CommandLineUtils.getCLIOptions(this, new PluginsConfigurationOptions());
  }

  /**
   * Constructs a {@link PluginConfiguration} instance based on the command line options.
   *
   * @param commandLine The command line instance containing parsed options.
   * @return A new {@link PluginConfiguration} instance.
   */
  public static PluginConfiguration fromCommandLine(final CommandLine commandLine) {
    List<PluginInfo> plugins =
        CommandLineUtils.getOptionValueOrDefault(
            commandLine, PLUGINS_OPTION_NAME, new PluginInfoConverter());

    boolean externalPluginsEnabled =
        CommandLineUtils.getOptionValueOrDefault(
            commandLine, PLUGINS_EXTERNAL_ENABLED_OPTION_NAME, Boolean::parseBoolean);

    boolean continueOnPluginError =
        CommandLineUtils.getOptionValueOrDefault(
            commandLine, CONTINUE_ON_PLUGIN_ERROR_OPTION_NAME, Boolean::parseBoolean);

    PluginsVerificationMode pluginsVerificationMode =
        CommandLineUtils.getOptionValueOrDefault(
            commandLine,
            PLUGINS_VERIFICATION_MODE_OPTION_NAME,
            PluginsVerificationMode::valueOfIgnoreCase);

    return ImmutablePluginConfiguration.builder()
        .requestedPluginsInfo(plugins)
        .externalPluginsEnabled(externalPluginsEnabled)
        .continueOnPluginError(continueOnPluginError)
        .pluginsVerificationMode(pluginsVerificationMode)
        .build();
  }
}
