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
package org.hyperledger.besu.services;

import org.hyperledger.besu.plugin.services.PicoCLIOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Model.OptionSpec;

/** The Pico cli options service implementation to specify plugins. */
public class PicoCLIOptionsImpl implements PicoCLIOptions {

  private static final Logger LOG = LoggerFactory.getLogger(PicoCLIOptionsImpl.class);

  private final CommandLine commandLine;
  private boolean optionsDefinitionCompleted = false;

  /**
   * Instantiates a new Pico cli options.
   *
   * @param commandLine the command line
   */
  public PicoCLIOptionsImpl(final CommandLine commandLine) {
    this.commandLine = commandLine;
  }

  /**
   * Marks the end of the option-definition phase. The command line is parsed right after this call,
   * so any later attempt to add options throws {@link OptionsAlreadyParsedException}.
   */
  public void optionsDefinitionCompleted() {
    optionsDefinitionCompleted = true;
  }

  @Override
  public void addPicoCLIOptions(final String namespace, final Object optionObject) {
    if (optionsDefinitionCompleted) {
      throw new OptionsAlreadyParsedException(namespace);
    }
    final String pluginPrefix = "--plugin-" + namespace + "-";
    final String unstablePrefix = "--Xplugin-" + namespace + "-";
    final CommandSpec mixin = CommandSpec.forAnnotatedObject(optionObject);
    boolean badOptionName = false;

    for (final OptionSpec optionSpec : mixin.options()) {
      for (final String optionName : optionSpec.names()) {
        if (!optionName.startsWith(pluginPrefix) && !optionName.startsWith(unstablePrefix)) {
          badOptionName = true;
          LOG.error(
              "Plugin option {} did not have the expected prefix of {}", optionName, pluginPrefix);
        }
      }
    }
    if (badOptionName) {
      throw new RuntimeException("Error loading CLI options");
    } else {
      commandLine.getCommandSpec().addMixin("Plugin " + namespace, mixin);
    }
  }

  /**
   * Thrown when a plugin adds CLI options after the command line has been parsed, typically from
   * {@code register()} instead of {@code defineOptions()}.
   */
  public static class OptionsAlreadyParsedException extends IllegalStateException {
    /**
     * Creates the exception for the given option namespace.
     *
     * @param namespace the namespace the options were added under
     */
    public OptionsAlreadyParsedException(final String namespace) {
      super(
          "CLI options for namespace '"
              + namespace
              + "' were added after the command line was parsed; plugins must declare their"
              + " options in defineOptions(), not in register()");
    }
  }
}
