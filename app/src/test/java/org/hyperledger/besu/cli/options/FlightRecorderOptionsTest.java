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
package org.hyperledger.besu.cli.options;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.cli.util.EnvironmentVariableDefaultProvider;

import java.util.Map;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

public class FlightRecorderOptionsTest {

  @Test
  void offByDefaultAndHiddenFromHelp() {
    final CommandLine commandLine = commandLine(Map.of());

    commandLine.parseArgs();

    assertThat(commandLine.<FlightRecorderOptions>getCommand().isEnabled()).isFalse();
    assertThat(commandLine.getUsageMessage()).doesNotContain("Xflight-recorder");
  }

  @Test
  void explicitFlagAndEnvironmentVariableOptIn() {
    final CommandLine enabled = commandLine(Map.of());
    enabled.parseArgs("--Xflight-recorder=true");
    assertThat(enabled.<FlightRecorderOptions>getCommand().isEnabled()).isTrue();

    final CommandLine fromEnvironment = commandLine(Map.of("BESU_XFLIGHT_RECORDER", "true"));
    fromEnvironment.parseArgs();
    assertThat(fromEnvironment.<FlightRecorderOptions>getCommand().isEnabled()).isTrue();

    final CommandLine explicitWins = commandLine(Map.of("BESU_XFLIGHT_RECORDER", "true"));
    explicitWins.parseArgs("--Xflight-recorder=false");
    assertThat(explicitWins.<FlightRecorderOptions>getCommand().isEnabled()).isFalse();
  }

  private static CommandLine commandLine(final Map<String, String> environment) {
    return new CommandLine(new FlightRecorderOptions())
        .setToggleBooleanFlags(false)
        .setDefaultValueProvider(new EnvironmentVariableDefaultProvider(environment));
  }
}
