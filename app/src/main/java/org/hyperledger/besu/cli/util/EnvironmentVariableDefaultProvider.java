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
package org.hyperledger.besu.cli.util;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import picocli.CommandLine.IDefaultValueProvider;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.OptionSpec;

/** The Environment variable default provider used in PicoCli. */
public class EnvironmentVariableDefaultProvider implements IDefaultValueProvider {
  private static final String ENV_VAR_PREFIX = "BESU_";

  private final Map<String, String> environment;

  /**
   * Instantiates a new Environment variable default provider.
   *
   * @param environment the environment
   */
  public EnvironmentVariableDefaultProvider(final Map<String, String> environment) {
    this.environment = environment;
  }

  @Override
  public String defaultValue(final ArgSpec argSpec) {
    if (argSpec.isPositional()) {
      return null; // skip default for positional params
    }

    return envVarNames((OptionSpec) argSpec)
        .map(environment::get)
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(null);
  }

  private Stream<String> envVarNames(final OptionSpec spec) {
    return Arrays.stream(spec.names())
        .filter(name -> name.startsWith("--")) // Only long options are allowed
        .flatMap(
            name -> Stream.of(ENV_VAR_PREFIX).map(prefix -> prefix + nameToEnvVarSuffix(name)));
  }

  private String nameToEnvVarSuffix(final String name) {
    return name.substring("--".length()).replace('-', '_').toUpperCase(Locale.US);
  }
}
