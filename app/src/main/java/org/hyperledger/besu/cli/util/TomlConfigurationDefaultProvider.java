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

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.util.number.Fraction;
import org.hyperledger.besu.util.number.Percentage;
import org.hyperledger.besu.util.number.PositiveNumber;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.tuweni.toml.Toml;
import org.apache.tuweni.toml.TomlArray;
import org.apache.tuweni.toml.TomlParseError;
import org.apache.tuweni.toml.TomlParseResult;
import picocli.CommandLine;
import picocli.CommandLine.IDefaultValueProvider;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Model.OptionSpec;
import picocli.CommandLine.ParameterException;

/** The Toml config file default value provider used by PicoCli. */
public class TomlConfigurationDefaultProvider implements IDefaultValueProvider {

  private final CommandLine commandLine;
  private final File configFile;
  private final InputStream configurationInputStream;
  private TomlParseResult result;
  private boolean isUnknownOptionsChecked;

  /**
   * Instantiates a new Toml config file default value provider.
   *
   * @param commandLine the command line
   * @param configFile the configuration file
   * @param configurationInputStream the input stream
   */
  private TomlConfigurationDefaultProvider(
      final CommandLine commandLine,
      final File configFile,
      final InputStream configurationInputStream) {
    this.commandLine = commandLine;
    this.configFile = configFile;
    this.configurationInputStream = configurationInputStream;
  }

  /**
   * Creates a new TomlConfigurationDefaultProvider from a file.
   *
   * @param commandLine the command line
   * @param configFile the configuration file
   * @return a new TomlConfigurationDefaultProvider
   * @throws ParameterException if the configuration file is not found
   */
  public static TomlConfigurationDefaultProvider fromFile(
      final CommandLine commandLine, final File configFile) {
    if (!configFile.exists()) {
      throw new ParameterException(
          commandLine, "Unable to read TOML configuration, file not found.");
    }
    return new TomlConfigurationDefaultProvider(commandLine, configFile, null);
  }

  /**
   * Creates a new TomlConfigurationDefaultProvider from an input stream.
   *
   * @param commandLine the command line
   * @param inputStream the input stream
   * @return a new TomlConfigurationDefaultProvider
   */
  public static TomlConfigurationDefaultProvider fromInputStream(
      final CommandLine commandLine, final InputStream inputStream) {
    return new TomlConfigurationDefaultProvider(commandLine, null, inputStream);
  }

  @Override
  public String defaultValue(final ArgSpec argSpec) {
    loadConfigurationIfNotLoaded();

    // only options can be used in config because a name is needed for the key
    // so we skip default for positional params
    return argSpec.isOption() ? getConfigurationValue(((OptionSpec) argSpec)) : null;
  }

  private String getConfigurationValue(final OptionSpec optionSpec) {
    // NOTE: This temporary fix is necessary to make certain options be treated as a multi-value.
    // This can be done automatically by picocli if the object implements Collection.
    final boolean isArray = getKeyName(optionSpec).map(result::isArray).orElse(false);

    if (optionSpec.type().equals(Boolean.class) || optionSpec.type().equals(boolean.class)) {
      return getBooleanEntryAsString(optionSpec);
    } else if (optionSpec.isMultiValue() || isArray) {
      return getListEntryAsString(optionSpec);
    } else if (isNumericType(optionSpec.type())) {
      return getNumericEntryAsString(optionSpec);
    } else { // else will be treated as String
      return getEntryAsString(optionSpec);
    }
  }

  private boolean isNumericType(final Class<?> type) {
    return type.equals(Byte.class)
        || type.equals(byte.class)
        || type.equals(Short.class)
        || type.equals(short.class)
        || type.equals(Integer.class)
        || type.equals(int.class)
        || type.equals(Long.class)
        || type.equals(long.class)
        || type.equals(Wei.class)
        || type.equals(BigInteger.class)
        || type.equals(Double.class)
        || type.equals(double.class)
        || type.equals(Float.class)
        || type.equals(float.class)
        || type.equals(Percentage.class)
        || type.equals(Fraction.class)
        || type.equals(PositiveNumber.class)
        || type.equals(Duration.class);
  }

  private String getEntryAsString(final OptionSpec spec) {
    // returns the string value of the config line corresponding to the option in toml file
    // or null if not present in the config
    return getKeyName(spec).map(result::getString).orElse(null);
  }

  private Optional<String> getKeyName(final OptionSpec spec) {
    // If any of the names of the option are used as key in the toml results
    // then returns the value of first one.
    Optional<String> keyName =
        Arrays.stream(spec.names())
            // remove leading dashes on option name as we can have "--" or "-" options
            .map(name -> name.replaceFirst("^-+", ""))
            .filter(result::contains)
            .findFirst();

    if (keyName.isEmpty()) {
      // If the base key name doesn't exist in the file it may be under a TOML table heading
      // e.g. TxPool.tx-pool-max-size
      keyName = getDottedKeyName(spec);
    }

    return keyName;
  }

  /*
   For all spec names, look to see if any of the TOML keyPathSet entries contain
   the name. A key path set might look like ["TxPool", "tx-max-pool-size"] where
   "TxPool" is the TOML table heading (which we ignore) and "tx-max-pool-size" is
   the name of the option being requested. For a request for "tx-max-pool-size" this
   function will return "TxPool.tx-max-pool-size" which can then be used directly
   as a query on the TOML result structure.
  */
  private Optional<String> getDottedKeyName(final OptionSpec spec) {
    List<String> foundNames = new ArrayList<>();

    Arrays.stream(spec.names())
        .forEach(
            nextSpecName -> {
              String specName =
                  result.keyPathSet().stream()
                      .filter(option -> option.contains(nextSpecName.replaceFirst("^-+", "")))
                      .findFirst()
                      .orElse(new ArrayList<>())
                      .stream()
                      .collect(Collectors.joining("."));
              if (specName.length() > 0) {
                foundNames.add(specName);
              }
            });

    return foundNames.stream().findFirst();
  }

  private String getListEntryAsString(final OptionSpec spec) {
    // returns the string representation of the array value of the config line in CLI format
    // corresponding to the option in toml file
    // or null if not present in the config
    return decodeTomlArray(
        getKeyName(spec).map(result::getArray).map(tomlArray -> tomlArray.toList()).orElse(null));
  }

  private String decodeTomlArray(final List<Object> tomlArrayElements) {
    if (tomlArrayElements == null) return null;
    return tomlArrayElements.stream()
        .map(
            tomlObject -> {
              if (tomlObject instanceof TomlArray) {
                return "[".concat(decodeTomlArray(((TomlArray) tomlObject).toList())).concat("]");
              } else {
                return tomlObject.toString();
              }
            })
        .collect(Collectors.joining(","));
  }

  private String getBooleanEntryAsString(final OptionSpec spec) {
    // return the string representation of the boolean value corresponding to the option in toml
    // file
    // or null if not present in the config
    return getKeyName(spec).map(result::getBoolean).map(Object::toString).orElse(null);
  }

  private String getNumericEntryAsString(final OptionSpec spec) {
    // return the string representation of the numeric value corresponding to the option in toml
    // file - this works for integer, double, and float
    // or null if not present in the config

    return getKeyName(spec).map(result::get).map(Object::toString).orElse(null);
  }

  private void checkConfigurationValidity() {
    if (result == null || result.isEmpty()) {
      throw new ParameterException(
          commandLine, "Unable to read from empty TOML configuration file.");
    }

    if (!isUnknownOptionsChecked && !commandLine.isUnmatchedArgumentsAllowed()) {
      checkUnknownOptions(result);
      isUnknownOptionsChecked = true;
    }
  }

  /** Load configuration from file. */
  public void loadConfigurationIfNotLoaded() {
    if (result == null) {
      if (configFile != null) {
        try (InputStream is = new FileInputStream(configFile)) {
          loadToml(is);
        } catch (final FileNotFoundException e) {
          throw new ParameterException(
              commandLine, "Unable to read TOML configuration, file not found.", e);
        } catch (final IOException e) {
          throw new ParameterException(commandLine, "Unable to read TOML configuration.", e);
        }
      } else {
        try {
          loadToml(configurationInputStream);
        } catch (final IOException e) {
          throw new ParameterException(commandLine, "Unable to read TOML configuration.", e);
        }
      }
    }
    checkConfigurationValidity();
  }

  private void loadToml(final InputStream is) throws IOException {
    final TomlParseResult result = Toml.parse(is);

    if (result.hasErrors()) {
      final String errors =
          result.errors().stream().map(TomlParseError::toString).collect(Collectors.joining("%n"));

      throw new ParameterException(
          commandLine, String.format("Invalid TOML configuration: %s", errors));
    }

    this.result = result;
  }

  private void checkUnknownOptions(final TomlParseResult result) {
    final CommandSpec commandSpec = commandLine.getCommandSpec();

    // Besu ignores TOML table headings (e.g. [TxPool]) so we use keyPathSet() and take the
    // last element in each one. For a TOML parameter that's not defined inside a table, the lists
    // returned in keyPathSet() will contain a single entry - the config parameter itself. For a
    // TOML
    // entry that is in a table the list will contain N entries, the last one being the config
    // parameter itself.
    final Set<String> optionsWithoutTables = new HashSet<String>();
    result.keyPathSet().stream()
        .forEach(
            strings -> {
              optionsWithoutTables.add(strings.get(strings.size() - 1));
            });

    // Once we've stripped TOML table headings from the lists, we can check that the remaining
    // options are valid
    final Set<String> unknownOptionsList =
        optionsWithoutTables.stream()
            .filter(option -> !commandSpec.optionsMap().containsKey("--" + option))
            .collect(Collectors.toSet());

    if (!unknownOptionsList.isEmpty()) {
      final String csvUnknownOptions = String.join(", ", unknownOptionsList);
      throw new ParameterException(
          commandLine,
          String.format(
              "Unknown option%s in TOML configuration file: %s",
              unknownOptionsList.size() > 1 ? "s" : "", csvUnknownOptions));
    }
  }
}
