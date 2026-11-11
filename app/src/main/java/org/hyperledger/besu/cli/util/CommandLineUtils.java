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

import org.hyperledger.besu.cli.converter.TypeFormatter;
import org.hyperledger.besu.cli.options.OptionParser;
import org.hyperledger.besu.util.StringUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import picocli.CommandLine;

/** The Command line utils. */
public class CommandLineUtils {
  /** The constant DEPENDENCY_WARNING_MSG. */
  public static final String DEPENDENCY_WARNING_MSG =
      "{} has been ignored because {} was not defined on the command line.";

  /** The constant MULTI_DEPENDENCY_WARNING_MSG. */
  public static final String MULTI_DEPENDENCY_WARNING_MSG =
      "{} ignored because none of {} was defined.";

  /** The constant DEPRECATION_WARNING_MSG. */
  public static final String DEPRECATION_WARNING_MSG = "{} has been deprecated, use {} instead.";

  /** The constant DEPRECATED_AND_USELESS_WARNING_MSG. */
  public static final String DEPRECATED_AND_USELESS_WARNING_MSG =
      "{} has been deprecated and is now useless, remove it.";

  private CommandLineUtils() {}

  /**
   * Check if options are passed that require an option to be true to have any effect and log a
   * warning with the list of affected options.
   *
   * <p>Note that in future version of PicoCLI some options dependency mechanism may be implemented
   * that could replace this. See https://github.com/remkop/picocli/issues/295
   *
   * @param logger the logger instance used to log the warning
   * @param commandLine the command line containing the options we want to check
   * @param mainOptionName the name of the main option to test dependency against. Only used for
   *     display.
   * @param isMainOptionCondition the condition to test the options dependencies, if true will test
   *     if not won't
   * @param dependentOptionsNames a list of option names that can't be used if condition is met.
   *     Example: if --rpc-http-max-active-connections is in the list and condition is that
   *     --rpc-http-enabled should not be false, we log a warning.
   */
  public static void checkOptionDependencies(
      final Logger logger,
      final CommandLine commandLine,
      final String mainOptionName,
      final boolean isMainOptionCondition,
      final List<String> dependentOptionsNames) {
    if (isMainOptionCondition) {
      final String affectedOptions = getAffectedOptions(commandLine, dependentOptionsNames);

      if (!affectedOptions.isEmpty()) {
        logger.warn(DEPENDENCY_WARNING_MSG, affectedOptions, mainOptionName);
      }
    }
  }

  /**
   * Fail if option doesn't meet requirement.
   *
   * @param commandLine the command line
   * @param errorMessage the error message
   * @param requirement the requirement
   * @param dependentOptionsNames the dependent options names
   */
  public static void failIfOptionDoesntMeetRequirement(
      final CommandLine commandLine,
      final String errorMessage,
      final boolean requirement,
      final List<String> dependentOptionsNames) {
    if (!requirement) {
      final String affectedOptions = getAffectedOptions(commandLine, dependentOptionsNames);

      if (!affectedOptions.isEmpty()) {
        throw new CommandLine.ParameterException(
            commandLine, errorMessage + " [" + affectedOptions + "]");
      }
    }
  }

  /**
   * Return all the option names declared in a class. Note this will recursively check in any inner
   * option class if present.
   *
   * @param optClass the class to look for options
   * @return a list of option names found in the class
   */
  public static List<String> getCLIOptionNames(final Class<?> optClass) {
    final List<String> cliOpts = new ArrayList<>();
    final Field[] fields = optClass.getDeclaredFields();
    for (Field field : fields) {
      field.setAccessible(true);
      Annotation ann = field.getAnnotation(CommandLine.Option.class);
      if (ann != null) {
        final var optAnn = CommandLine.Option.class.cast(ann);
        cliOpts.add(optAnn.names()[0]);
      } else {
        ann = field.getAnnotation(CommandLine.ArgGroup.class);
        if (ann != null) {
          cliOpts.addAll(getCLIOptionNames(field.getType()));
        }
      }
    }
    return cliOpts;
  }

  /**
   * Converts the runtime options into their CLI representation. Options with a value equals to its
   * default are not included in the result since redundant. Note this will recursively check in any
   * inner option class if present.
   *
   * @param currOptions the actual runtime options
   * @param defaults the default option values
   * @return a list of CLI arguments
   */
  public static List<String> getCLIOptions(final Object currOptions, final Object defaults) {
    final List<String> cliOpts = new ArrayList<>();
    final Field[] fields = currOptions.getClass().getDeclaredFields();
    for (Field field : fields) {
      field.setAccessible(true);
      Annotation ann = field.getAnnotation(CommandLine.Option.class);
      if (ann != null) {
        try {
          var optVal = field.get(currOptions);
          if (!Objects.equals(optVal, field.get(defaults))) {
            var optAnn = CommandLine.Option.class.cast(ann);
            final var optConverter = optAnn.converter();
            cliOpts.add(optAnn.names()[0] + "=" + formatValue(optConverter, optVal));
          }
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      } else {
        ann = field.getAnnotation(CommandLine.ArgGroup.class);
        if (ann != null) {
          try {
            cliOpts.addAll(getCLIOptions(field.get(currOptions), field.get(defaults)));
          } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
    return cliOpts;
  }

  /**
   * There are different ways to format an option value back to its CLI form, the first is to use a
   * {@link TypeFormatter} if present, otherwise the formatting it is delegated to {@link
   * OptionParser#format(Object)}
   *
   * @param optConverter the list of converter types for the option
   * @param optVal the value of the options
   * @return a string with the CLI form of the value
   */
  @SuppressWarnings("unchecked")
  private static String formatValue(
      final Class<? extends CommandLine.ITypeConverter<?>>[] optConverter, final Object optVal) {
    return Arrays.stream(optConverter)
        .filter(c -> Arrays.stream(c.getInterfaces()).anyMatch(i -> i.equals(TypeFormatter.class)))
        .findFirst()
        .map(
            ctf -> {
              try {
                return (TypeFormatter) ctf.getDeclaredConstructor().newInstance();
              } catch (InstantiationException
                  | IllegalAccessException
                  | InvocationTargetException
                  | NoSuchMethodException e) {
                throw new RuntimeException(e);
              }
            })
        .map(tf -> tf.format(optVal))
        .orElseGet(() -> OptionParser.format(optVal));
  }

  private static String getAffectedOptions(
      final CommandLine commandLine, final List<String> dependentOptionsNames) {
    return commandLine.getCommandSpec().options().stream()
        .filter(option -> Arrays.stream(option.names()).anyMatch(dependentOptionsNames::contains))
        .filter(CommandLineUtils::isOptionSet)
        .map(option -> option.names()[0])
        .collect(
            Collectors.collectingAndThen(
                Collectors.toList(), StringUtils.joiningWithLastDelimiter(", ", " and ")));
  }

  private static boolean isOptionSet(final CommandLine.Model.OptionSpec option) {
    final CommandLine commandLine = option.command().commandLine();
    try {
      return !option.stringValues().isEmpty()
          || !Strings.isNullOrEmpty(commandLine.getDefaultValueProvider().defaultValue(option));
    } catch (final Exception e) {
      return false;
    }
  }

  /**
   * Is the option with that name set on the command line?
   *
   * @param commandLine the command line
   * @param optionName the option name to check
   * @return true if set
   */
  public static boolean isOptionSet(final CommandLine commandLine, final String optionName) {
    return commandLine.getCommandSpec().options().stream()
        .filter(optionSpec -> Arrays.stream(optionSpec.names()).anyMatch(optionName::equals))
        .anyMatch(CommandLineUtils::isOptionSet);
  }

  /**
   * Retrieves the value of a specified command line option, converting it to its appropriate type,
   * or returns the default value if the option was not specified.
   *
   * @param <T> The type of the option value.
   * @param commandLine The {@link CommandLine} instance containing the parsed command line options.
   * @param optionName The name of the option whose value is to be retrieved.
   * @param converter A converter that converts the option's string value to its appropriate type.
   * @return The value of the specified option converted to its type, or the default value if the
   *     option was not specified. Returns {@code null} if the option does not exist or if there is
   *     no default value and the option was not specified.
   */
  public static <T> T getOptionValueOrDefault(
      final CommandLine commandLine,
      final String optionName,
      final CommandLine.ITypeConverter<T> converter) {

    return commandLine
        .getParseResult()
        .matchedOptionValue(optionName, getDefaultOptionValue(commandLine, optionName, converter));
  }

  /**
   * Retrieves the default value for a specified command line option, converting it to its
   * appropriate type.
   *
   * @param <T> The type of the option value.
   * @param commandLine The {@link CommandLine} instance containing the parsed command line options.
   * @param optionName The name of the option whose default value is to be retrieved.
   * @param converter A converter that converts the option's default string value to its appropriate
   *     type.
   * @return The default value of the specified option converted to its type, or {@code null} if the
   *     option does not exist, does not have a default value, or if an error occurs during
   *     conversion.
   * @throws RuntimeException if there is an error converting the default value string to its type.
   */
  private static <T> T getDefaultOptionValue(
      final CommandLine commandLine,
      final String optionName,
      final CommandLine.ITypeConverter<T> converter) {

    CommandLine.Model.OptionSpec optionSpec = commandLine.getCommandSpec().findOption(optionName);
    if (optionSpec == null || commandLine.getDefaultValueProvider() == null) {
      return null;
    }

    try {
      String defaultValueString = commandLine.getDefaultValueProvider().defaultValue(optionSpec);
      return defaultValueString != null
          ? converter.convert(defaultValueString)
          : optionSpec.getValue();
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to convert default value for option " + optionName + ": " + e.getMessage(), e);
    }
  }
}
