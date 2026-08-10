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
package org.hyperledger.besu.testfuzz;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.hyperledger.besu.util.LogConfigurator;

import java.io.InputStream;
import java.io.PrintWriter;

import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * This is the root command for the `BesuFuzz` command line tool. It is a collection of fuzzers that
 * are guided by Besu's implementations.
 */
@Command(
    description = "Executes Besu based fuzz tests",
    abbreviateSynopsis = true,
    name = "evm",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class,
    sortOptions = false,
    header = "Usage:",
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription:%n%n",
    optionListHeading = "%nOptions:%n",
    footerHeading = "%n",
    footer = "Hyperledger Besu is licensed under the Apache License 2.0",
    subcommands = {P256VerifySubCommand.class})
@SuppressWarnings("java:S106")
public class BesuFuzzCommand implements Runnable {

  /** Default Constructor */
  BesuFuzzCommand() {
    // this method is here only for JavaDoc linting
  }

  void execute(final String... args) {
    execute(System.in, new PrintWriter(System.out, true, UTF_8), args);
  }

  void execute(final InputStream input, final PrintWriter output, final String[] args) {
    final CommandLine commandLine = new CommandLine(this).setOut(output);

    // don't require exact case to match enum values
    commandLine.setCaseInsensitiveEnumValuesAllowed(true);

    commandLine.setExecutionStrategy(new CommandLine.RunLast());
    commandLine.execute(args);
  }

  @Override
  public void run() {
    LogConfigurator.setLevel("", "OFF");
    System.out.println("No default command, please select a subcommand");
    System.exit(1);
  }
}
