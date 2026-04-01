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
package org.hyperledger.besu.cli.subcommands;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hyperledger.besu.cli.subcommands.PublicKeySubCommand.COMMAND_NAME;

import org.hyperledger.besu.cli.BesuCommand;
import org.hyperledger.besu.cli.DefaultCommandValues;
import org.hyperledger.besu.cli.options.NodePrivateKeyFileOption;
import org.hyperledger.besu.cli.subcommands.PublicKeySubCommand.AddressSubCommand;
import org.hyperledger.besu.cli.subcommands.PublicKeySubCommand.ExportSubCommand;
import org.hyperledger.besu.cli.util.VersionProvider;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.ethereum.core.Util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

/** Node's public key related sub-command */
@Command(
    name = COMMAND_NAME,
    description = "This command provides node public key related actions.",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class,
    subcommands = {ExportSubCommand.class, AddressSubCommand.class})
public class PublicKeySubCommand implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(PublicKeySubCommand.class);

  /** The constant COMMAND_NAME. */
  public static final String COMMAND_NAME = "public-key";

  @SuppressWarnings("unused")
  @ParentCommand
  private BesuCommand parentCommand; // Picocli injects reference to parent command

  @SuppressWarnings("unused")
  @Spec
  private CommandSpec spec; // Picocli injects reference to command spec

  private final PrintWriter out;

  /**
   * Instantiates a new Public key sub command.
   *
   * @param out the out
   */
  public PublicKeySubCommand(final PrintWriter out) {
    this.out = out;
  }

  @Override
  public void run() {
    spec.commandLine().usage(out);
  }

  /**
   * Public key export sub-command
   *
   * <p>Export of the public key is writing the key to the standard output by default. An option
   * enables to write it in a file. Indeed, a direct output of the value to standard out is not
   * always recommended as reading can be made difficult as the value can be mixed with other
   * information like logs that are in KeyPairUtil that is inevitable.
   */
  @Command(
      name = "export",
      description = "This command outputs the node public key. Default output is standard output.",
      mixinStandardHelpOptions = true,
      versionProvider = VersionProvider.class)
  static class ExportSubCommand extends KeyPairSubcommand implements Runnable {

    @Option(
        names = "--to",
        paramLabel = DefaultCommandValues.MANDATORY_FILE_FORMAT_HELP,
        description = "File to write public key to instead of standard output",
        arity = "1..1")
    private final File publicKeyExportFile = null;

    @Override
    public void run() {
      configureEcCurve(ecCurve, parentCommand.spec.commandLine());
      run(publicKeyExportFile, keyPair -> keyPair.getPublicKey().toString());
    }
  }

  /**
   * Account address export sub-command
   *
   * <p>Export of the account address is writing the address to the standard output by default. An
   * option enables to write it in a file. Indeed, a direct output of the value to standard out is
   * not always recommended as reading can be made difficult as the value can be mixed with other
   * information like logs that are in KeyPairUtil that is inevitable.
   */
  @Command(
      name = "export-address",
      description =
          "This command outputs the node's account address. "
              + "Default output is standard output.",
      mixinStandardHelpOptions = true,
      versionProvider = VersionProvider.class)
  static class AddressSubCommand extends KeyPairSubcommand implements Runnable {

    @Option(
        names = "--to",
        paramLabel = DefaultCommandValues.MANDATORY_FILE_FORMAT_HELP,
        description = "File to write address to instead of standard output",
        arity = "1..1")
    private final File addressExportFile = null;

    @Override
    public void run() {
      configureEcCurve(ecCurve, parentCommand.spec.commandLine());
      run(addressExportFile, keyPair -> Util.publicKeyToAddress(keyPair.getPublicKey()).toString());
    }
  }

  private static class KeyPairSubcommand {

    /** The Parent command. */
    @SuppressWarnings("unused")
    @ParentCommand
    protected PublicKeySubCommand parentCommand; // Picocli injects reference to parent command

    @Mixin private final NodePrivateKeyFileOption nodePrivateKeyFileOption = null;

    /** The Ec curve. */
    @Option(
        names = "--ec-curve",
        paramLabel = "<NAME>",
        description =
            "Elliptic curve to use when creating a new key (default: "
                + SignatureAlgorithmFactory.DEFAULT_EC_CURVE_NAME
                + ")",
        arity = "0..1")
    @SuppressWarnings("FieldCanBeFinal")
    protected String ecCurve = null;

    @Spec private final CommandSpec spec = null;

    /**
     * Run.
     *
     * @param exportFile the export file
     * @param outputFunction the output function
     */
    protected final void run(
        final File exportFile, final Function<KeyPair, String> outputFunction) {
      checkNotNull(parentCommand);
      final BesuCommand besuCommand = parentCommand.parentCommand;
      checkNotNull(besuCommand);

      final File nodePrivateKeyFile = nodePrivateKeyFileOption.getNodePrivateKeyFile();
      if (nodePrivateKeyFile != null && !nodePrivateKeyFile.exists()) {
        throw new CommandLine.ParameterException(
            spec.commandLine(), "Private key file doesn't exist");
      }

      final KeyPair keyPair;
      try {
        keyPair = besuCommand.loadKeyPair(nodePrivateKeyFileOption.getNodePrivateKeyFile());
      } catch (IllegalArgumentException e) {
        throw new CommandLine.ParameterException(
            spec.commandLine(), "Private key cannot be loaded from file", e);
      }
      final String output = outputFunction.apply(keyPair);
      if (exportFile != null) {
        final Path path = exportFile.toPath();

        try (final BufferedWriter fileWriter = Files.newBufferedWriter(path, UTF_8)) {
          fileWriter.write(output);
        } catch (final IOException e) {
          LOG.error("An error occurred while trying to write to output file", e);
        }
      } else {
        parentCommand.out.println(output);
      }
    }

    /**
     * Configure ec curve.
     *
     * @param ecCurve the ec curve
     * @param commandLine the command line
     */
    protected static void configureEcCurve(final String ecCurve, final CommandLine commandLine) {
      if (ecCurve != null) {
        try {
          SignatureAlgorithmFactory.switchInstance(ecCurve);
        } catch (IllegalArgumentException e) {
          throw new CommandLine.ParameterException(commandLine, e.getMessage(), e);
        }
      }
    }
  }
}
