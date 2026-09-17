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
package org.hyperledger.besu.cli;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.contentOf;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.util.BesuVersionUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.tuweni.bytes.Bytes32;
import org.bouncycastle.asn1.sec.SECNamedCurves;
import org.bouncycastle.asn1.x9.X9ECParameters;
import org.bouncycastle.crypto.params.ECDomainParameters;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import picocli.CommandLine.Model.CommandSpec;

@ExtendWith(MockitoExtension.class)
public class PublicKeySubCommandTest extends CommandTestAbstract {

  private static final String EXPECTED_PUBLIC_KEY_USAGE =
      "Usage: besu public-key [-hV] [COMMAND]"
          + System.lineSeparator()
          + "This command provides node public key related actions."
          + System.lineSeparator()
          + "  -h, --help      Show this help message and exit."
          + System.lineSeparator()
          + "  -V, --version   Print version information and exit."
          + System.lineSeparator()
          + "Commands:"
          + System.lineSeparator()
          + "  export          This command outputs the node public key. Default output is"
          + System.lineSeparator()
          + "                    standard output."
          + System.lineSeparator()
          + "  export-address  This command outputs the node's account address. Default"
          + System.lineSeparator()
          + "                    output is standard output."
          + System.lineSeparator();

  private static final String EXPECTED_PUBLIC_KEY_EXPORT_USAGE =
      "Usage: besu public-key export [-hV] [--ec-curve[=<NAME>]]"
          + System.lineSeparator()
          + "                              [--node-private-key-file=<PATH>] [--to=<FILE>]"
          + System.lineSeparator()
          + "This command outputs the node public key. Default output is standard output."
          + System.lineSeparator()
          + "      --ec-curve[=<NAME>]   Elliptic curve to use when creating a new key"
          + System.lineSeparator()
          + "                              (default: secp256k1)"
          + System.lineSeparator()
          + "  -h, --help                Show this help message and exit."
          + System.lineSeparator()
          + "      --node-private-key-file=<PATH>"
          + System.lineSeparator()
          + "                            The node's private key file (default: a file named"
          + System.lineSeparator()
          + "                              \"key\" in the Besu data directory)"
          + System.lineSeparator()
          + "      --to=<FILE>           File to write public key to instead of standard"
          + System.lineSeparator()
          + "                              output"
          + System.lineSeparator()
          + "  -V, --version             Print version information and exit."
          + System.lineSeparator();

  private static final String EXPECTED_PUBLIC_KEY_EXPORT_ADDRESS_USAGE =
      "Usage: besu public-key export-address [-hV] [--ec-curve[=<NAME>]]"
          + System.lineSeparator()
          + "                                      [--node-private-key-file=<PATH>]"
          + System.lineSeparator()
          + "                                      [--to=<FILE>]"
          + System.lineSeparator()
          + "This command outputs the node's account address. Default output is standard"
          + System.lineSeparator()
          + "output."
          + System.lineSeparator()
          + "      --ec-curve[=<NAME>]   Elliptic curve to use when creating a new key"
          + System.lineSeparator()
          + "                              (default: secp256k1)"
          + System.lineSeparator()
          + "  -h, --help                Show this help message and exit."
          + System.lineSeparator()
          + "      --node-private-key-file=<PATH>"
          + System.lineSeparator()
          + "                            The node's private key file (default: a file named"
          + System.lineSeparator()
          + "                              \"key\" in the Besu data directory)"
          + System.lineSeparator()
          + "      --to=<FILE>           File to write address to instead of standard output"
          + System.lineSeparator()
          + "  -V, --version             Print version information and exit."
          + System.lineSeparator();

  private static final String PUBLIC_KEY_SUBCOMMAND_NAME = "public-key";
  private static final String PUBLIC_KEY_EXPORT_SUBCOMMAND_NAME = "export";
  private static final String PUBLIC_KEY_EXPORT_ADDRESS_SUBCOMMAND_NAME = "export-address";
  private static final String CURVE_NAME = "secp256k1";
  private static final String ALGORITHM = SignatureAlgorithm.ALGORITHM;
  private static ECDomainParameters curve;

  @BeforeAll
  public static void setUp() {
    final X9ECParameters params = SECNamedCurves.getByName(CURVE_NAME);
    curve = new ECDomainParameters(params.getCurve(), params.getG(), params.getN(), params.getH());
  }

  @BeforeEach
  public void before() {
    SignatureAlgorithmFactory.resetInstance();
  }

  // public-key sub-command
  @Test
  public void publicKeySubCommandExistsAndHasSubCommands() {
    CommandSpec spec = parseCommand().getSpec();
    assertThat(spec.subcommands()).containsKeys(PUBLIC_KEY_SUBCOMMAND_NAME);
    assertThat(spec.subcommands().get(PUBLIC_KEY_SUBCOMMAND_NAME).getSubcommands())
        .containsKeys(PUBLIC_KEY_EXPORT_SUBCOMMAND_NAME)
        .containsKeys(PUBLIC_KEY_EXPORT_ADDRESS_SUBCOMMAND_NAME);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void callingPublicKeySubCommandWithoutSubSubcommandMustDisplayUsage() {
    parseCommand(PUBLIC_KEY_SUBCOMMAND_NAME);
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(commandOutput.toString(UTF_8)).startsWith(EXPECTED_PUBLIC_KEY_USAGE);
  }

  @Test
  public void callingPublicKeySubCommandHelpMustDisplayUsage() {
    parseCommand(PUBLIC_KEY_SUBCOMMAND_NAME, "--help");
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(commandOutput.toString(UTF_8)).startsWith(EXPECTED_PUBLIC_KEY_USAGE);
  }

  @Test
  public void callingPublicKeySubCommandVersionMustDisplayVersion() {
    parseCommand(PUBLIC_KEY_SUBCOMMAND_NAME, "--version");
    assertThat(commandOutput.toString(UTF_8))
        .isEqualToIgnoringWhitespace(BesuVersionUtils.version());
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  // Export public key sub-sub-command
  @Test
  public void callingPublicKeyExportSubCommandHelpMustDisplayUsage() {
    parseCommand(PUBLIC_KEY_SUBCOMMAND_NAME, PUBLIC_KEY_EXPORT_SUBCOMMAND_NAME, "--help");
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(commandOutput.toString(UTF_8)).startsWith(EXPECTED_PUBLIC_KEY_EXPORT_USAGE);
  }

  @Test
  public void callingPublicKeyExportSubCommandVersionMustDisplayVersion() {
    parseCommand(PUBLIC_KEY_SUBCOMMAND_NAME, PUBLIC_KEY_EXPORT_SUBCOMMAND_NAME, "--version");
    assertThat(commandOutput.toString(UTF_8))
        .isEqualToIgnoringWhitespace(BesuVersionUtils.version());
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void callingPublicKeyExportSubCommandWithoutPathMustWriteKeyToStandardOutput() {
    final NodeKey nodeKey = getNodeKey();

    parseCommand(PUBLIC_KEY_SUBCOMMAND_NAME, PUBLIC_KEY_EXPORT_SUBCOMMAND_NAME);

    final String expectedOutputStart = nodeKey.getPublicKey().toString();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(commandOutput.toString(UTF_8)).startsWith(expectedOutputStart);
  }

  @Test
  public void callingPublicKeyExportSubCommandWithFilePathMustWritePublicKeyInThisFile()
      throws Exception {

    final NodeKey nodeKey = getNodeKey();

    final File file = File.createTempFile("public", "key");

    parseCommand(
        PUBLIC_KEY_SUBCOMMAND_NAME, PUBLIC_KEY_EXPORT_SUBCOMMAND_NAME, "--to", file.getPath());

    assertThat(contentOf(file))
        .startsWith(nodeKey.getPublicKey().toString())
        .endsWith(nodeKey.getPublicKey().toString());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void callingPublicKeyExportSubCommandWithPrivateKeyFileMustWriteKeyToStandardOutput()
      throws IOException {
    final SECPPrivateKey privateKey =
        SECPPrivateKey.create(
            Bytes32.fromHexString(
                "0x8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63"),
            ALGORITHM);
    final KeyPair keyPair = KeyPair.create(privateKey, curve, ALGORITHM);

    final Path privateKeyFile = Files.createTempFile("private", "address");
    Files.writeString(privateKeyFile, privateKey.toString());

    parseCommand(
        PUBLIC_KEY_SUBCOMMAND_NAME,
        PUBLIC_KEY_EXPORT_SUBCOMMAND_NAME,
        "--node-private-key-file",
        privateKeyFile.toString());

    final String expectedOutputStart = keyPair.getPublicKey().toString();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(commandOutput.toString(UTF_8)).startsWith(expectedOutputStart);
  }

  @Test
  public void callingPublicKeyExportSubCommandWithNonExistentMustDisplayError() {
    parseCommand(
        PUBLIC_KEY_SUBCOMMAND_NAME,
        PUBLIC_KEY_EXPORT_SUBCOMMAND_NAME,
        "--node-private-key-file",
        "/non/existent/file");
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).startsWith("Private key file doesn't exist");
  }

  @Test
  public void callingPublicKeyExportSubCommandWithInvalidFileMustDisplayError() throws IOException {
    final Path privateKeyFile = Files.createTempFile("private", "address");
    Files.writeString(privateKeyFile, "invalid private key");

    parseCommand(
        PUBLIC_KEY_SUBCOMMAND_NAME,
        PUBLIC_KEY_EXPORT_SUBCOMMAND_NAME,
        "--node-private-key-file",
        privateKeyFile.toString());
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .startsWith("Private key cannot be loaded from file");
  }

  // Export address sub-sub-command
  @Test
  public void callingPublicKeyExportAddressSubCommandHelpMustDisplayUsage() {
    parseCommand(PUBLIC_KEY_SUBCOMMAND_NAME, PUBLIC_KEY_EXPORT_ADDRESS_SUBCOMMAND_NAME, "--help");
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(commandOutput.toString(UTF_8)).startsWith(EXPECTED_PUBLIC_KEY_EXPORT_ADDRESS_USAGE);
  }

  @Test
  public void callingPublicKeyExportAddressSubCommandVersionMustDisplayVersion() {
    parseCommand(
        PUBLIC_KEY_SUBCOMMAND_NAME, PUBLIC_KEY_EXPORT_ADDRESS_SUBCOMMAND_NAME, "--version");
    assertThat(commandOutput.toString(UTF_8))
        .isEqualToIgnoringWhitespace(BesuVersionUtils.version());
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void callingPublicKeyExportAddressSubCommandWithoutPathMustWriteAddressToStandardOutput() {
    final NodeKey nodeKey = getNodeKey();

    parseCommand(PUBLIC_KEY_SUBCOMMAND_NAME, PUBLIC_KEY_EXPORT_ADDRESS_SUBCOMMAND_NAME);

    final String expectedOutputStart = Util.publicKeyToAddress(nodeKey.getPublicKey()).toString();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(commandOutput.toString(UTF_8)).startsWith(expectedOutputStart);
  }

  @Test
  public void callingPublicKeyExportAddressSubCommandWithFilePathMustWriteAddressInThisFile()
      throws Exception {

    final NodeKey nodeKey = getNodeKey();

    final File file = File.createTempFile("public", "address");

    parseCommand(
        PUBLIC_KEY_SUBCOMMAND_NAME,
        PUBLIC_KEY_EXPORT_ADDRESS_SUBCOMMAND_NAME,
        "--to",
        file.getPath());

    assertThat(contentOf(file))
        .startsWith(Util.publicKeyToAddress(nodeKey.getPublicKey()).toString())
        .endsWith(Util.publicKeyToAddress(nodeKey.getPublicKey()).toString());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void
      callingPublicKeyExportAddressSubCommandWithPrivateKeyFileMustWriteKeyToStandardOutput()
          throws IOException {
    final SECPPrivateKey privateKey =
        SECPPrivateKey.create(
            Bytes32.fromHexString(
                "0x8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63"),
            ALGORITHM);
    final KeyPair keyPair = KeyPair.create(privateKey, curve, ALGORITHM);

    final Path privateKeyFile = Files.createTempFile("private", "address");
    Files.writeString(privateKeyFile, privateKey.toString());

    parseCommand(
        PUBLIC_KEY_SUBCOMMAND_NAME,
        PUBLIC_KEY_EXPORT_ADDRESS_SUBCOMMAND_NAME,
        "--node-private-key-file",
        privateKeyFile.toString());

    final String expectedOutputStart = Util.publicKeyToAddress(keyPair.getPublicKey()).toString();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(commandOutput.toString(UTF_8)).startsWith(expectedOutputStart);
  }

  @Test
  public void callingPublicKeyExportAddressSubCommandWithNonExistentMustDisplayError() {
    parseCommand(
        PUBLIC_KEY_SUBCOMMAND_NAME,
        PUBLIC_KEY_EXPORT_ADDRESS_SUBCOMMAND_NAME,
        "--node-private-key-file",
        "/non/existent/file");
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).startsWith("Private key file doesn't exist");
  }

  @Test
  public void callingPublicKeyExportAddressSubCommandWithInvalidFileMustDisplayError()
      throws IOException {
    final Path privateKeyFile = Files.createTempFile("private", "address");
    Files.writeString(privateKeyFile, "invalid private key");

    parseCommand(
        PUBLIC_KEY_SUBCOMMAND_NAME,
        PUBLIC_KEY_EXPORT_ADDRESS_SUBCOMMAND_NAME,
        "--node-private-key-file",
        privateKeyFile.toString());
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .startsWith("Private key cannot be loaded from file");
  }

  @Test
  public void
      callingPublicKeyExportSubCommandWithEcCurveNameCorrectlyConfiguresSignatureAlgorithmFactory()
          throws Exception {
    final File file = File.createTempFile("public", "key");

    parseCommand(
        PUBLIC_KEY_SUBCOMMAND_NAME,
        PUBLIC_KEY_EXPORT_SUBCOMMAND_NAME,
        "--to",
        file.getPath(),
        "--ec-curve",
        CURVE_NAME);

    assertThat(SignatureAlgorithmFactory.getInstance().getCurveName()).isEqualTo(CURVE_NAME);
  }

  @Test
  public void
      callingPublicKeyExportSubCommandWithoutEcCurveNameDoesNotConfiguresSignatureAlgorithmFactory()
          throws Exception {
    final File file = File.createTempFile("public", "key");

    parseCommand(
        PUBLIC_KEY_SUBCOMMAND_NAME, PUBLIC_KEY_EXPORT_SUBCOMMAND_NAME, "--to", file.getPath());

    assertThat(SignatureAlgorithmFactory.getInstance().getCurveName()).isEqualTo("secp256k1");
  }

  @Test
  public void callingPublicKeyExportSubCommandWithInvalidEcCurveNameFails() throws Exception {
    final File file = File.createTempFile("public", "key");

    parseCommand(
        PUBLIC_KEY_SUBCOMMAND_NAME,
        PUBLIC_KEY_EXPORT_SUBCOMMAND_NAME,
        "--to",
        file.getPath(),
        "--ec-curve",
        "foo");

    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("foo is not in the list of valid elliptic curves");
  }

  @Test
  public void
      callingPublicKeyExportAddressSubCommandWithEcCurveNameCorrectlyConfiguresSignatureAlgorithmFactory()
          throws Exception {
    final SECPPrivateKey privateKey =
        SECPPrivateKey.create(
            Bytes32.fromHexString(
                "0x8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63"),
            ALGORITHM);

    final Path privateKeyFile = Files.createTempFile("private", "address");
    Files.writeString(privateKeyFile, privateKey.toString());

    parseCommand(
        PUBLIC_KEY_SUBCOMMAND_NAME,
        PUBLIC_KEY_EXPORT_ADDRESS_SUBCOMMAND_NAME,
        "--node-private-key-file",
        privateKeyFile.toString(),
        "--ec-curve",
        CURVE_NAME);

    assertThat(SignatureAlgorithmFactory.getInstance().getCurveName()).isEqualTo(CURVE_NAME);
  }

  @Test
  public void
      callingPublicKeyExportAddressSubCommandWithoutEcCurveNameDoesNotConfiguresSignatureAlgorithmFactory()
          throws Exception {
    final SECPPrivateKey privateKey =
        SECPPrivateKey.create(
            Bytes32.fromHexString(
                "0x8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63"),
            ALGORITHM);

    final Path privateKeyFile = Files.createTempFile("private", "address");
    Files.writeString(privateKeyFile, privateKey.toString());

    parseCommand(
        PUBLIC_KEY_SUBCOMMAND_NAME,
        PUBLIC_KEY_EXPORT_ADDRESS_SUBCOMMAND_NAME,
        "--node-private-key-file",
        privateKeyFile.toString());

    assertThat(SignatureAlgorithmFactory.getInstance().getCurveName()).isEqualTo("secp256k1");
  }

  @Test
  public void callingPublicKeyExportAddressSubCommandWithInvalidEcCurveNameFails()
      throws Exception {
    final SECPPrivateKey privateKey =
        SECPPrivateKey.create(
            Bytes32.fromHexString(
                "0x8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63"),
            ALGORITHM);

    final Path privateKeyFile = Files.createTempFile("private", "address");
    Files.writeString(privateKeyFile, privateKey.toString());

    parseCommand(
        PUBLIC_KEY_SUBCOMMAND_NAME,
        PUBLIC_KEY_EXPORT_ADDRESS_SUBCOMMAND_NAME,
        "--node-private-key-file",
        privateKeyFile.toString(),
        "--ec-curve",
        "foo");

    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("foo is not in the list of valid elliptic curves");
  }
}
