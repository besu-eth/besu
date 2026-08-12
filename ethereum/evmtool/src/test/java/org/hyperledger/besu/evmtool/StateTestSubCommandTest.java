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
package org.hyperledger.besu.evmtool;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.evmtool.exception.UnsupportedForkException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class StateTestSubCommandTest {

  @Test
  void shouldDetectUnsupportedFork() {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    EvmToolCommand parentCommand =
        new EvmToolCommand(System.in, new PrintWriter(baos, true, UTF_8));
    final StateTestSubCommand stateTestSubCommand = new StateTestSubCommand(parentCommand);
    final CommandLine cmd = new CommandLine(stateTestSubCommand);
    cmd.parseArgs(
        StateTestSubCommandTest.class.getResource("unsupported-fork-state-test.json").getPath());
    assertThatThrownBy(stateTestSubCommand::run)
        .hasMessageContaining("Fork 'UnknownFork' not supported")
        .isInstanceOf(UnsupportedForkException.class);
  }

  @Test
  void testNameMatchesExactlyAndRunMatchesSubstrings() {
    // Conflating the two would make a caller that passes an exact test id run every test whose
    // name merely contains it.
    assertThat(runWithArgs("--test-name", "accessLis", "access-list.json")).doesNotContain("\"d\"");
    assertThat(runWithArgs("--test-name", "accessList", "access-list.json")).contains("\"d\"");
    assertThat(runWithArgs("--run", "accessLis", "access-list.json")).contains("\"d\"");
    assertThat(runWithArgs("--run", "*ccess?ist", "access-list.json")).contains("\"d\"");
  }

  @Test
  void summaryOnlySuppressesThePerTestLineForPassingTests() {
    assertThat(runWithArgs("access-list.json")).contains("\"pass\":true");
    assertThat(runWithArgs("--summary-only", "access-list.json"))
        .doesNotContain("\"pass\":true")
        .contains("State test summary: ");
  }

  @Test
  void missingFileIsReportedRatherThanReadAsAListOfFilenamesFromStdin() {
    // An empty file list means "read filenames from stdin", so a path that resolves to nothing
    // has to be an error rather than a silent drop, or the command blocks forever.
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final StateTestSubCommand stateTestSubCommand =
        new StateTestSubCommand(new EvmToolCommand(System.in, new PrintWriter(baos, true, UTF_8)));
    new CommandLine(stateTestSubCommand).parseArgs("./file-does-not-exist.json");
    stateTestSubCommand.run();
    assertThat(stateTestSubCommand.getExitCode()).isEqualTo(1);
  }

  private String runWithArgs(final String... args) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final EvmToolCommand parentCommand =
        new EvmToolCommand(System.in, new PrintWriter(baos, true, UTF_8));
    final StateTestSubCommand stateTestSubCommand = new StateTestSubCommand(parentCommand);
    final String[] resolved = args.clone();
    resolved[resolved.length - 1] =
        StateTestSubCommandTest.class.getResource(resolved[resolved.length - 1]).getPath();
    new CommandLine(stateTestSubCommand).parseArgs(resolved);
    stateTestSubCommand.run();
    return baos.toString(UTF_8);
  }

  @Test
  void shouldWorkWithValidStateTest() {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    EvmToolCommand parentCommand =
        new EvmToolCommand(System.in, new PrintWriter(baos, true, UTF_8));
    final StateTestSubCommand stateTestSubCommand = new StateTestSubCommand(parentCommand);
    final CommandLine cmd = new CommandLine(stateTestSubCommand);
    cmd.parseArgs(StateTestSubCommandTest.class.getResource("valid-state-test.json").getPath());
    stateTestSubCommand.run();
  }

  @Test
  void shouldWorkWithValidAccessListStateTest() {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    EvmToolCommand parentCommand =
        new EvmToolCommand(System.in, new PrintWriter(baos, true, UTF_8));
    final StateTestSubCommand stateTestSubCommand = new StateTestSubCommand(parentCommand);
    final CommandLine cmd = new CommandLine(stateTestSubCommand);
    cmd.parseArgs(StateTestSubCommandTest.class.getResource("access-list.json").getPath());
    stateTestSubCommand.run();
  }

  @Test
  void noJsonTracer() {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    EvmToolCommand parentCommand =
        new EvmToolCommand(System.in, new PrintWriter(baos, true, UTF_8));
    CommandLine parentCmd = new CommandLine(parentCommand);
    parentCmd.parseArgs("--json=false");
    final StateTestSubCommand stateTestSubCommand = new StateTestSubCommand(parentCommand);
    final CommandLine cmd = new CommandLine(stateTestSubCommand);
    cmd.parseArgs(StateTestSubCommandTest.class.getResource("access-list.json").getPath());
    stateTestSubCommand.run();
    assertThat(baos.toString(UTF_8)).doesNotContain("\"pc\"");
  }

  @Test
  void testsInvalidTransactions() {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final ByteArrayInputStream bais =
        new ByteArrayInputStream(
            StateTestSubCommandTest.class
                .getResource("HighGasPrice.json")
                .getPath()
                .getBytes(UTF_8));
    final StateTestSubCommand stateTestSubCommand =
        new StateTestSubCommand(new EvmToolCommand(bais, new PrintWriter(baos, true, UTF_8)));
    stateTestSubCommand.run();
    assertThat(baos.toString(UTF_8)).contains("Upfront gas cost cannot exceed 2^256 Wei");
  }

  @Test
  void shouldStreamTests() {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final ByteArrayInputStream bais =
        new ByteArrayInputStream(
            StateTestSubCommandTest.class
                .getResource("access-list.json")
                .getPath()
                .getBytes(UTF_8));
    final StateTestSubCommand stateTestSubCommand =
        new StateTestSubCommand(new EvmToolCommand(bais, new PrintWriter(baos, true, UTF_8)));
    stateTestSubCommand.run();
    assertThat(baos.toString(UTF_8)).contains("\"pass\":true");
  }

  @Test
  void failStreamMissingFile() {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final ByteArrayInputStream bais =
        new ByteArrayInputStream("./file-dose-not-exist.json".getBytes(UTF_8));
    final StateTestSubCommand stateTestSubCommand =
        new StateTestSubCommand(new EvmToolCommand(bais, new PrintWriter(baos, true, UTF_8)));
    stateTestSubCommand.run();
    assertThat(baos.toString(UTF_8)).contains("File not found: ./file-dose-not-exist.json");
  }

  @Test
  void failStreamBadFile() {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final ByteArrayInputStream bais =
        new ByteArrayInputStream(
            StateTestSubCommandTest.class.getResource("bogus-test.json").getPath().getBytes(UTF_8));
    final StateTestSubCommand stateTestSubCommand =
        new StateTestSubCommand(new EvmToolCommand(bais, new PrintWriter(baos, true, UTF_8)));
    stateTestSubCommand.run();
    assertThat(baos.toString(UTF_8)).contains("File content error: ");
  }

  @Test
  void invalidTransactionShouldNotModifyState() {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    EvmToolCommand parentCommand =
        new EvmToolCommand(System.in, new PrintWriter(baos, true, UTF_8));
    final StateTestSubCommand stateTestSubCommand = new StateTestSubCommand(parentCommand);
    final CommandLine cmd = new CommandLine(stateTestSubCommand);
    cmd.parseArgs(StateTestSubCommandTest.class.getResource("HighGasPrice.json").getPath());
    stateTestSubCommand.run();

    final String output = baos.toString(UTF_8);
    // Invalid transaction should have validation error
    assertThat(output).contains("validationError");
    // Should have the upfront gas cost error message
    assertThat(output).contains("Upfront gas cost cannot exceed 2^256 Wei");
    // State root should match expected (no state modification occurred)
    assertThat(output)
        .contains(
            "\"stateRoot\":\"0x1751725d1aad5298768fbcf64069b2c1b85aeaffcc561146067d6beedd08052a\"");
    // Both error and validationError fields should be present for invalid transactions
    assertThat(output).contains("\"error\":\"Upfront gas cost cannot exceed 2^256 Wei\"");
  }

  @Test
  void shouldUseExcessBlobGasFromEnvironment() {
    // Tests that BLOBBASEFEE opcode uses currentExcessBlobGas from the test environment.
    // With excessBlobGas=0x240000 and Cancun blob fee fraction (3338477), blob price = 2.
    // The contract stores BLOBBASEFEE result in storage slot 0.
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    EvmToolCommand parentCommand =
        new EvmToolCommand(System.in, new PrintWriter(baos, true, UTF_8));
    final StateTestSubCommand stateTestSubCommand = new StateTestSubCommand(parentCommand);
    final CommandLine cmd = new CommandLine(stateTestSubCommand);
    cmd.parseArgs(StateTestSubCommandTest.class.getResource("excess-blob-gas.json").getPath());
    stateTestSubCommand.run();

    final String output = baos.toString(UTF_8);
    // State root should match expected value (computed with correct blob gas price = 2)
    assertThat(output)
        .contains(
            "\"stateRoot\":\"0x4f0dafcdc942cf538ffe1f870ab031c2761857b3066f595e56c74bcb222eb0bb\"");
    assertThat(output).contains("\"pass\":true");
  }

  @Test
  void eip6780FailedCreateShouldNotDeletePreexistingAccount() {
    // Regression test for consensus bug: a depth-0 CREATE whose initcode does a successful
    // CREATE2→SELFDESTRUCT child and then returns oversized code (MAX_CODE_SIZE+1) must NOT
    // delete the pre-existing child account. Besu previously produced a divergent post-state root
    // (0x850df194…) instead of the reference root (0x252171e5…) agreed upon by all other clients.
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    EvmToolCommand parentCommand =
        new EvmToolCommand(System.in, new PrintWriter(baos, true, UTF_8));
    final StateTestSubCommand stateTestSubCommand = new StateTestSubCommand(parentCommand);
    final CommandLine cmd = new CommandLine(stateTestSubCommand);
    cmd.parseArgs(
        StateTestSubCommandTest.class.getResource("eip6780-failed-create.json").getPath());
    stateTestSubCommand.run();

    final String output = baos.toString(UTF_8);
    assertThat(output)
        .as("post-state root must match the reference value from geth/erigon/revm/eels")
        .contains(
            "\"stateRoot\":\"0x252171e59678d6fd87e7fcec3992f266da5566b63c1d5c1595b79ccb53c76145\"");
    assertThat(output).contains("\"pass\":true");
    assertThat(output).doesNotContain("\"pass\":false");
  }
}
