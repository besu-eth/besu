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

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.referencetests.GeneralStateTestCaseSpec;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestProtocolSchedules;
import org.hyperledger.besu.evmtool.exception.UnsupportedForkException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
  void testNameMatchesSubstringsAndPatterns() {
    // Same semantics as block-test: a substring, or a whole-id regex once the expression carries a
    // wildcard.
    assertThat(runWithArgs("--test-name", "accessList", "access-list.json")).contains("\"d\"");
    assertThat(runWithArgs("--test-name", "accessLis", "access-list.json")).contains("\"d\"");
    assertThat(runWithArgs("--test-name", "*ccess?ist", "access-list.json")).contains("\"d\"");
  }

  @Test
  void aTestNameThatMatchesNothingIsAnErrorRatherThanAnEmptyPass() {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final StateTestSubCommand stateTestSubCommand =
        new StateTestSubCommand(new EvmToolCommand(System.in, new PrintWriter(baos, true, UTF_8)));
    new CommandLine(stateTestSubCommand)
        .parseArgs(
            "--test-name",
            "noSuchTest",
            StateTestSubCommandTest.class.getResource("access-list.json").getPath());
    stateTestSubCommand.run();
    assertThat(stateTestSubCommand.getExitCode()).isEqualTo(1);
    assertThat(baos.toString(UTF_8))
        .contains("No state test was executed matching --test-name 'noSuchTest'");
  }

  @Test
  void anEmptyRunIsAnErrorEvenWithoutAFilter() {
    // Not gated on --test-name, so a fixture tree that did not materialise also fails the run.
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final StateTestSubCommand stateTestSubCommand =
        new StateTestSubCommand(
            new EvmToolCommand(
                new ByteArrayInputStream(new byte[0]), new PrintWriter(baos, true, UTF_8)));
    stateTestSubCommand.run();
    assertThat(stateTestSubCommand.getExitCode()).isEqualTo(1);
    assertThat(baos.toString(UTF_8)).contains("No state test was executed.");
  }

  @Test
  void summaryOnlySuppressesThePerTestLineForPassingTests() {
    assertThat(runWithArgs("access-list.json")).contains("\"pass\":true");
    assertThat(runWithArgs("--summary-only", "access-list.json"))
        .doesNotContain("\"pass\":true")
        .contains("State test summary: ");
  }

  @Test
  void jsonOutputContainsOnlyJsonLines() throws Exception {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final EvmToolCommand parentCommand =
        new EvmToolCommand(System.in, new PrintWriter(baos, true, UTF_8));
    new CommandLine(parentCommand).parseArgs("--json", "--notime");
    final StateTestSubCommand stateTestSubCommand = new StateTestSubCommand(parentCommand);
    new CommandLine(stateTestSubCommand)
        .parseArgs(StateTestSubCommandTest.class.getResource("access-list.json").getPath());

    stateTestSubCommand.run();

    final String output = baos.toString(UTF_8);
    for (final String line : output.lines().filter(value -> !value.isBlank()).toList()) {
      assertThat(JsonUtils.createObjectMapper().readTree(line)).isNotNull();
    }
    assertThat(output).contains("\"test\":\"accessList\"").doesNotContain("State test summary:");
  }

  @Test
  void missingFileIsReportedRatherThanReadAsAListOfFilenamesFromStdin() {
    // An empty file list means "read filenames from stdin", so dropping an unresolvable path here
    // would leave the command blocked on stdin.
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
    assertThat(baos.toString(UTF_8)).contains("not readable as state tests");
    // A fixture that could not be parsed has to fail the run.
    assertThat(stateTestSubCommand.getExitCode()).isEqualTo(1);
  }

  @Test
  void anUnreadableFixtureInAFilenameListFailsTheRunEvenBesideAPassingOne() {
    // The filename-list mode used to bypass the declared-case count, so reconciliation was off.
    final String filenames =
        resourcePath("access-list.json") + "\n" + resourcePath("bogus-test.json") + "\n";
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final StateTestSubCommand stateTestSubCommand =
        new StateTestSubCommand(
            new EvmToolCommand(
                new ByteArrayInputStream(filenames.getBytes(UTF_8)),
                new PrintWriter(baos, true, UTF_8)));
    stateTestSubCommand.run();
    assertThat(baos.toString(UTF_8)).contains("\"pass\":true").contains("NOT RUN:");
    assertThat(stateTestSubCommand.getExitCode()).isEqualTo(1);
  }

  @Test
  void aCaseWithNoVariantsInAFilenameListIsNotRun(@TempDir final Path dir) throws IOException {
    final Path fixture = writeFixture(dir, Map.of("emptyPost", Map.of()));
    final String filenames = resourcePath("access-list.json") + "\n" + fixture + "\n";
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final StateTestSubCommand stateTestSubCommand =
        new StateTestSubCommand(
            new EvmToolCommand(
                new ByteArrayInputStream(filenames.getBytes(UTF_8)),
                new PrintWriter(baos, true, UTF_8)));
    stateTestSubCommand.run();
    assertThat(baos.toString(UTF_8)).contains("NOT RUN: 1 of 2 declared case(s) never executed");
    assertThat(stateTestSubCommand.getExitCode()).isEqualTo(1);
  }

  @Test
  void anUndeserializableStdinFixtureIsCountedBesideAPassingFile() {
    // Standard input cannot be re-read, so the count has to come from the same buffered content.
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final StateTestSubCommand stateTestSubCommand =
        new StateTestSubCommand(
            new EvmToolCommand(
                new ByteArrayInputStream("{\"a\": 1, \"b\": 2}".getBytes(UTF_8)),
                new PrintWriter(baos, true, UTF_8)));
    new CommandLine(stateTestSubCommand).parseArgs("stdin", resourcePath("access-list.json"));
    stateTestSubCommand.run();
    assertThat(baos.toString(UTF_8)).contains("NOT RUN: 2 of 3 declared case(s) never executed");
    assertThat(stateTestSubCommand.getExitCode()).isEqualTo(1);
  }

  @Test
  void aStdinFixtureThatIsNotJsonStillCountsAsACaseNotRun() {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final StateTestSubCommand stateTestSubCommand =
        new StateTestSubCommand(
            new EvmToolCommand(
                new ByteArrayInputStream("not json".getBytes(UTF_8)),
                new PrintWriter(baos, true, UTF_8)));
    new CommandLine(stateTestSubCommand).parseArgs("stdin", resourcePath("access-list.json"));
    stateTestSubCommand.run();
    assertThat(baos.toString(UTF_8)).contains("NOT RUN: 1 of 2 declared case(s) never executed");
    assertThat(stateTestSubCommand.getExitCode()).isEqualTo(1);
  }

  @Test
  void aCaseWhollyExcludedByForkIndexIsFilteredRatherThanNotRun(@TempDir final Path dir)
      throws IOException {
    // Selecting one fork from a mixed tree is ordinary use; the cases without it must not fail it.
    final Map<String, Object> berlin = accessListCase();
    final Map<String, Object> cancunOnly = accessListCase();
    cancunOnly.put("post", Map.of("Cancun", ((Map<?, ?>) berlin.get("post")).get("Berlin")));
    final Path fixture = writeFixture(dir, Map.of("berlin", berlin, "cancunOnly", cancunOnly));

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final StateTestSubCommand stateTestSubCommand =
        new StateTestSubCommand(new EvmToolCommand(System.in, new PrintWriter(baos, true, UTF_8)));
    new CommandLine(stateTestSubCommand).parseArgs("--fork-index", "Berlin", fixture.toString());
    stateTestSubCommand.run();
    assertThat(baos.toString(UTF_8)).contains("\"pass\":true").doesNotContain("NOT RUN");
    assertThat(stateTestSubCommand.getExitCode()).isEqualTo(0);
  }

  @ParameterizedTest
  @ValueSource(strings = {"legacy-fork-tangerinewhistle.json", "legacy-fork-spuriousdragon.json"})
  void legacyForkNamesRunUnderTheirEipSchedules(final String fixture) {
    // The stable fixtures name these forks TangerineWhistle and SpuriousDragon, which the schedules
    // know as EIP150 and EIP158. A touch test, so the two expect different roots: it only passes
    // if each alias also picks up its fork's empty-account rule (EIP-161 from SpuriousDragon).
    assertThat(runWithArgs("--summary-only", fixture))
        .contains("State test summary: 1 passed, 0 failed")
        .doesNotContain("NOT RUN");
  }

  @Test
  void aCaseWithNoVariantsIsNotRunRatherThanExecuted(@TempDir final Path dir) throws IOException {
    final Map<String, Object> emptyPost = accessListCase();
    emptyPost.put("post", Map.of());
    final Path fixture =
        writeFixture(dir, Map.of("accessList", accessListCase(), "emptyPost", emptyPost));

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final StateTestSubCommand stateTestSubCommand =
        new StateTestSubCommand(new EvmToolCommand(System.in, new PrintWriter(baos, true, UTF_8)));
    new CommandLine(stateTestSubCommand).parseArgs(fixture.toString());
    stateTestSubCommand.run();
    assertThat(baos.toString(UTF_8)).contains("NOT RUN: 1 of 2 declared case(s) never executed");
    assertThat(stateTestSubCommand.getExitCode()).isEqualTo(1);
  }

  @Test
  void anOversizedScalarIsDecodedFromTheRawTransactionBytes() throws IOException {
    // No builder can hold a 33-byte gas price, so only Besu's own decoder can say whether it
    // rejects one. Resolving the variant to no transaction at all would pass it unchecked.
    final KeyPair keys = SignatureAlgorithmFactory.getInstance().generateKeyPair();
    final Transaction raw =
        Transaction.builder()
            .type(TransactionType.FRONTIER)
            .nonce(0)
            .gasPrice(Wei.ONE)
            .gasLimit(400_000)
            .to(Address.fromHexString("0x095e7baea6a6c7c4c2dfeb977efac326af552d87"))
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .signAndBuild(keys);
    final Map<String, Object> overflow = accessListCase();
    final Map<String, Object> transaction = mutableMap(overflow.get("transaction"));
    transaction.put("gasPrice", "0x01" + "00".repeat(32));
    overflow.put("transaction", transaction);
    final Map<String, Object> variant =
        mutableMap(((List<?>) ((Map<?, ?>) overflow.get("post")).get("Berlin")).getFirst());
    variant.put(
        "txbytes",
        TransactionEncoder.encodeOpaqueBytes(raw, EncodingContext.BLOCK_BODY).toHexString());
    overflow.put("post", Map.of("Berlin", List.of(variant)));

    final GeneralStateTestCaseSpec spec =
        JsonUtils.createObjectMapper().convertValue(overflow, GeneralStateTestCaseSpec.class);
    assertThat(spec.finalStateSpecs().get("Berlin").getFirst().getTransaction(0)).isEqualTo(raw);
  }

  @ParameterizedTest
  @ValueSource(strings = {"nonce", "to"})
  void aMissingNonceOrMalformedFieldMakesTheFixtureUnreadable(
      final String field, @TempDir final Path dir) throws IOException {
    // Neither is a bounds test. Run as one, a missing nonce became 0 and a non-hex recipient an
    // invalid transaction, so a broken fixture passed.
    final Map<String, Object> broken = accessListCase();
    final Map<String, Object> transaction = mutableMap(broken.get("transaction"));
    if ("nonce".equals(field)) {
      transaction.remove("nonce");
    } else {
      transaction.put("to", "0xnothex");
    }
    broken.put("transaction", transaction);
    final Run run = run(writeFixture(dir, Map.of("broken", broken)));
    assertThat(run.out()).contains("not readable as state tests").contains("NOT RUN:");
    assertThat(run.exitCode()).isEqualTo(1);
  }

  @Test
  void anUnreadableFixtureWhoseCasesAreAllUnselectedDoesNotFailAFilteredRun(@TempDir final Path dir)
      throws IOException {
    // A run filtered to some tests is not failed by an unreadable fixture it would not have run.
    final Path unreadable = dir.resolve("unreadable.json");
    Files.writeString(unreadable, "{\"otherTest\": 1}");
    final Run run =
        run("--test-name", "accessList", resourcePath("access-list.json"), unreadable.toString());
    assertThat(run.out()).contains("\"pass\":true").doesNotContain("NOT RUN");
    assertThat(run.exitCode()).isEqualTo(0);
  }

  @Test
  void jsonArrayCarriesAFailingRowForEveryUnreadableFileAndTheCasesNotRun() throws IOException {
    // The run exits 1, so the array alone has to say why: every executed row here passes.
    final Run run =
        run("--json-array", resourcePath("access-list.json"), resourcePath("bogus-test.json"));
    final List<Map<String, Object>> rows =
        JsonUtils.createObjectMapper().readValue(run.out(), new TypeReference<>() {});
    assertThat(rows)
        .filteredOn(row -> Boolean.FALSE.equals(row.get("pass")))
        .extracting(row -> row.get("name"))
        .containsExactly(resourcePath("bogus-test.json"), "NOT RUN");
    assertThat(run.exitCode()).isEqualTo(1);
  }

  @Test
  void anExecutionErrorFailsOnlyItsOwnVariant(@TempDir final Path dir) throws IOException {
    // An excess blob gas this large prices blob gas beyond what Wei can hold, which throws outside
    // the transaction processor. It used to abandon every later variant in that fork, so the
    // second variant here, an expected rejection, never ran.
    final Map<String, Map<String, Object>> fixture =
        JsonUtils.createObjectMapper()
            .readValue(new File(resourcePath("excess-blob-gas.json")), new TypeReference<>() {});
    final Map<String, Object> broken = new LinkedHashMap<>(fixture.get("excessBlobGasTest"));
    final Map<String, Object> env = mutableMap(broken.get("env"));
    env.put("currentExcessBlobGas", "0x40000000");
    broken.put("env", env);
    final Object throwing = ((List<?>) ((Map<?, ?>) broken.get("post")).get("Cancun")).getFirst();
    final Map<String, Object> rejected = mutableMap(throwing);
    rejected.put("indexes", Map.of("data", 0, "gas", 0, "value", 9));
    rejected.put("expectException", "TransactionException.INTRINSIC_GAS_TOO_LOW");
    broken.put("post", Map.of("Cancun", List.of(throwing, rejected)));

    final Run run = run(writeFixture(dir, Map.of("broken", broken)));
    assertThat(run.out())
        .contains("FAIL: broken [Cancun d=0 g=0 v=0] - execution error")
        .contains("1 passed, 1 failed");
    assertThat(run.exitCode()).isEqualTo(1);
  }

  @Test
  void variantsOverTheBlockGasLimitAreListedAsSkipped(@TempDir final Path dir) throws IOException {
    final Map<String, Object> oversized = accessListCase();
    final Map<String, Object> transaction = mutableMap(oversized.get("transaction"));
    transaction.put("gasLimit", List.of("0xffffffffffffff"));
    oversized.put("transaction", transaction);
    final Run run =
        run(
            "--summary-only",
            writeFixture(dir, Map.of("accessList", accessListCase(), "oversized", oversized))
                .toString());
    assertThat(run.out())
        .contains("Filtered out: 1 case(s)")
        .contains("2 variant(s) skipped, their transaction exceeding the block gas limit")
        .contains("  - oversized [Berlin d=0 g=0 v=0]");
    assertThat(run.exitCode()).isEqualTo(0);
  }

  @Test
  void forkIndexMatchesAForkByEitherOfItsNames() {
    assertThat(runWithArgs("--fork-index", "EIP150", "legacy-fork-tangerinewhistle.json"))
        .contains("\"pass\":true");
    assertThat(runWithArgs("--fork-index", "eip158", "legacy-fork-spuriousdragon.json"))
        .contains("\"pass\":true");
  }

  @Test
  void forkOverridesTheEmptyAccountRulesAsWellAsTheSchedule() {
    // SpuriousDragon's expected root assumes EIP-161 clears the touched empty account. Run under
    // TangerineWhistle it is kept, so the roots differ and the variant has to fail.
    assertThat(
            runWithArgs(
                "--summary-only", "--fork", "TangerineWhistle", "legacy-fork-spuriousdragon.json"))
        .contains("0 passed, 1 failed");
  }

  @ParameterizedTest
  @ValueSource(strings = {"Frontier", "homestead", "EIP150", "eip150", "TangerineWhistle"})
  void forksBeforeSpuriousDragonKeepEmptyAccountsWhateverTheCase(final String fork) {
    assertThat(ReferenceTestProtocolSchedules.shouldClearEmptyAccounts(fork)).isFalse();
  }

  private record Run(String out, int exitCode) {}

  private static Run run(final Path fixture) {
    return run(fixture.toString());
  }

  private static Run run(final String... args) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final StateTestSubCommand command =
        new StateTestSubCommand(new EvmToolCommand(System.in, new PrintWriter(baos, true, UTF_8)));
    new CommandLine(command).parseArgs(args);
    command.run();
    return new Run(baos.toString(UTF_8), command.getExitCode());
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> mutableMap(final Object map) {
    return new LinkedHashMap<>((Map<String, Object>) map);
  }

  private static String resourcePath(final String resource) {
    return StateTestSubCommandTest.class.getResource(resource).getPath();
  }

  private static Map<String, Object> accessListCase() throws IOException {
    final Map<String, Map<String, Object>> fixture =
        JsonUtils.createObjectMapper()
            .readValue(new File(resourcePath("access-list.json")), new TypeReference<>() {});
    return new LinkedHashMap<>(fixture.get("accessList"));
  }

  private static Path writeFixture(final Path dir, final Map<String, ?> cases) throws IOException {
    final Path fixture = dir.resolve("fixture.json");
    JsonUtils.createObjectMapper().writeValue(fixture.toFile(), cases);
    return fixture;
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
