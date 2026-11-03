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
package org.hyperledger.besu.evmtool;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * Regression coverage for {@code block-test --json-array} when a block's RLP cannot be decoded (see
 * #11328).
 */
class BlockchainTestSubCommandTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void jsonArrayReportsUndecodableRlpAsFailedRatherThanOmittingTheTest() throws Exception {
    final String output = runJsonArray("blockchain-truncated-rlp.json");
    final List<Map<String, Object>> results = MAPPER.readValue(output, new TypeReference<>() {});

    assertThat(results).hasSize(1);
    assertThat(results.getFirst().get("name")).isEqualTo("truncated_rlp_no_expectException");
    assertThat(results.getFirst().get("pass")).isEqualTo(false);
    assertThat(results.getFirst().get("error").toString()).contains("RLP exception");
  }

  @Test
  void jsonArrayKeepsSiblingTestsWhenOneBlockRlpIsCorrupt() throws Exception {
    final String output = runJsonArray("blockchain-mixed-rlp.json");
    final List<Map<String, Object>> results =
        MAPPER.readValue(output, new TypeReference<List<Map<String, Object>>>() {});

    assertThat(results).hasSize(2);
    final Map<String, Map<String, Object>> byName =
        results.stream()
            .collect(java.util.stream.Collectors.toMap(r -> (String) r.get("name"), r -> r));
    assertThat(byName).containsKeys("good_london_block", "truncated_rlp_no_expectException");
    assertThat(byName.get("truncated_rlp_no_expectException").get("pass")).isEqualTo(false);
    assertThat(byName.get("truncated_rlp_no_expectException").get("error").toString())
        .contains("RLP exception");
  }

  @Test
  void jsonArrayKeepsFirstBlockFailureWhenLaterBlockImports() throws Exception {
    final String output = runJsonArray("blockchain-truncated-then-good-rlp.json");
    final List<Map<String, Object>> results = MAPPER.readValue(output, new TypeReference<>() {});

    assertThat(results).hasSize(1);
    assertThat(results.getFirst().get("pass")).isEqualTo(false);
    assertThat(results.getFirst().get("error")).asString().contains("RLP exception");
  }

  @Test
  void anUnreadableFixtureInAFilenameListIsReportedWithoutAbandoningTheRest(@TempDir final Path dir)
      throws Exception {
    // The filename-list mode used to abort on the first unreadable file, skipping every file after
    // it, and bypassed the declared-case count entirely.
    final Map<String, Object> mixed =
        MAPPER.readValue(
            new File(resourcePath("blockchain-mixed-rlp.json")), new TypeReference<>() {});
    final Path good = dir.resolve("good.json");
    MAPPER.writeValue(good.toFile(), Map.of("good_london_block", mixed.get("good_london_block")));
    final String filenames = resourcePath("bogus-test.json") + "\n" + good + "\n";

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final BlockchainTestSubCommand command =
        new BlockchainTestSubCommand(
            new EvmToolCommand(
                new ByteArrayInputStream(filenames.getBytes(UTF_8)),
                new PrintWriter(baos, true, UTF_8)));
    new CommandLine(command).parseArgs();
    command.run();

    assertThat(baos.toString(UTF_8))
        .contains("Passed:       1")
        .contains("Unreadable:   1 file(s)")
        .contains("NOT RUN:      1 of 2 declared case(s) never executed");
    assertThat(command.getExitCode()).isEqualTo(1);
  }

  @Test
  @SuppressWarnings("unchecked")
  void anUnknownNetworkFailsItsTestWithoutAbandoningTheFile(@TempDir final Path dir)
      throws Exception {
    // The null schedule used to throw out of the whole file, so its siblings never ran and the
    // --json-array output was an empty array.
    final Map<String, Object> mixed =
        MAPPER.readValue(
            new File(resourcePath("blockchain-mixed-rlp.json")), new TypeReference<>() {});
    final Map<String, Object> unknown =
        new LinkedHashMap<>((Map<String, Object>) mixed.get("good_london_block"));
    unknown.put("network", "NoSuchFork");
    final Path fixture = dir.resolve("fixture.json");
    MAPPER.writeValue(
        fixture.toFile(),
        Map.of("good_london_block", mixed.get("good_london_block"), "unknown_network", unknown));

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final BlockchainTestSubCommand command =
        new BlockchainTestSubCommand(
            new EvmToolCommand(System.in, new PrintWriter(baos, true, UTF_8)));
    new CommandLine(command).parseArgs("--json-array", fixture.toString());
    command.run();

    final List<Map<String, Object>> rows =
        MAPPER.readValue(baos.toString(UTF_8), new TypeReference<>() {});
    assertThat(rows)
        .extracting(row -> row.get("name"), row -> row.get("pass"), row -> row.get("error"))
        .containsExactlyInAnyOrder(
            tuple("good_london_block", true, null),
            tuple("unknown_network", false, "Unsupported fork: NoSuchFork"));
    assertThat(command.getExitCode()).isEqualTo(1);
  }

  @Test
  void jsonArrayCarriesAFailingRowForAnUnreadableFileAndTheCasesNotRun() throws Exception {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final BlockchainTestSubCommand command =
        new BlockchainTestSubCommand(
            new EvmToolCommand(System.in, new PrintWriter(baos, true, UTF_8)));
    new CommandLine(command).parseArgs("--json-array", resourcePath("bogus-test.json"));
    command.run();

    final List<Map<String, Object>> rows =
        MAPPER.readValue(baos.toString(UTF_8), new TypeReference<>() {});
    assertThat(rows)
        .extracting(row -> row.get("name"), row -> row.get("pass"))
        .containsExactly(tuple(resourcePath("bogus-test.json"), false), tuple("NOT RUN", false));
    assertThat(command.getExitCode()).isEqualTo(1);
  }

  @Test
  void blocksCanBeImportedWithoutTheirAccessList() {
    // Through the root command, whose negation pattern makes this --noblock-access-list
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final int exitCode =
        new EvmToolCommand()
            .execute(
                System.in,
                new PrintWriter(baos, true, UTF_8),
                new String[] {
                  "block-test",
                  "--noblock-access-list",
                  "--test-name",
                  "good_london_block",
                  resourcePath("blockchain-mixed-rlp.json")
                });
    assertThat(baos.toString(UTF_8)).contains("Passed:       1");
    assertThat(exitCode).isEqualTo(0);
  }

  private static String resourcePath(final String resource) {
    return BlockchainTestSubCommandTest.class.getResource(resource).getPath();
  }

  private static String runJsonArray(final String fixtureResource) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final EvmToolCommand parentCommand =
        new EvmToolCommand(System.in, new PrintWriter(baos, true, UTF_8));
    final BlockchainTestSubCommand command = new BlockchainTestSubCommand(parentCommand);
    new CommandLine(command)
        .parseArgs(
            "--json-array",
            "--workers",
            "1",
            BlockchainTestSubCommandTest.class.getResource(fixtureResource).getPath());
    command.run();
    assertThat(command.getExitCode()).isEqualTo(1);
    return baos.toString(UTF_8).trim();
  }
}
