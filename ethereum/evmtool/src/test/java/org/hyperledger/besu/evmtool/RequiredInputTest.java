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
package org.hyperledger.besu.evmtool;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;
import picocli.CommandLine.ParameterException;

/** Missing, null or empty required inputs give a usage error instead of a stack trace. */
class RequiredInputTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String[] T8N_STDIN_ARGS = {
    "--input.alloc=stdin",
    "--input.txs=stdin",
    "--input.env=stdin",
    "--output.result=stdout",
    "--state.fork=Berlin"
  };

  @TempDir Path tempDir;

  @Test
  void t8nWithoutEnvReportsMissingEnv() throws IOException {
    final ObjectNode stdin = t8nStdin();
    stdin.remove("env");

    assertThatThrownBy(() -> runT8n(stdin.toString(), T8N_STDIN_ARGS))
        .isInstanceOf(ParameterException.class)
        .hasMessage("Missing env input, see --input.env");
  }

  @Test
  void t8nWithNullAllocReportsMissingAlloc() throws IOException {
    final ObjectNode stdin = t8nStdin();
    stdin.set("alloc", NullNode.getInstance());

    assertThatThrownBy(() -> runT8n(stdin.toString(), T8N_STDIN_ARGS))
        .isInstanceOf(ParameterException.class)
        .hasMessage("Missing alloc input, see --input.alloc");
  }

  @Test
  void t8nWithEmptyEnvFileReportsMissingEnv() throws IOException {
    final ObjectNode stdin = t8nStdin();
    stdin.remove("env");
    final Path env = write("env.json", "");

    assertThatThrownBy(
            () ->
                runT8n(
                    stdin.toString(),
                    "--input.alloc=stdin",
                    "--input.txs=stdin",
                    "--input.env=" + env,
                    "--output.result=stdout",
                    "--state.fork=Berlin"))
        .isInstanceOf(ParameterException.class)
        .hasMessage("Missing env input, see --input.env");
  }

  @Test
  void b11rWithoutHeaderReportsMissingHeader() throws IOException {
    final ObjectNode stdin = (ObjectNode) readSpec("b11r/simple.json").get("stdin");
    stdin.remove("header");

    assertThatThrownBy(() -> runB11r(stdin.toString(), b11rArgs("--input.header=stdin")))
        .isInstanceOf(ParameterException.class)
        .hasMessage("Missing header input, see --input.header");
  }

  @Test
  void b11rWithEmptyStdinAndNoHeaderReportsMissingHeader() {
    assertThatThrownBy(() -> runB11r("", b11rArgs("--input.header=stdin")))
        .isInstanceOf(ParameterException.class)
        .hasMessage("Missing header input, see --input.header");
  }

  @Test
  void b11rWithEmptyStdinBuildsTheBlockFromTheHeaderFile() throws IOException {
    final JsonNode spec = readSpec("b11r/simple.json");
    final Path header = write("header.json", spec.get("stdin").get("header").toString());

    final String out = runB11r("", b11rArgs("--input.header=" + header));

    assertThat(MAPPER.readTree(out)).isEqualTo(spec.get("stdout"));
  }

  private static String[] b11rArgs(final String headerArg) {
    return new String[] {
      headerArg,
      "--input.txs=stdin",
      "--input.ommers=stdin",
      "--seal.clique=stdin",
      "--output.block=stdout"
    };
  }

  private ObjectNode t8nStdin() throws IOException {
    return (ObjectNode) readSpec("t8n/berlin-no-tx.json").get("stdin");
  }

  private JsonNode readSpec(final String name) throws IOException {
    try (InputStream in = RequiredInputTest.class.getResourceAsStream(name)) {
      return MAPPER.readTree(in);
    }
  }

  private Path write(final String name, final String content) throws IOException {
    return Files.writeString(tempDir.resolve(name), content, UTF_8);
  }

  private static void runT8n(final String stdin, final String... args) {
    final EvmToolCommand parent = parent(stdin, new ByteArrayOutputStream());
    final T8nSubCommand t8n = new T8nSubCommand(parent);
    new CommandLine(t8n).parseArgs(args);
    t8n.run();
  }

  /** Runs b11r through the subcommand so a ParameterException reaches the test. */
  private static String runB11r(final String stdin, final String... args) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final B11rSubCommand b11r = new B11rSubCommand(parent(stdin, baos));
    new CommandLine(b11r).parseArgs(args);
    b11r.run();
    return baos.toString(UTF_8);
  }

  private static EvmToolCommand parent(final String stdin, final ByteArrayOutputStream out) {
    return new EvmToolCommand(
        new ByteArrayInputStream(stdin.getBytes(UTF_8)), new PrintWriter(out, true, UTF_8));
  }
}
