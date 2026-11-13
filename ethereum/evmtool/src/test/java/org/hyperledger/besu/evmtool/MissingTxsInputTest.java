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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Inputs with no transactions that read some inputs from files, which the stdin-only spec tests in
 * {@link EvmToolSpecTests} can't express.
 */
class MissingTxsInputTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir Path tempDir;

  @Test
  void t8nWithEmptyStdinForTxsExecutesAnEmptyBlock() throws IOException {
    final JsonNode spec = readSpec("t8n/berlin-missing-txs.json");
    final Path alloc = write("alloc.json", spec.get("stdin").get("alloc").toString());
    final Path env = write("env.json", spec.get("stdin").get("env").toString());

    final String out =
        run(
            "",
            "t8n",
            "--input.alloc=" + alloc,
            "--input.env=" + env,
            "--input.txs=stdin",
            "--output.result=stdout",
            "--output.alloc=stdout",
            "--output.body=stdout",
            "--state.fork=Berlin",
            "--state.chainid=1",
            "--state.reward=0");

    assertThat(MAPPER.readTree(out)).isEqualTo(spec.get("stdout"));
  }

  @Test
  void b11rWithEmptyTxsFileBuildsABlockWithNoTransactions() throws IOException {
    final JsonNode spec = readSpec("b11r/simple.json");
    final Path txs = write("txs.rlp", "");

    final String out =
        run(
            spec.get("stdin").toString(),
            "b11r",
            "--input.header=stdin",
            "--input.txs=" + txs,
            "--input.ommers=stdin",
            "--seal.clique=stdin",
            "--output.block=stdout");

    assertThat(MAPPER.readTree(out)).isEqualTo(spec.get("stdout"));
  }

  private JsonNode readSpec(final String name) throws IOException {
    try (InputStream in = MissingTxsInputTest.class.getResourceAsStream(name)) {
      return MAPPER.readTree(in);
    }
  }

  private Path write(final String name, final String content) throws IOException {
    return Files.writeString(tempDir.resolve(name), content, UTF_8);
  }

  private static String run(final String stdin, final String... args) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new EvmToolCommand()
        .execute(
            new ByteArrayInputStream(stdin.getBytes(UTF_8)),
            new PrintWriter(baos, true, UTF_8),
            args);
    return baos.toString(UTF_8);
  }
}
