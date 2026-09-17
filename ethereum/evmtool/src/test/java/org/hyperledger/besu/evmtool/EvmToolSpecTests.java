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

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.IterableAssert.assertThatIterable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class EvmToolSpecTests {

  static final ObjectMapper objectMapper = new ObjectMapper();
  static final ObjectReader specReader = objectMapper.reader();

  public static Object[][] b11rTests() {
    return findSpecFiles(new String[] {"b11r"});
  }

  public static Object[][] stateTestTests() {
    return findSpecFiles(new String[] {"state-test"});
  }

  public static Object[][] t8nTests() {
    return findSpecFiles(new String[] {"t8n"});
  }

  public static Object[][] traceTests() {
    return findSpecFiles(new String[] {"trace"});
  }

  public static Object[][] findSpecFiles(
      final String[] subDirectoryPaths, final String... exceptions) {
    final List<Object[]> specFiles = new ArrayList<>();
    for (final String path : subDirectoryPaths) {
      final URL url = EvmToolSpecTests.class.getResource(path);
      checkState(url != null, "Cannot find test directory " + path);
      final Path dir;
      try {
        dir = Paths.get(url.toURI());
      } catch (final URISyntaxException e) {
        throw new RuntimeException("Problem converting URL to URI " + url, e);
      }
      try (final Stream<Path> s = Files.walk(dir, 1)) {
        s.map(Path::toFile)
            .filter(f -> f.getPath().endsWith(".json"))
            .filter(f -> Arrays.stream(exceptions).noneMatch(f.getPath()::contains))
            .map(f -> pathToParams(path, f))
            .forEach(specFiles::add);
      } catch (final IOException e) {
        throw new RuntimeException("Problem reading directory " + dir, e);
      }
    }
    final Object[][] result = new Object[specFiles.size()][2];
    for (int i = 0; i < specFiles.size(); i++) {
      result[i] = specFiles.get(i);
    }
    return result;
  }

  private static Object[] pathToParams(final String subDir, final File file) {
    try {
      var spec = specReader.readTree(new FileInputStream(file));
      return new Object[] {
        subDir + "/" + file.getName(), spec.get("cli"), spec.get("stdin"), spec.get("stdout")
      };
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource({"b11rTests", "stateTestTests", "t8nTests", "traceTests"})
  void testBySpec(
      final String file,
      final JsonNode cliNode,
      final JsonNode stdinNode,
      final JsonNode stdoutNode)
      throws IOException {

    String[] cli;
    if (cliNode.isTextual()) {
      cli = cliNode.textValue().split("\\s+");
    } else {
      cli =
          StreamSupport.stream(
                  Spliterators.spliteratorUnknownSize(cliNode.iterator(), Spliterator.ORDERED),
                  false)
              .map(JsonNode::textValue)
              .toArray(String[]::new);
    }

    InputStream inputStream;
    if (stdinNode.isTextual()) {
      inputStream = new ByteArrayInputStream(stdinNode.textValue().getBytes(UTF_8));
    } else {
      inputStream = new ByteArrayInputStream(stdinNode.toString().getBytes(UTF_8));
    }

    EvmToolCommand evmTool = new EvmToolCommand();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    evmTool.execute(inputStream, new PrintWriter(baos, true, UTF_8), cli);

    if (stdoutNode.isTextual()) {
      assertThat(baos.toString(UTF_8)).isEqualTo(stdoutNode.textValue());
    } else if (stdoutNode.isArray()) {
      ArrayNode arrayNode = (ArrayNode) specReader.createArrayNode();
      int pos = 0;
      byte[] output = baos.toByteArray();
      while (pos < output.length) {
        int next = pos;
        //noinspection StatementWithEmptyBody
        while (output[next++] != ((byte) '\n')) {}
        try {
          JsonNode value = specReader.readTree(output, pos, next - pos);
          if (JsonNodeType.MISSING != value.getNodeType()) {
            arrayNode.add(value);
          }
        } catch (JsonParseException jpe) {
          // Discard non-well-formed lines.
          // If those are needed for validation use the text node option.
        }
        pos = next;
      }
      assertThatIterable(arrayNode::elements).containsExactlyElementsOf(stdoutNode::elements);
    } else {
      var actualNode = specReader.readTree(baos.toByteArray());
      assertThat(actualNode).isEqualTo(stdoutNode);
    }
  }
}
