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
package org.hyperledger.besu.ethereum.api.jsonrpc;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;
import static com.google.common.base.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractJsonRpcHttpBySpecTest extends AbstractJsonRpcHttpServiceTest {

  private static final boolean UPDATE_SPECS = Boolean.getBoolean("besu.test.update.specs");
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final ObjectMapper prettyObjectMapper =
      new ObjectMapper().configure(INDENT_OUTPUT, true);
  private static final Pattern GAS_MATCH_FOR_STATE_DIFF =
      Pattern.compile("\"balance\":(?!\"=\").*?},");
  private static final Pattern GAS_MATCH_FOR_VM_TRACE =
      Pattern.compile(",*\"cost\":[0-9a-fA-F]+,*|,\"used\":[0-9a-fA-F]+");
  private static final Pattern GAS_MATCH_FOR_TRACE =
      Pattern.compile("\"gasUsed\":\"[x0-9a-fA-F]+\",");
  private static final Pattern ERROR_MESSAGE_NORMALIZATION =
      Pattern.compile("\"error\"\\s*:\\s*\"([^\"]+)\"");

  private URL specURL;
  private volatile boolean initialized = false;

  /**
   * Subclasses implement this to perform one-time blockchain + service setup. Called the first time
   * {@link #setup()} runs; skipped on subsequent parameterized test invocations.
   */
  protected abstract void doSetup() throws Exception;

  @Override
  @BeforeEach
  public void setup() throws Exception {
    if (!initialized) {
      initialized = true;
      doSetup();
    } else if (filterManager != null) {
      filterManager.stop();
    }
  }

  @AfterAll
  public void teardownAll() {
    super.shutdownServer();
  }

  @Override
  public void shutdownServer() {
    // Suppressed: teardown happens once in @AfterAll, not after every parameterized test case
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("specs")
  public void jsonRPCCallWithSpecFile(final String specName, final URL specURL) throws Exception {
    this.specURL = specURL;
    jsonRPCCall(this.specURL);
  }

  /**
   * Searches the provided subdirectories for spec files containing test cases, and returns
   * parameters required to run these tests.
   *
   * @param subDirectoryPaths The subdirectories to search.
   * @return Parameters for this test which will contain the name of the test and the url of the
   *     spec file to run.
   */
  public static Object[][] findSpecFiles(
      final String[] subDirectoryPaths, final String... exceptions) {
    final List<Object[]> specFiles = new ArrayList<>();
    for (final String path : subDirectoryPaths) {
      final URL url = AbstractJsonRpcHttpBySpecTest.class.getResource(path);
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
            .filter(f -> !f.getPath().contains("genesis"))
            .filter(f -> Arrays.stream(exceptions).noneMatch(f.getPath()::contains))
            .map(AbstractJsonRpcHttpBySpecTest::fileToParams)
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

  private static Object[] fileToParams(final File file) {
    try {
      final String fileName = file.toPath().getFileName().toString();
      final URL fileURL = file.toURI().toURL();
      return new Object[] {fileName, fileURL};
    } catch (final MalformedURLException e) {
      throw new RuntimeException("Problem reading spec file " + file.getAbsolutePath(), e);
    }
  }

  private void jsonRPCCall(final URL specFile) throws IOException {
    final String json = Resources.toString(specFile, StandardCharsets.UTF_8);
    final ObjectNode specNode = (ObjectNode) objectMapper.readTree(json);
    final String rawRequestBody = specNode.get("request").toString();

    final RequestBody requestBody = RequestBody.create(rawRequestBody, JSON);
    final Request request = new Request.Builder().post(requestBody).url(baseUrl).build();

    try (final Response resp = client.newCall(request).execute()) {
      final int expectedStatusCode = specNode.get("statusCode").asInt();
      assertThat(resp.code()).isEqualTo(expectedStatusCode);

      final String responseString = Objects.requireNonNull(resp.body()).string();
      final JsonNode actualResponseNode = objectMapper.readTree(responseString);

      if (UPDATE_SPECS) {
        updateSpecFile(specFile, specNode, actualResponseNode);
        return;
      }

      final JsonNode expectedResponse = specNode.get("response");
      if (expectedResponse.isObject()) {
        try {
          checkResponse(
              (ObjectNode) actualResponseNode,
              (ObjectNode) expectedResponse,
              getMethod(rawRequestBody),
              getTraceType(specFile.toString()));
        } catch (final Exception e) {
          throw new RuntimeException("Unable to parse response as json object", e);
        }
      } else if (expectedResponse.isArray()) {
        final ArrayNode responseBody;
        try {
          responseBody = (ArrayNode) actualResponseNode;
        } catch (final Exception e) {
          throw new RuntimeException("Unable to parse response as json Array", e);
        }
        for (int i = 0; i < ((ArrayNode) expectedResponse).size(); i++) {
          checkResponse(
              (ObjectNode) responseBody.get(i),
              (ObjectNode) ((ArrayNode) expectedResponse).get(i),
              getMethod(rawRequestBody),
              getTraceType(specFile.toString()));
        }
      }
    }
  }

  private void updateSpecFile(
      final URL specFile, final ObjectNode specNode, final JsonNode actualResponse)
      throws IOException {
    final Path sourcePath = resolveSourcePath(specFile);
    if (sourcePath == null) {
      return;
    }
    // Only update if response actually differs
    final JsonNode originalResponse = specNode.get("response");
    if (originalResponse != null && originalResponse.equals(actualResponse)) {
      return;
    }
    // Targeted replacement: find leaf value differences and replace in original text
    final String originalText = Files.readString(sourcePath, StandardCharsets.UTF_8);
    final List<String[]> replacements = new ArrayList<>();
    collectLeafDifferences(originalResponse, actualResponse, replacements);
    if (!replacements.isEmpty()) {
      String updatedText = originalText;
      for (final String[] pair : replacements) {
        updatedText = updatedText.replace(pair[0], pair[1]);
      }
      Files.writeString(sourcePath, updatedText, StandardCharsets.UTF_8);
    }
  }

  private void collectLeafDifferences(
      final JsonNode original, final JsonNode actual, final List<String[]> diffs) {
    if (original == null || actual == null) {
      return;
    }
    if (original.isObject() && actual.isObject()) {
      final var fields = original.fieldNames();
      while (fields.hasNext()) {
        final String field = fields.next();
        if (actual.has(field)) {
          collectLeafDifferences(original.get(field), actual.get(field), diffs);
        }
      }
    } else if (original.isArray() && actual.isArray()) {
      for (int i = 0; i < Math.min(original.size(), actual.size()); i++) {
        collectLeafDifferences(original.get(i), actual.get(i), diffs);
      }
    } else if (original.isValueNode() && actual.isValueNode() && !original.equals(actual)) {
      // Skip differences that would be normalized to the same value during comparison
      if (original.isTextual() && actual.isTextual()) {
        final String normalizedOld = normalizeSpecificErrors(original.asText());
        final String normalizedNew = normalizeSpecificErrors(actual.asText());
        if (normalizedOld.equals(normalizedNew)) {
          return;
        }
      }
      diffs.add(new String[] {original.toString(), actual.toString()});
    }
  }

  private Path resolveSourcePath(final URL specFile) {
    try {
      final Path buildPath = Paths.get(specFile.toURI());
      final String pathStr = buildPath.toString();
      // Convert build/resources/test/ path to src/test/resources/ path
      final int buildIdx = pathStr.indexOf("/build/resources/test/");
      if (buildIdx >= 0) {
        final String moduleRoot = pathStr.substring(0, buildIdx);
        final String resourcePath = pathStr.substring(buildIdx + "/build/resources/test/".length());
        return Paths.get(moduleRoot, "src", "test", "resources", resourcePath);
      }
      return null;
    } catch (final URISyntaxException e) {
      return null;
    }
  }

  private enum Method {
    TRACE_CALL_MANY,
    OTHER
  }

  private enum TraceType {
    TRACE,
    VM_TRACE,
    STATE_DIFF,
    OTHER
  }

  private Method getMethod(final String rawRequest) {
    if (rawRequest.contains("\"method\":\"trace_callMany\"")) {
      return Method.TRACE_CALL_MANY;
    } else {
      return Method.OTHER;
    }
  }

  private TraceType getTraceType(final String fileName) {
    if (fileName.contains("_trace")) {
      return TraceType.TRACE;
    } else if (fileName.contains("_vmTrace")) {
      return TraceType.VM_TRACE;
    } else if (fileName.contains("_stateDiff")) {
      return TraceType.STATE_DIFF;
    } else {
      return TraceType.OTHER;
    }
  }

  private void checkResponse(
      final ObjectNode responseBody,
      final ObjectNode expectedResponse,
      final Method method,
      final TraceType traceType)
      throws JsonProcessingException {
    // Check id
    final String actualId = responseBody.get("id").toString();
    final String expectedId = expectedResponse.get("id").toString();
    assertThat(actualId).isEqualTo(expectedId);

    // Check version
    final String actualVersion = responseBody.get("jsonrpc").toString();
    final String expectedVersion = expectedResponse.get("jsonrpc").toString();
    assertThat(actualVersion).isEqualTo(expectedVersion);

    // Check result
    if (expectedResponse.has("result")) {
      assertThat(responseBody.has("result")).isTrue();

      String expectedResult = expectedResponse.get("result").toString();
      String actualResult = responseBody.get("result").toString();

      if (method.equals(Method.TRACE_CALL_MANY)) {
        // TODO: There are differences in gas cost (causing different balances as well). These are
        // caused by
        // OpenEthereum not implementing "Istanbul" correctly. Specially the SSTORE to dirty storage
        // Slots should cost 800 gas rather than 5000
        // This should be fixed, e.g. by using
        // Erigon to
        // create the expected output, or by making OpenEthereum work correctly.
        switch (traceType) {
          case TRACE:
            expectedResult = filterStringTrace(expectedResult);
            actualResult = filterStringTrace(actualResult);
            break;
          case VM_TRACE:
            expectedResult = filterStringVmTrace(expectedResult);
            actualResult = filterStringVmTrace(actualResult);
            break;
          case STATE_DIFF:
            expectedResult = filterStringStateDiff(expectedResult);
            actualResult = filterStringStateDiff(actualResult);
            break;
          default:
            throw new RuntimeException(
                "Unrecognized trace type (expected trace | vmTrace | stateDiff)");
        }
      }

      // Apply error message normalization to both expected and actual results
      expectedResult = normalizeErrorMessages(expectedResult);
      actualResult = normalizeErrorMessages(actualResult);

      assertThat(
              prettyObjectMapper
                  .writerWithDefaultPrettyPrinter()
                  .withoutAttribute("creationMethod")
                  .writeValueAsString(prettyObjectMapper.readTree(actualResult)))
          .isEqualTo(
              prettyObjectMapper
                  .writerWithDefaultPrettyPrinter()
                  .withoutAttribute("creationMethod")
                  .writeValueAsString(prettyObjectMapper.readTree(expectedResult)));
    }

    // Check error
    if (expectedResponse.has("error")) {
      assertThat(responseBody.has("error")).isTrue();
      final String expectedError = expectedResponse.get("error").toString();
      final String actualError = responseBody.get("error").toString();
      assertThat(actualError).isEqualToIgnoringWhitespace(expectedError);
    }
  }

  private String filterStringStateDiff(final String expectedResult) {
    final Matcher m = GAS_MATCH_FOR_STATE_DIFF.matcher(expectedResult);
    return m.replaceAll("");
  }

  private String filterStringVmTrace(final String expectedResult) {
    final Matcher m = GAS_MATCH_FOR_VM_TRACE.matcher(expectedResult);
    return m.replaceAll("");
  }

  private String filterStringTrace(final String expectedResult) {
    final Matcher m = GAS_MATCH_FOR_TRACE.matcher(expectedResult);
    return m.replaceAll("");
  }

  private String normalizeErrorMessages(final String jsonString) {
    final Matcher matcher = ERROR_MESSAGE_NORMALIZATION.matcher(jsonString);
    StringBuilder result = new StringBuilder();

    while (matcher.find()) {
      String errorMessage = matcher.group(1);
      String normalizedError = normalizeSpecificErrors(errorMessage);
      matcher.appendReplacement(result, "\"error\":\"" + normalizedError + "\"");
    }
    matcher.appendTail(result);

    return result.toString();
  }

  private String normalizeSpecificErrors(final String errorMessage) {
    if (errorMessage == null) {
      return null;
    }

    // Convert to lowercase for case-insensitive matching
    String lowerError = errorMessage.toLowerCase(Locale.ROOT);

    // Normalize precompile input length errors
    if (lowerError.contains("invalid input length")) {
      return "invalid input length";
    }

    // Normalize gas-related errors
    if (lowerError.contains("out of gas") || lowerError.contains("insufficient gas")) {
      return "out of gas";
    }

    // Normalize stack errors - case insensitive
    if (lowerError.contains("stack underflow")) {
      return "stack underflow";
    }

    if (lowerError.contains("stack limit") || lowerError.contains("stack overflow")) {
      return "stack limit reached";
    }

    // Normalize invalid opcode errors
    if (lowerError.contains("invalid opcode")) {
      return "invalid opcode";
    }

    // Normalize write protection errors
    if (lowerError.contains("write protection") || lowerError.contains("illegal state change")) {
      return "write protection";
    }

    // Normalize jump destination errors
    if (lowerError.contains("invalid jump destination")
        || lowerError.contains("bad jump destination")) {
      return "invalid jump destination";
    }

    // Normalize precompile failures
    if (lowerError.contains("precompile") && lowerError.contains("fail")) {
      return "precompile failed";
    }

    // Add more normalizations as needed
    return errorMessage;
  }
}
