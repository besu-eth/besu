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
package org.hyperledger.besu.ethereum.api.graphql;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import com.google.common.io.Resources;
import io.vertx.core.json.JsonObject;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class EthGraphQLHttpBySpecTest extends AbstractEthGraphQLHttpServiceTest {

  @SuppressWarnings("StreamResourceLeak")
  public static Stream<Arguments> specs() throws IOException, URISyntaxException {
    final URL url =
        EthGraphQLHttpBySpecTest.class.getResource(
            "/org/hyperledger/besu/ethereum/api/graphql/eth_blockNumber.json");
    checkState(url != null, "Cannot find test directory org/hyperledger/besu/ethereum/api/graphql");
    final Path dir = Paths.get(url.toURI()).getParent();
    return Files.list(dir)
        .map(Path::getFileName)
        .map(Path::toString)
        .filter(p -> p.endsWith(".json"))
        .filter(p -> !p.contains("genesis"))
        .map(Arguments::of);
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("specs")
  void graphQLCallWithSpecFile(final String specFileName) throws Exception {
    graphQLCall(specFileName);
  }

  private void graphQLCall(final String name) throws IOException {
    final String json =
        Resources.toString(
            EthGraphQLHttpBySpecTest.class.getResource(name), StandardCharsets.UTF_8);
    final JsonObject spec = new JsonObject(json);
    final String rawRequestBody = spec.getString("request");
    final String rawVariables = spec.getString("variables");
    final RequestBody requestBody =
        rawVariables == null
            ? RequestBody.create(rawRequestBody, GRAPHQL)
            : RequestBody.create(
                "{ \"query\":\"" + rawRequestBody + "\", \"variables\": " + rawVariables + "}",
                JSON);
    final Request request = new Request.Builder().post(requestBody).url(baseUrl).build();

    try (final Response resp = client.newCall(request).execute()) {
      final JsonObject expectedRespBody = spec.getJsonObject("response");
      final String resultStr = resp.body().string();

      final JsonObject result = new JsonObject(resultStr);
      Assertions.assertThat(result).isEqualTo(expectedRespBody);

      final int expectedStatusCode = spec.getInteger("statusCode");
      Assertions.assertThat(resp.code()).isEqualTo(expectedStatusCode);
    }
  }
}
