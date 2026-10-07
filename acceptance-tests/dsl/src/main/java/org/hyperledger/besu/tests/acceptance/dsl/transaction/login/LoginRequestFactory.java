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
package org.hyperledger.besu.tests.acceptance.dsl.transaction.login;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import io.vertx.core.json.JsonObject;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class LoginRequestFactory {

  private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
  private static final String LOGIN_PATH = "/login";

  private final String contextRootUrl;
  private final OkHttpClient client;

  public LoginRequestFactory(final String contextRootUrl) {
    this(contextRootUrl, new OkHttpClient());
  }

  public LoginRequestFactory(final String contextRootUrl, final OkHttpClient client) {
    this.contextRootUrl = contextRootUrl;
    this.client = client;
  }

  JsonObject successful(final String username, final String password) throws IOException {
    final Request request = loginRequest(username, password);

    try (final Response response = client.newCall(request).execute()) {
      assertThat(response).isNotNull();
      assertThat(response.code()).isEqualTo(200);
      assertThat(response.message()).isEqualTo("OK");

      final ResponseBody responseBody = response.body();
      assertThat(responseBody).isNotNull();
      assertThat(responseBody.contentType()).isNotNull();
      assertThat(responseBody.contentType().type()).isEqualTo(JSON.type());
      assertThat(responseBody.contentType().subtype()).isEqualTo(JSON.subtype());

      final String bodyString = responseBody.string();
      assertThat(bodyString).isNotNull();
      assertThat(bodyString).isNotBlank();

      return new JsonObject(bodyString);
    }
  }

  String send(final String username, final String password) throws IOException {
    final Request request = loginRequest(username, password);

    try (final Response response = client.newCall(request).execute()) {
      assertThat(response).isNotNull();
      assertThat(response.message()).isNotNull();
      return response.message();
    }
  }

  private Request loginRequest(final String username, final String password) {
    final RequestBody requestBody =
        RequestBody.create(
            String.format("{\"username\":\"%s\",\"password\":\"%s\"}", username, password), JSON);
    return new Request.Builder().post(requestBody).url(loginUri()).build();
  }

  private String loginUri() {
    return contextRootUrl + LOGIN_PATH;
  }
}
