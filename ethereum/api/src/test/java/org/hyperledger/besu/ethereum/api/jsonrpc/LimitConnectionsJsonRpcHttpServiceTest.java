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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import okhttp3.OkHttpClient;
import okhttp3.Response;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class LimitConnectionsJsonRpcHttpServiceTest extends JsonRpcHttpServiceTestBase {

  @BeforeAll
  public static void initLimitedServerAndClient() throws Exception {
    maxConnections = 2;
    initServerAndClient();
  }

  @Test
  public void limitActiveConnections() throws Exception {
    final List<OkHttpClient> heldClients = new ArrayList<>();
    for (int i = 0; i < maxConnections; i++) {
      // create a new client for each request because we want to test the limit
      final OkHttpClient newClient = new OkHttpClient();
      heldClients.add(newClient);
      try (final Response resp = newClient.newCall(buildGetRequest("/readiness")).execute()) {
        assertThat(resp.code()).isEqualTo(200);
      }
    }
    // keep clients reachable so their pooled connections stay open for the limit check
    assertThat(heldClients).hasSize(maxConnections);

    // rejected connections must not inflate the active connection count
    final int countRejections = 2;
    for (int i = 0; i < countRejections; i++) {
      final OkHttpClient rejectedClient = new OkHttpClient();
      // end of stream gets wrapped locally by ConnectException with message "Connection refused"
      // but in CI it comes through as is
      assertThatThrownBy(() -> rejectedClient.newCall(buildGetRequest("/readiness")).execute())
          .isInstanceOf(IOException.class)
          .hasStackTraceContaining("unexpected end of stream");
    }

    assertThat(metricsSystem.getGaugeValue("active_http_connection_count"))
        .as("rejected connections should not increase the active connection count")
        .isEqualTo(maxConnections);

    // retain until after the gauge assert so connections are not GC'd mid-test
    assertThat(heldClients).hasSize(maxConnections);
  }
}
