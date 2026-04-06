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
package org.hyperledger.besu.ethereum.p2p.discovery.dns;

import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;

import java.security.Security;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.ThreadingModel;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class DNSDaemonTest {
  private static final int EXPECTED_SEQ = 932;
  private static final String holeskyEnr =
      "enrtree://AKA3AM6LPBYEUDMVNU3BSVQJ5AD45Y7YPOHJLEF6W26QOE4VTUDPE@all.holesky.ethdisco.net";
  private final MockDnsServerVerticle mockDnsServerVerticle = new MockDnsServerVerticle();
  private DNSDaemon dnsDaemon;

  @BeforeAll
  static void setup() {
    Security.addProvider(new BouncyCastleProvider());
  }

  @BeforeEach
  @DisplayName("Deploy Mock Dns Server Verticle")
  void prepare(final Vertx vertx, final VertxTestContext vertxTestContext) {
    vertx.deployVerticle(mockDnsServerVerticle, vertxTestContext.succeedingThenComplete());
  }

  @Test
  @DisplayName("Test DNS Daemon with a mock DNS server")
  void testDNSDaemon(final Vertx vertx, final VertxTestContext testContext) {
    final Checkpoint checkpoint = testContext.checkpoint();
    dnsDaemon =
        new DNSDaemon(
            holeskyEnr,
            (seq, records) -> {
              if (seq != EXPECTED_SEQ) {
                testContext.failNow(
                    String.format(
                        "Expecting sequence to be %d in first pass but got: %d",
                        EXPECTED_SEQ, seq));
              }
              if (records.size() != 115) {
                testContext.failNow(
                    "Expecting 115 records in first pass but got: " + records.size());
              }
              records.forEach(
                  enr -> {
                    try {
                      // make sure enode url can be built from record
                      EnodeURLImpl.builder()
                          .ipAddress(enr.ip().get())
                          .nodeId(enr.publicKey())
                          .discoveryPort(enr.udp())
                          .listeningPort(enr.tcp())
                          .build();
                    } catch (final Exception e) {
                      testContext.failNow(e);
                    }
                  });
              checkpoint.flag();
            },
            0,
            1L,
            0,
            "localhost:" + mockDnsServerVerticle.port());

    final DeploymentOptions options =
        new DeploymentOptions()
            .setThreadingModel(ThreadingModel.VIRTUAL_THREAD)
            .setWorkerPoolSize(1);
    vertx.deployVerticle(dnsDaemon, options);
  }

  @Test
  @DisplayName("Test DNS Daemon with a mock DNS server with invalid ENR records")
  void invalidEnrShouldNotCrashDaemon(final Vertx vertx, final VertxTestContext testContext) {
    final Checkpoint checkpoint = testContext.checkpoint();
    dnsDaemon =
        new DNSDaemon(
            holeskyEnr,
            (seq, records) -> {
              if (seq != EXPECTED_SEQ) {
                testContext.failNow(
                    String.format(
                        "Expecting sequence to be %d in first pass but got: %d",
                        EXPECTED_SEQ, seq));
              }
              if (records.size() != 115) {
                testContext.failNow(
                    "Expecting 115 records in first pass but got: " + records.size());
              }
              records.forEach(
                  enr -> {
                    try {
                      // make sure enode url can be built from record
                      EnodeURLImpl.builder()
                          .ipAddress(enr.ip().get())
                          .nodeId(enr.publicKey())
                          .discoveryPort(enr.udp())
                          .listeningPort(enr.tcp())
                          .build();
                    } catch (final Exception e) {
                      testContext.failNow(e);
                    }
                  });
              checkpoint.flag();
            },
            0,
            1L,
            0,
            "localhost:" + mockDnsServerVerticle.port());

    mockDnsServerVerticle.addTxtRecord(
        "FDXN3SN67NA5DKA4J2GOK7BVQI.all.holesky.ethdisco.net",
        "enrtree-branch:I56MJAJBMXTZZEPBQR6HWNAH7A,I56GAAJBMXTZZEPBQR6HWNAH7A,I56HGDJBMXTZZEPBQR6HWNAH7A");

    // ENR value - invalid encoding
    mockDnsServerVerticle.addTxtRecord(
        "I56MJAJBMXTZZEPBQR6HWNAH7A.all.holesky.ethdisco.net", "enr:-Lu4QMFaK");

    // empty ENR record
    mockDnsServerVerticle.addTxtRecord(
        "I56GAAJBMXTZZEPBQR6HWNAH7A.all.holesky.ethdisco.net", "enr:");

    // invalid IP field in ENR record
    mockDnsServerVerticle.addTxtRecord(
        "I56HGDJBMXTZZEPBQR6HWNAH7A.all.holesky.ethdisco.net",
        "enr:-KS4QK1ecw-CGrDDZ4YwFrhgqctD0tWMHKJhUVxsS4um3aUFe3yBHRtVL9uYKk16DurN1IdSKTOB1zNCvjBybjZ_KAqGAYtJ5U8wg2V0a"
            + "MfGhJsZKtCAgmlkgnY0gmlwhQ_MtDn_iXNlY3AyNTZrMaEDVw-3497LHMjigYh2MteIoI9byWFnSmGR-590_KkaSHGEc25hcMCDdGNwgnZf"
            + "g3VkcIJ2Xw");

    final DeploymentOptions options =
        new DeploymentOptions()
            .setThreadingModel(ThreadingModel.VIRTUAL_THREAD)
            .setWorkerPoolSize(1);
    vertx.deployVerticle(dnsDaemon, options);
  }

  @Disabled("test is flaky see https://github.com/hyperledger/besu/issues/8373")
  @Test
  @DisplayName("Test DNS Daemon with periodic lookup to a mock DNS server")
  void testDNSDaemonPeriodic(final Vertx vertx, final VertxTestContext testContext)
      throws InterruptedException {
    // checkpoint should be flagged twice
    final Checkpoint checkpoint = testContext.checkpoint(2);
    final AtomicInteger pass = new AtomicInteger(0);
    dnsDaemon =
        new DNSDaemon(
            holeskyEnr,
            (seq, records) -> {
              switch (pass.incrementAndGet()) {
                case 1:
                  if (seq != EXPECTED_SEQ) {
                    testContext.failNow(
                        String.format(
                            "Expecting sequence to be %d in first pass but got: %d",
                            EXPECTED_SEQ, seq));
                  }
                  if (records.size() != 115) {
                    testContext.failNow(
                        "Expecting 115 records in first pass but got: " + records.size());
                  }
                  break;
                case 2:
                  if (seq != EXPECTED_SEQ) {
                    testContext.failNow(
                        String.format(
                            "Expecting sequence to be %d in second pass but got: %d",
                            EXPECTED_SEQ, seq));
                  }
                  if (!records.isEmpty()) {
                    testContext.failNow(
                        "Expecting 0 records in second pass but got: " + records.size());
                  }
                  break;
                default:
                  testContext.failNow("Third pass is not expected");
              }
              checkpoint.flag();
            },
            0,
            1, // initial delay
            3000, // second lookup after 3 seconds (the thread scheduling can be slower in CI)
            "localhost:" + mockDnsServerVerticle.port());

    final DeploymentOptions options =
        new DeploymentOptions()
            .setThreadingModel(ThreadingModel.VIRTUAL_THREAD)
            .setWorkerPoolSize(1);
    vertx.deployVerticle(dnsDaemon, options);
  }
}
