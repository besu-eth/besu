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
package org.hyperledger.besu.cli.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.cli.util.DevnetConfigResolver.DevnetConfig;
import org.hyperledger.besu.cli.util.DevnetConfigResolver.DevnetConfigResolutionException;
import org.hyperledger.besu.datatypes.Hash;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DevnetConfigResolverTest {

  private static final String DEVNET = "glamsterdam-devnet-11";
  private static final String METADATA =
      "/glamsterdam-devnets/master/network-configs/devnet-11/metadata/";
  private static final String GENESIS = "{\"config\":{\"chainId\":701419869}}";
  private static final String ENODE =
      "enode://0fcfecff57e5200d31e23f057217e6a81e340c0b9645f7abc31f8560184615a1ed0f86221f8111031de977d22fca7d2bb124dbf2b41798e8a3e38887ea502617@188.166.23.5:30303?discport=30303";
  private static final String ENR =
      "enr:-Iu4QD3wy38adtNiGE3jc02YEDE4grbxiOc7snmWw8ILlKOcOkViIOtAIGXuf7E-zVh3y1h-RZhp3h_lRPa5mIIr9lqAgmlkgnY0gmlwhLymFwWJc2VjcDI1NmsxoQMPz-z_V-UgDTHiPwVyF-aoHjQMC5ZF96vDH4VgGEYVoYN0Y3CCdl-DdWRwgnZf";
  private static final String GENESIS_HASH =
      "0x84a7fe7db58879db376a354b3cde27d94990e5dfb5c64bdb73172b91079347b4";

  private final Map<String, String> files = new ConcurrentHashMap<>();
  private final Map<String, AtomicInteger> requests = new ConcurrentHashMap<>();
  private HttpServer server;
  private String baseUrl;

  @TempDir private Path cacheDir;

  @BeforeEach
  void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext(
        "/",
        exchange -> {
          final String path = exchange.getRequestURI().getPath();
          requests.computeIfAbsent(path, p -> new AtomicInteger()).incrementAndGet();
          final String body = files.get(path);
          if (body == null) {
            exchange.sendResponseHeaders(404, -1);
          } else {
            final byte[] bytes = body.getBytes(UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
          }
          exchange.close();
        });
    server.start();
    baseUrl = "http://localhost:" + server.getAddress().getPort();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  @Test
  void mapsNameToRepositoryAndDirectory() {
    assertThat(DevnetConfigResolver.metadataUri(DEVNET, "https://example.com/ethpandaops/"))
        .hasToString(
            "https://example.com/ethpandaops/glamsterdam-devnets/master/network-configs/devnet-11/metadata/");
    assertThat(DevnetConfigResolver.metadataUri("fusaka-sepsf-0", "https://example.com"))
        .hasToString("https://example.com/fusaka-devnets/master/network-configs/sepsf-0/metadata/");
  }

  @Test
  void rejectsInvalidNames() {
    for (final String name :
        new String[] {"glamsterdam", "../devnet-1", "Foo-Bar", "a-..", "a-b/c"}) {
      assertThatThrownBy(() -> DevnetConfigResolver.metadataUri(name, baseUrl))
          .describedAs(name)
          .isInstanceOf(DevnetConfigResolutionException.class)
          .hasMessageContaining("Invalid devnet name");
    }
  }

  @Test
  void downloadsGenesisBootnodesAndGenesisHash() throws IOException {
    serveDevnet();

    final DevnetConfig config = DevnetConfigResolver.resolve(DEVNET, cacheDir, baseUrl);

    assertThat(config.genesisFile()).isEqualTo(cacheDir.resolve("genesis.json"));
    assertThat(Files.readString(config.genesisFile())).isEqualTo(GENESIS);
    assertThat(config.bootnodes()).containsExactly(ENODE, ENR);
    assertThat(config.expectedGenesisHash()).contains(Hash.fromHexString(GENESIS_HASH));
  }

  @Test
  void keepsCachedGenesisButRefreshesBootnodes() throws IOException {
    serveDevnet();
    DevnetConfigResolver.resolve(DEVNET, cacheDir, baseUrl);

    files.put(METADATA + "genesis.json", "{\"config\":{\"chainId\":1}}");
    files.put(
        METADATA + "enodes.txt", "# refreshed\n\n" + ENODE.replace("188.166.23.5", "10.0.0.1"));
    final DevnetConfig config = DevnetConfigResolver.resolve(DEVNET, cacheDir, baseUrl);

    assertThat(Files.readString(config.genesisFile())).isEqualTo(GENESIS);
    assertThat(requests.get(METADATA + "genesis.json")).hasValue(1);
    assertThat(config.bootnodes()).containsExactly(ENODE.replace("188.166.23.5", "10.0.0.1"), ENR);
  }

  @Test
  void usesCachedBootnodesWhenServerIsUnreachable() {
    serveDevnet();
    DevnetConfigResolver.resolve(DEVNET, cacheDir, baseUrl);
    server.stop(0);

    final DevnetConfig config = DevnetConfigResolver.resolve(DEVNET, cacheDir, baseUrl);

    assertThat(config.bootnodes()).containsExactly(ENODE, ENR);
    assertThat(config.expectedGenesisHash()).contains(Hash.fromHexString(GENESIS_HASH));
  }

  @Test
  void failsWhenServerIsUnreachableAndNothingIsCached() {
    server.stop(0);

    assertThatThrownBy(() -> DevnetConfigResolver.resolve(DEVNET, cacheDir, baseUrl))
        .isInstanceOf(DevnetConfigResolutionException.class)
        .hasMessageContaining("Failed to resolve devnet 'glamsterdam-devnet-11'");
  }

  @Test
  void toleratesMissingOptionalFiles() {
    files.put(METADATA + "genesis.json", GENESIS);
    files.put(METADATA + "enodes.txt", ENODE + "\n");

    final DevnetConfig config = DevnetConfigResolver.resolve(DEVNET, cacheDir, baseUrl);

    assertThat(config.bootnodes()).containsExactly(ENODE);
    assertThat(config.expectedGenesisHash()).isEmpty();
  }

  @Test
  void failsForUnknownDevnet() {
    assertThatThrownBy(() -> DevnetConfigResolver.resolve(DEVNET, cacheDir, baseUrl))
        .isInstanceOf(DevnetConfigResolutionException.class)
        .hasMessageContaining("Unknown devnet 'glamsterdam-devnet-11'");
    assertThat(cacheDir.resolve("genesis.json")).doesNotExist();
  }

  private void serveDevnet() {
    files.put(METADATA + "genesis.json", GENESIS);
    files.put(METADATA + "enodes.txt", ENODE + "\n");
    files.put(METADATA + "el_enrs.txt", ENR + "\n");
    files.put(METADATA + "deposit_contract_block_hash.txt", GENESIS_HASH + "\n");
  }
}
