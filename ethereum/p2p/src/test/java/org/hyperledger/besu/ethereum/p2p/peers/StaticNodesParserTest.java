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
package org.hyperledger.besu.ethereum.p2p.peers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.plugin.data.EnodeURL;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import io.vertx.core.json.DecodeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class StaticNodesParserTest {

  // NOTE: The invalid_static_nodes file is identical to the valid, however one node's port is set
  // to "A".

  // First peer in the valid_static_nodes file.
  private final List<EnodeURL> validFileItems =
      Lists.newArrayList(
          EnodeURLImpl.builder()
              .nodeId(
                  "50203c6bfca6874370e71aecc8958529fd723feb05013dc1abca8fc1fff845c5259faba05852e9dfe5ce172a7d6e7c2a3a5eaa8b541c8af15ea5518bbff5f2fa")
              .ipAddress("127.0.0.1")
              .useDefaultPorts()
              .build(),
          EnodeURLImpl.builder()
              .nodeId(
                  "02beb46bc17227616be44234071dfa18516684e45eed88049190b6cb56b0bae218f045fd0450f123b8f55c60b96b78c45e8e478004293a8de6818aa4e02eff97")
              .ipAddress("127.0.0.1")
              .discoveryAndListeningPorts(30304)
              .build(),
          EnodeURLImpl.builder()
              .nodeId(
                  "819e5cbd81f123516b10f04bf620daa2b385efef06d77253148b814bf1bb6197ff58ebd1fd7bf5dc765b49a4440c733bf941e479c800173f2bfeb887e4fbcbc2")
              .ipAddress("10.0.0.1")
              .discoveryAndListeningPorts(30305)
              .build(),
          EnodeURLImpl.builder()
              .nodeId(
                  "6cf53e25d2a98a22e7e205a86bda7077e3c8a7bc99e5ff88ddfd2037a550969ab566f069ffa455df0cfae0c21f7aec3447e414eccc473a3e8b20984b90f164ac")
              .ipAddress("127.0.0.1")
              .discoveryAndListeningPorts(30306)
              .build());

  @TempDir private Path testFolder;

  @Test
  public void validFileLoadsWithExpectedEnodes() throws IOException, URISyntaxException {
    final URL resource = StaticNodesParserTest.class.getResource("valid_static_nodes.json");
    final File validFile = new File(resource.getFile());
    final Set<EnodeURLImpl> enodes =
        StaticNodesParser.fromPath(validFile.toPath(), EnodeDnsConfiguration.DEFAULT_CONFIG);

    assertThat(enodes)
        .containsExactlyInAnyOrder(validFileItems.toArray(new EnodeURLImpl[validFileItems.size()]));
  }

  @Test
  public void validFileLoadsWithExpectedEnodesWhenDnsEnabled()
      throws IOException, URISyntaxException {
    final URL resource =
        StaticNodesParserTest.class.getResource("valid_hostname_static_nodes.json");
    final File validFile = new File(resource.getFile());
    final EnodeDnsConfiguration enodeDnsConfiguration =
        ImmutableEnodeDnsConfiguration.builder().dnsEnabled(true).updateEnabled(false).build();
    final Set<EnodeURLImpl> enodes =
        StaticNodesParser.fromPath(validFile.toPath(), enodeDnsConfiguration);

    assertThat(enodes)
        .containsExactlyInAnyOrder(validFileItems.toArray(new EnodeURLImpl[validFileItems.size()]));
  }

  @Test
  public void fileWithHostnameThrowsAnExceptionWhenDnsDisabled()
      throws IOException, URISyntaxException {
    final URL resource =
        StaticNodesParserTest.class.getResource("valid_hostname_static_nodes.json");
    final File invalidFile = new File(resource.getFile());

    assertThatThrownBy(
            () ->
                StaticNodesParser.fromPath(
                    invalidFile.toPath(), EnodeDnsConfiguration.DEFAULT_CONFIG))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void fileWithUnknownHostnameNotThrowsAnExceptionWhenDnsAndUpdateEnabled()
      throws IOException, URISyntaxException {
    final URL resource =
        StaticNodesParserTest.class.getResource("unknown_hostname_static_nodes.json");
    final File validFile = new File(resource.getFile());
    final EnodeDnsConfiguration enodeDnsConfiguration =
        ImmutableEnodeDnsConfiguration.builder().dnsEnabled(true).updateEnabled(true).build();
    final Set<EnodeURLImpl> enodes =
        StaticNodesParser.fromPath(validFile.toPath(), enodeDnsConfiguration);

    assertThat(enodes)
        .containsExactlyInAnyOrder(validFileItems.toArray(new EnodeURLImpl[validFileItems.size()]));
    final Set<EnodeURLImpl> actual =
        StaticNodesParser.fromPath(validFile.toPath(), enodeDnsConfiguration);

    final EnodeURLImpl[] expected = validFileItems.toArray(new EnodeURLImpl[validFileItems.size()]);

    assertThat(actual).containsExactlyInAnyOrder(expected);
  }

  @Test
  public void fileWithUnknownHostnameThrowsAnExceptionWhenOnlyDnsEnabled()
      throws IOException, URISyntaxException {
    final URL resource =
        StaticNodesParserTest.class.getResource("unknown_hostname_static_nodes.json");
    final File invalidFile = new File(resource.getFile());
    final EnodeDnsConfiguration enodeDnsConfiguration =
        ImmutableEnodeDnsConfiguration.builder().dnsEnabled(true).updateEnabled(false).build();

    assertThatThrownBy(
            () -> StaticNodesParser.fromPath(invalidFile.toPath(), enodeDnsConfiguration))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void invalidFileThrowsAnException() {
    final URL resource = StaticNodesParserTest.class.getResource("invalid_static_nodes.json");
    final File invalidFile = new File(resource.getFile());

    assertThatThrownBy(
            () ->
                StaticNodesParser.fromPath(
                    invalidFile.toPath(), EnodeDnsConfiguration.DEFAULT_CONFIG))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void fromPath_withNonListeningNodesThrowsException() {
    final URL resource =
        StaticNodesParserTest.class.getResource("invalid_static_nodes_no_listening_port.json");
    final File invalidFile = new File(resource.getFile());

    assertThatThrownBy(
            () ->
                StaticNodesParser.fromPath(
                    invalidFile.toPath(), EnodeDnsConfiguration.DEFAULT_CONFIG))
        .isInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Static node must be configured with a valid listening port.");
  }

  @Test
  public void nonJsonFileThrowsAnException() throws IOException {
    final Path tempFile = Files.createTempFile(testFolder, "file", "txt");
    Files.write(tempFile, "This Is Not Json".getBytes(Charset.forName("UTF-8")));

    assertThatThrownBy(
            () -> StaticNodesParser.fromPath(tempFile, EnodeDnsConfiguration.DEFAULT_CONFIG))
        .isInstanceOf(DecodeException.class);
  }

  @Test
  public void anEmptyCacheIsCreatedIfTheFileDoesNotExist() throws IOException {
    final Path path = Paths.get("./arbirtraryFilename.txt");

    final Set<EnodeURLImpl> enodes =
        StaticNodesParser.fromPath(path, EnodeDnsConfiguration.DEFAULT_CONFIG);
    assertThat(enodes.size()).isZero();
  }

  @Test
  public void cacheIsCreatedIfFileExistsButIsEmpty() throws IOException {
    final Path tempFile = Files.createTempFile(testFolder, "file", "txt");

    final Set<EnodeURLImpl> enodes =
        StaticNodesParser.fromPath(tempFile, EnodeDnsConfiguration.DEFAULT_CONFIG);
    assertThat(enodes.size()).isZero();
  }
}
