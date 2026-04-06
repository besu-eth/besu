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
package org.hyperledger.besu.cli.options;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.cli.CommandTestAbstract;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.permissioning.LocalPermissioningConfiguration;
import org.hyperledger.besu.ethereum.permissioning.PermissioningConfiguration;
import org.hyperledger.besu.plugin.data.EnodeURL;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PermissionsOptionsTest extends CommandTestAbstract {
  private static final String PERMISSIONING_CONFIG_TOML = "/permissioning_config.toml";

  @Test
  public void errorIsRaisedIfStaticNodesAreNotAllowed(final @TempDir Path testFolder)
      throws IOException {
    final Path staticNodesFile = testFolder.resolve("static-nodes.json");
    final Path permissioningConfig = testFolder.resolve("permissioning.json");

    final EnodeURL staticNodeURI =
        EnodeURLImpl.builder()
            .nodeId(
                "50203c6bfca6874370e71aecc8958529fd723feb05013dc1abca8fc1fff845c5259faba05852e9dfe5ce172a7d6e7c2a3a5eaa8b541c8af15ea5518bbff5f2fa")
            .ipAddress("127.0.0.1")
            .useDefaultPorts()
            .build();

    final EnodeURL allowedNode =
        EnodeURLImpl.builder()
            .nodeId(
                "50203c6bfca6874370e71aecc8958529fd723feb05013dc1abca8fc1fff845c5259faba05852e9dfe5ce172a7d6e7c2a3a5eaa8b541c8af15ea5518bbff5f2fa")
            .useDefaultPorts()
            .ipAddress("127.0.0.1")
            .listeningPort(30304)
            .build();

    Files.write(staticNodesFile, ("[\"" + staticNodeURI.toString() + "\"]").getBytes(UTF_8));
    Files.write(
        permissioningConfig,
        ("nodes-allowlist=[\"" + allowedNode.toString() + "\"]").getBytes(UTF_8));

    parseCommand(
        "--data-path=" + testFolder,
        "--bootnodes",
        "--permissions-nodes-config-file-enabled=true",
        "--permissions-nodes-config-file=" + permissioningConfig);
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(staticNodeURI.toString(), "not in nodes-allowlist");
  }

  @Test
  public void nodePermissioningTomlPathWithoutOptionMustDisplayUsage() {
    parseCommand("--permissions-nodes-config-file");

    Mockito.verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandErrorOutput.toString(UTF_8))
        .startsWith("Missing required parameter for option '--permissions-nodes-config-file'");
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void accountPermissioningTomlPathWithoutOptionMustDisplayUsage() {
    parseCommand("--permissions-accounts-config-file");

    Mockito.verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandErrorOutput.toString(UTF_8))
        .startsWith("Missing required parameter for option '--permissions-accounts-config-file'");
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void nodePermissioningEnabledWithNonexistentConfigFileMustError() {
    parseCommand(
        "--permissions-nodes-config-file-enabled",
        "--permissions-nodes-config-file",
        "file-does-not-exist");

    Mockito.verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandErrorOutput.toString(UTF_8)).contains("Configuration file does not exist");
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void accountPermissioningEnabledWithNonexistentConfigFileMustError() {
    parseCommand(
        "--permissions-accounts-config-file-enabled",
        "--permissions-accounts-config-file",
        "file-does-not-exist");

    Mockito.verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandErrorOutput.toString(UTF_8)).contains("Configuration file does not exist");
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void nodePermissioningTomlFileWithNoPermissionsEnabledMustNotError() throws IOException {

    final URL configFile = this.getClass().getResource(PERMISSIONING_CONFIG_TOML);
    final Path permToml = createTempFile("toml", Resources.toByteArray(configFile));
    parseCommand("--permissions-nodes-config-file", permToml.toString());

    verify(mockRunnerBuilder).build();

    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void accountPermissioningTomlFileWithNoPermissionsEnabledMustNotError()
      throws IOException {

    final URL configFile = this.getClass().getResource(PERMISSIONING_CONFIG_TOML);
    final Path permToml = createTempFile("toml", Resources.toByteArray(configFile));
    parseCommand("--permissions-accounts-config-file", permToml.toString());

    verify(mockRunnerBuilder).build();

    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void defaultPermissionsTomlFileWithNoPermissionsEnabledMustNotError() {
    parseCommand("--p2p-enabled", "false");

    verify(mockRunnerBuilder).build();

    assertThat(commandErrorOutput.toString(UTF_8)).doesNotContain("no permissions enabled");
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void nodePermissioningTomlPathMustUseOption() throws IOException {
    final List<EnodeURLImpl> allowedNodes =
        Lists.newArrayList(
            EnodeURLImpl.fromString(
                "enode://6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@192.168.0.9:4567"),
            EnodeURLImpl.fromString(
                "enode://6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@192.169.0.9:4568"));

    final URL configFile = this.getClass().getResource(PERMISSIONING_CONFIG_TOML);
    final Path permToml = createTempFile("toml", Resources.toByteArray(configFile));

    final String allowedNodesString =
        allowedNodes.stream().map(Object::toString).collect(Collectors.joining(","));
    parseCommand(
        "--permissions-nodes-config-file-enabled",
        "--permissions-nodes-config-file",
        permToml.toString(),
        "--bootnodes",
        allowedNodesString);
    final LocalPermissioningConfiguration localPermissioningConfiguration =
        LocalPermissioningConfiguration.createDefault();
    localPermissioningConfiguration.setNodePermissioningConfigFilePath(permToml.toString());
    localPermissioningConfiguration.setNodeAllowlist(allowedNodes);

    verify(mockRunnerBuilder)
        .permissioningConfiguration(permissioningConfigurationArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    final PermissioningConfiguration config =
        permissioningConfigurationArgumentCaptor.getValue().get();
    assertThat(config.getLocalConfig().get())
        .usingRecursiveComparison()
        .isEqualTo(localPermissioningConfiguration);

    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void accountPermissioningTomlPathMustUseOption() throws IOException {

    final URL configFile = this.getClass().getResource(PERMISSIONING_CONFIG_TOML);
    final Path permToml = createTempFile("toml", Resources.toByteArray(configFile));

    parseCommand(
        "--permissions-accounts-config-file-enabled",
        "--permissions-accounts-config-file",
        permToml.toString());
    final LocalPermissioningConfiguration localPermissioningConfiguration =
        LocalPermissioningConfiguration.createDefault();
    localPermissioningConfiguration.setAccountPermissioningConfigFilePath(permToml.toString());
    localPermissioningConfiguration.setAccountAllowlist(
        Collections.singletonList("0x0000000000000000000000000000000000000009"));

    verify(mockRunnerBuilder)
        .permissioningConfiguration(permissioningConfigurationArgumentCaptor.capture());
    final PermissioningConfiguration permissioningConfiguration =
        permissioningConfigurationArgumentCaptor.getValue().get();
    assertThat(permissioningConfiguration.getLocalConfig()).isPresent();

    final LocalPermissioningConfiguration effectiveLocalPermissioningConfig =
        permissioningConfiguration.getLocalConfig().get();
    assertThat(effectiveLocalPermissioningConfig.isAccountAllowlistEnabled()).isTrue();
    assertThat(effectiveLocalPermissioningConfig.getAccountPermissioningConfigFilePath())
        .isEqualTo(permToml.toString());

    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
  }
}
