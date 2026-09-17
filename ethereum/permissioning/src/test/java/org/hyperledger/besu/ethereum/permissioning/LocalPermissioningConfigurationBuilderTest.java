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
package org.hyperledger.besu.ethereum.permissioning;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.hyperledger.besu.ethereum.p2p.peers.EnodeDnsConfiguration.dnsDisabled;

import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.ImmutableEnodeDnsConfiguration;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.common.io.Resources;
import org.junit.jupiter.api.Test;

public class LocalPermissioningConfigurationBuilderTest {

  private static final String PERMISSIONING_CONFIG_VALID = "/permissioning_config.toml";

  private static final String PERMISSIONING_CONFIG_ACCOUNT_ALLOWLIST_ONLY =
      "/permissioning_config_account_allowlist_only.toml";
  private static final String PERMISSIONING_CONFIG_NODE_ALLOWLIST_ONLY =
      "/permissioning_config_node_allowlist_only.toml";
  private static final String PERMISSIONING_CONFIG_INVALID_ENODE =
      "/permissioning_config_invalid_enode.toml";
  private static final String PERMISSIONING_CONFIG_INVALID_ACCOUNT =
      "/permissioning_config_invalid_account.toml";
  private static final String PERMISSIONING_CONFIG_EMPTY_ALLOWLISTS =
      "/permissioning_config_empty_allowlists.toml";
  private static final String PERMISSIONING_CONFIG_ABSENT_ALLOWLISTS =
      "/permissioning_config_absent_allowlists.toml";
  private static final String PERMISSIONING_CONFIG_UNRECOGNIZED_KEY =
      "/permissioning_config_unrecognized_key.toml";
  private static final String PERMISSIONING_CONFIG_NODE_ALLOWLIST_ONLY_MULTILINE =
      "/permissioning_config_node_allowlist_only_multiline.toml";
  private static final String PERMISSIONING_CONFIG_NODE_ALLOWLIST_WITH_DNS =
      "/permissioning_config_node_allowlist_with_dns.toml";
  private final String VALID_NODE_ID =
      "6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0";

  @Test
  public void permissioningConfig() throws Exception {
    final String uri = "enode://" + VALID_NODE_ID + "@192.168.0.9:4567";
    final String uri2 = "enode://" + VALID_NODE_ID + "@192.169.0.9:4568";

    final URL configFile = this.getClass().getResource(PERMISSIONING_CONFIG_VALID);
    final Path toml = createTempFile("toml", Resources.toByteArray(configFile));

    LocalPermissioningConfiguration permissioningConfiguration = permissioningConfig(toml);

    assertThat(permissioningConfiguration.isAccountAllowlistEnabled()).isTrue();
    assertThat(permissioningConfiguration.getAccountAllowlist())
        .containsExactly("0x0000000000000000000000000000000000000009");
    assertThat(permissioningConfiguration.isNodeAllowlistEnabled()).isTrue();
    assertThat(permissioningConfiguration.getNodeAllowlist())
        .containsExactly(EnodeURLImpl.fromString(uri), EnodeURLImpl.fromString(uri2));
  }

  @Test
  public void permissioningConfigWithOnlyNodeAllowlistSet() throws Exception {
    final String uri = "enode://" + VALID_NODE_ID + "@192.168.0.9:4567";

    final URL configFile = this.getClass().getResource(PERMISSIONING_CONFIG_NODE_ALLOWLIST_ONLY);
    final Path toml = createTempFile("toml", Resources.toByteArray(configFile));

    LocalPermissioningConfiguration permissioningConfiguration =
        PermissioningConfigurationBuilder.permissioningConfiguration(
            true,
            dnsDisabled(),
            toml.toAbsolutePath().toString(),
            false,
            toml.toAbsolutePath().toString());

    assertThat(permissioningConfiguration.isAccountAllowlistEnabled()).isFalse();
    assertThat(permissioningConfiguration.isNodeAllowlistEnabled()).isTrue();
    assertThat(permissioningConfiguration.getNodeAllowlist())
        .containsExactly(EnodeURLImpl.fromString(uri));
  }

  @Test
  public void permissioningConfigWithNodeAllowlistSetWithDnsEnabled() throws Exception {
    final String uri = "enode://" + VALID_NODE_ID + "@127.0.0.1:4567";

    final URL configFile =
        this.getClass().getResource(PERMISSIONING_CONFIG_NODE_ALLOWLIST_WITH_DNS);
    final Path toml = createTempFile("toml", Resources.toByteArray(configFile));

    LocalPermissioningConfiguration permissioningConfiguration =
        PermissioningConfigurationBuilder.permissioningConfiguration(
            true,
            ImmutableEnodeDnsConfiguration.builder().dnsEnabled(true).updateEnabled(false).build(),
            toml.toAbsolutePath().toString(),
            false,
            toml.toAbsolutePath().toString());

    assertThat(permissioningConfiguration.isAccountAllowlistEnabled()).isFalse();
    assertThat(permissioningConfiguration.isNodeAllowlistEnabled()).isTrue();
    assertThat(permissioningConfiguration.getNodeAllowlist())
        .containsExactly(EnodeURLImpl.fromString(uri));
  }

  @Test
  public void permissioningConfigWithNodeAllowlistSetWithDnsDisabled() throws Exception {

    final URL configFile =
        this.getClass().getResource(PERMISSIONING_CONFIG_NODE_ALLOWLIST_WITH_DNS);
    final Path toml = createTempFile("toml", Resources.toByteArray(configFile));

    assertThatThrownBy(
            () ->
                PermissioningConfigurationBuilder.permissioningConfiguration(
                    true,
                    dnsDisabled(),
                    toml.toAbsolutePath().toString(),
                    false,
                    toml.toAbsolutePath().toString()))
        .hasMessageContaining("Invalid enode URL syntax")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void permissioningConfigWithOnlyAccountAllowlistSet() throws Exception {
    final URL configFile = this.getClass().getResource(PERMISSIONING_CONFIG_ACCOUNT_ALLOWLIST_ONLY);
    final Path toml = createTempFile("toml", Resources.toByteArray(configFile));

    LocalPermissioningConfiguration permissioningConfiguration =
        PermissioningConfigurationBuilder.permissioningConfiguration(
            false,
            dnsDisabled(),
            toml.toAbsolutePath().toString(),
            true,
            toml.toAbsolutePath().toString());

    assertThat(permissioningConfiguration.isNodeAllowlistEnabled()).isFalse();
    assertThat(permissioningConfiguration.isAccountAllowlistEnabled()).isTrue();
    assertThat(permissioningConfiguration.getAccountAllowlist())
        .containsExactly("0x0000000000000000000000000000000000000009");
  }

  @Test
  public void enabledNodePermissioningRequiresAConfigFilePath() {
    assertThatThrownBy(
            () ->
                PermissioningConfigurationBuilder.permissioningConfiguration(
                    true, dnsDisabled(), null, false, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage(
            "Node permissioning config file path is required when node permissioning is enabled");
  }

  @Test
  public void enabledAccountPermissioningRequiresAConfigFilePath() {
    assertThatThrownBy(
            () ->
                PermissioningConfigurationBuilder.permissioningConfiguration(
                    false, dnsDisabled(), null, true, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage(
            "Account permissioning config file path is required when account permissioning is enabled");
  }

  @Test
  public void permissioningConfigWithInvalidAccount() throws Exception {
    final URL configFile = this.getClass().getResource(PERMISSIONING_CONFIG_INVALID_ACCOUNT);
    final Path toml = createTempFile("toml", Resources.toByteArray(configFile));

    final Throwable thrown = catchThrowable(() -> accountOnlyPermissioningConfig(toml));

    assertThat(thrown)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Invalid account 0xfoo");
  }

  @Test
  public void permissioningConfigWithInvalidEnode() throws Exception {
    final URL configFile = this.getClass().getResource(PERMISSIONING_CONFIG_INVALID_ENODE);
    final Path toml = createTempFile("toml", Resources.toByteArray(configFile));

    final Throwable thrown = catchThrowable(() -> nodeOnlyPermissioningConfig(toml));

    assertThat(thrown)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid node ID");
  }

  @Test
  public void permissioningConfigWithEmptyAllowlistMustNotError() throws Exception {
    final URL configFile = this.getClass().getResource(PERMISSIONING_CONFIG_EMPTY_ALLOWLISTS);
    final Path toml = createTempFile("toml", Resources.toByteArray(configFile));

    LocalPermissioningConfiguration permissioningConfiguration = permissioningConfig(toml);

    assertThat(permissioningConfiguration.isNodeAllowlistEnabled()).isTrue();
    assertThat(permissioningConfiguration.getNodeAllowlist()).isEmpty();
    assertThat(permissioningConfiguration.isAccountAllowlistEnabled()).isTrue();
    assertThat(permissioningConfiguration.getAccountAllowlist()).isEmpty();
  }

  @Test
  public void permissioningConfigWithAbsentAllowlistMustThrowException() throws Exception {
    final URL configFile = this.getClass().getResource(PERMISSIONING_CONFIG_ABSENT_ALLOWLISTS);
    final Path toml = createTempFile("toml", Resources.toByteArray(configFile));

    final Throwable thrown = catchThrowable(() -> permissioningConfig(toml));

    assertThat(thrown).isInstanceOf(Exception.class).hasMessageContaining("Unexpected end of line");
  }

  @Test
  public void permissioningConfigWithUnrecognizedKeyMustThrowException() throws Exception {
    final URL configFile = this.getClass().getResource(PERMISSIONING_CONFIG_UNRECOGNIZED_KEY);
    final Path toml = createTempFile("toml", Resources.toByteArray(configFile));

    final Throwable thrown = catchThrowable(() -> accountOnlyPermissioningConfig(toml));

    assertThat(thrown)
        .isInstanceOf(Exception.class)
        .hasMessageContaining("config option missing")
        .hasMessageContaining(PermissioningConfigurationBuilder.ACCOUNTS_ALLOWLIST_KEY);
  }

  @Test
  public void permissioningConfigWithEmptyFileMustThrowException() throws Exception {
    // write an empty file
    final Path toml = createTempFile("toml", "".getBytes(UTF_8));

    final Throwable thrown = catchThrowable(() -> permissioningConfig(toml));

    assertThat(thrown).isInstanceOf(Exception.class).hasMessageContaining("Empty TOML result");
  }

  @Test
  public void permissioningConfigFromFileMustSetFilePath() throws Exception {
    final URL configFile = this.getClass().getResource(PERMISSIONING_CONFIG_VALID);
    final Path toml = createTempFile("toml", Resources.toByteArray(configFile));

    LocalPermissioningConfiguration permissioningConfiguration =
        PermissioningConfigurationBuilder.permissioningConfiguration(
            true, dnsDisabled(), toml.toString(), true, toml.toString());

    assertThat(permissioningConfiguration.getNodePermissioningConfigFilePath())
        .isEqualTo(toml.toString());
    assertThat(permissioningConfiguration.getAccountPermissioningConfigFilePath())
        .isEqualTo(toml.toString());
  }

  @Test
  public void permissioningConfigFromNonexistentFileMustThrowException() {
    final Throwable thrown =
        catchThrowable(() -> permissioningConfig(Paths.get("file-does-not-exist")));

    assertThat(thrown)
        .isInstanceOf(Exception.class)
        .hasMessageContaining("Configuration file does not exist");
  }

  @Test
  public void permissioningConfigFromMultilineFileMustParseCorrectly() throws Exception {
    final URL configFile =
        this.getClass().getResource(PERMISSIONING_CONFIG_NODE_ALLOWLIST_ONLY_MULTILINE);
    final LocalPermissioningConfiguration permissioningConfiguration =
        PermissioningConfigurationBuilder.permissioningConfiguration(
            true, dnsDisabled(), configFile.getPath(), false, configFile.getPath());

    assertThat(permissioningConfiguration.isNodeAllowlistEnabled()).isTrue();
    assertThat(permissioningConfiguration.getNodeAllowlist().size()).isEqualTo(5);
  }

  private LocalPermissioningConfiguration accountOnlyPermissioningConfig(final Path toml)
      throws Exception {
    return PermissioningConfigurationBuilder.permissioningConfiguration(
        false, dnsDisabled(), null, true, toml.toAbsolutePath().toString());
  }

  private LocalPermissioningConfiguration nodeOnlyPermissioningConfig(final Path toml)
      throws Exception {
    return PermissioningConfigurationBuilder.permissioningConfiguration(
        true, dnsDisabled(), toml.toAbsolutePath().toString(), false, null);
  }

  private LocalPermissioningConfiguration permissioningConfig(final Path toml) throws Exception {
    return PermissioningConfigurationBuilder.permissioningConfiguration(
        true,
        dnsDisabled(),
        toml.toAbsolutePath().toString(),
        true,
        toml.toAbsolutePath().toString());
  }

  private Path createTempFile(final String filename, final byte[] contents) throws IOException {
    final Path file = Files.createTempFile(filename, "");
    Files.write(file, contents);
    file.toFile().deleteOnExit();
    return file;
  }
}
