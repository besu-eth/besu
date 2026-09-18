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
package org.hyperledger.besu.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import org.hyperledger.besu.cli.config.EthNetworkConfig;
import org.hyperledger.besu.config.NetworkDefinition;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeDnsConfiguration;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.ImmutableEnodeDnsConfiguration;
import org.hyperledger.besu.ethereum.permissioning.LocalPermissioningConfiguration;
import org.hyperledger.besu.ethereum.permissioning.PermissioningConfigurationBuilder;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LocalPermissioningConfigurationValidatorTest {

  static final String PERMISSIONING_CONFIG_SEPOLIA_BOOTNODES =
      "/permissioning_config_sepolia_bootnodes.toml";
  static final String PERMISSIONING_CONFIG = "/permissioning_config.toml";
  static final String PERMISSIONING_CONFIG_VALID_HOSTNAME =
      "/permissioning_config_valid_hostname.toml";
  static final String PERMISSIONING_CONFIG_UNKNOWN_HOSTNAME =
      "/permissioning_config_unknown_hostname.toml";

  @Test
  public void sepoliaWithNodesAllowlistOptionWhichDoesIncludeRopstenBootnodesMustNotError()
      throws Exception {

    EthNetworkConfig ethNetworkConfig =
        EthNetworkConfig.getNetworkConfig(NetworkDefinition.SEPOLIA);

    final URL configFile = this.getClass().getResource(PERMISSIONING_CONFIG_SEPOLIA_BOOTNODES);
    final Path toml = Files.createTempFile("toml", "");
    Files.write(toml, Resources.toByteArray(configFile));

    LocalPermissioningConfiguration permissioningConfiguration =
        PermissioningConfigurationBuilder.permissioningConfiguration(
            true,
            EnodeDnsConfiguration.DEFAULT_CONFIG,
            toml.toAbsolutePath().toString(),
            true,
            toml.toAbsolutePath().toString());

    final List<EnodeURLImpl> enodeURIs = ethNetworkConfig.enodeBootNodes();
    PermissioningConfigurationValidator.areAllNodesInAllowlist(
        enodeURIs, permissioningConfiguration);
  }

  @Test
  public void nodesAllowlistOptionWhichDoesNotIncludeBootnodesMustError() throws Exception {

    EthNetworkConfig ethNetworkConfig =
        EthNetworkConfig.getNetworkConfig(NetworkDefinition.SEPOLIA);

    final URL configFile = this.getClass().getResource(PERMISSIONING_CONFIG);
    final Path toml = Files.createTempFile("toml", "");
    toml.toFile().deleteOnExit();
    Files.write(toml, Resources.toByteArray(configFile));

    LocalPermissioningConfiguration permissioningConfiguration =
        PermissioningConfigurationBuilder.permissioningConfiguration(
            true,
            EnodeDnsConfiguration.DEFAULT_CONFIG,
            toml.toAbsolutePath().toString(),
            true,
            toml.toAbsolutePath().toString());

    try {
      final List<EnodeURLImpl> enodeURIs = ethNetworkConfig.enodeBootNodes();
      PermissioningConfigurationValidator.areAllNodesInAllowlist(
          enodeURIs, permissioningConfiguration);
      fail("expected exception because sepolia bootnodes are not in node-allowlist");
    } catch (Exception e) {
      assertThat(e.getMessage()).startsWith("Specified node(s) not in nodes-allowlist");
      assertThat(e.getMessage())
          .contains(
              "enode://4aff27bd8f1f667a56be304fcab797b4d7b630bf78581ffe6bc0d84852bb73362361ab2884ad396418abe25e7da621be0cadf5b02b6709b49d76b404813c9ddc@212.99.218.66:0?discport=20152");
      assertThat(e.getMessage())
          .contains(
              "enode://665565ef7b9734bafb27fda8234ca43ca51ea097d0f29d9c9340daee0437c9e1d409c8283d1ce135eedccc2850f94c39c3cc63c641ad5fb0bef9dd37bd0fa6c1@129.212.166.61:0?discport=30403");
      assertThat(e.getMessage())
          .contains(
              "enode://8e41eb6b03ef7b4c42d4cee19e8150f5fdc1ca28d9e33a627d09875e493a2bedfdea0bfe8c5bab778929a5334a311ca8d37e9016928fc6141c8fe52e708fbd98@144.126.252.24:0?discport=30403");
      assertThat(e.getMessage())
          .contains(
              "enode://b1e27df0cb42adc27b990879a5c4c99ce1bd15bdf0b829afdfdec50fbe352ad602075607f488ca805781e7f127d9709117a8bf8763ebac6e769f7cf50ded3806@178.156.215.140:0?discport=30403");
      assertThat(e.getMessage())
          .contains(
              "enode://02dc5303f128bd0c8055a1fb126c9929bd74d401fca2f2aeba5d74208d1854b08cd5e7932d2db39e21891bf0347599b2cb63bedf14c96fac2bd92c77def82352@5.223.94.81:0?discport=30403");
    }
  }

  @Test
  public void nodeAllowlistCheckShouldIgnoreDiscoveryPortParam() throws Exception {
    final URL configFile = this.getClass().getResource(PERMISSIONING_CONFIG);
    final Path toml = Files.createTempFile("toml", "");
    toml.toFile().deleteOnExit();
    Files.write(toml, Resources.toByteArray(configFile));

    final LocalPermissioningConfiguration permissioningConfiguration =
        PermissioningConfigurationBuilder.permissioningConfiguration(
            true,
            EnodeDnsConfiguration.DEFAULT_CONFIG,
            toml.toAbsolutePath().toString(),
            true,
            toml.toAbsolutePath().toString());

    // This node is defined in the PERMISSIONING_CONFIG file without the discovery port
    final EnodeURLImpl enodeURL =
        EnodeURLImpl.fromString(
            "enode://6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@192.168.0.9:4567?discport=30303");

    // In an URI comparison the URLs should not match
    boolean isInAllowlist = permissioningConfiguration.getNodeAllowlist().contains(enodeURL);
    assertThat(isInAllowlist).isFalse();

    // However, for the allowlist validation, we should ignore the discovery port and don't throw an
    // error
    try {
      PermissioningConfigurationValidator.areAllNodesInAllowlist(
          Lists.newArrayList(enodeURL), permissioningConfiguration);
    } catch (Exception e) {
      fail(
          "Exception not expected. Validation of nodes in allowlist should ignore the optional discovery port param.");
    }
  }

  @Test
  public void nodeAllowlistCheckShouldWorkWithHostnameIfDnsEnabled() throws Exception {
    final URL configFile = this.getClass().getResource(PERMISSIONING_CONFIG_VALID_HOSTNAME);
    final Path toml = Files.createTempFile("toml", "");
    toml.toFile().deleteOnExit();
    Files.write(toml, Resources.toByteArray(configFile));

    final ImmutableEnodeDnsConfiguration enodeDnsConfiguration =
        ImmutableEnodeDnsConfiguration.builder().dnsEnabled(true).updateEnabled(false).build();
    final LocalPermissioningConfiguration permissioningConfiguration =
        PermissioningConfigurationBuilder.permissioningConfiguration(
            true,
            enodeDnsConfiguration,
            toml.toAbsolutePath().toString(),
            true,
            toml.toAbsolutePath().toString());

    // This node is defined in the PERMISSIONING_CONFIG_DNS file without the discovery port
    final EnodeURLImpl enodeURL =
        EnodeURLImpl.fromString(
            "enode://6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@localhost:4567?discport=30303",
            enodeDnsConfiguration);

    // In an URI comparison the URLs should not match
    boolean isInAllowlist = permissioningConfiguration.getNodeAllowlist().contains(enodeURL);
    assertThat(isInAllowlist).isFalse();

    // However, for the allowlist validation, we should ignore the discovery port and don't throw an
    // error
    try {
      PermissioningConfigurationValidator.areAllNodesInAllowlist(
          Lists.newArrayList(enodeURL), permissioningConfiguration);
    } catch (Exception e) {
      fail(
          "Exception not expected. Validation of nodes in allowlist should ignore the optional discovery port param.");
    }
  }

  @Test
  public void nodeAllowlistCheckShouldNotWorkWithHostnameWhenDnsDisabled() throws Exception {
    final URL configFile = this.getClass().getResource(PERMISSIONING_CONFIG_VALID_HOSTNAME);
    final Path toml = Files.createTempFile("toml", "");
    toml.toFile().deleteOnExit();
    Files.write(toml, Resources.toByteArray(configFile));

    final ImmutableEnodeDnsConfiguration enodeDnsConfiguration =
        ImmutableEnodeDnsConfiguration.builder().dnsEnabled(false).updateEnabled(false).build();

    assertThatThrownBy(
            () ->
                PermissioningConfigurationBuilder.permissioningConfiguration(
                    true,
                    enodeDnsConfiguration,
                    toml.toAbsolutePath().toString(),
                    true,
                    toml.toAbsolutePath().toString()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid enode URL syntax 'enode://6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@localhost:4567'. "
                + "Enode URL should have the following format 'enode://<node_id>@<ip>:<listening_port>[?discport=<discovery_port>]'. Invalid ip address.");
  }

  @Test
  public void nodeAllowlistCheckShouldNotWorkWithUnknownHostnameWhenOnlyDnsEnabled()
      throws Exception {
    final URL configFile = this.getClass().getResource(PERMISSIONING_CONFIG_UNKNOWN_HOSTNAME);
    final Path toml = Files.createTempFile("toml", "");
    toml.toFile().deleteOnExit();
    Files.write(toml, Resources.toByteArray(configFile));

    final ImmutableEnodeDnsConfiguration enodeDnsConfiguration =
        ImmutableEnodeDnsConfiguration.builder().dnsEnabled(true).updateEnabled(false).build();

    assertThatThrownBy(
            () ->
                PermissioningConfigurationBuilder.permissioningConfiguration(
                    true,
                    enodeDnsConfiguration,
                    toml.toAbsolutePath().toString(),
                    true,
                    toml.toAbsolutePath().toString()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid IP address");
  }
}
