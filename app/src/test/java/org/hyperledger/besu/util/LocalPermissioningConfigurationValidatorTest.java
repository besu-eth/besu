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
              "enode://4e5e92199ee224a01932a377160aa432f31d0b351f84ab413a8e0a42f4f36476f8fb1cbe914af0d9aef0d51665c214cf653c651c4bbd9d5550a934f241f1682b@138.197.51.181:30303");
      assertThat(e.getMessage())
          .contains(
              "enode://143e11fb766781d22d92a2e33f8f104cddae4411a122295ed1fdb6638de96a6ce65f5b7c964ba3763bba27961738fef7d3ecc739268f3e5e771fb4c87b6234ba@146.190.1.103:30303");
      assertThat(e.getMessage())
          .contains(
              "enode://8b61dc2d06c3f96fddcbebb0efb29d60d3598650275dc469c22229d3e5620369b0d3dedafd929835fe7f489618f19f456fe7c0df572bf2d914a9f4e006f783a9@170.64.250.88:30303");
      assertThat(e.getMessage())
          .contains(
              "enode://10d62eff032205fcef19497f35ca8477bea0eadfff6d769a147e895d8b2b8f8ae6341630c645c30f5df6e67547c03494ced3d9c5764e8622a26587b083b028e8@139.59.49.206:30303");
      assertThat(e.getMessage())
          .contains(
              "enode://9e9492e2e8836114cc75f5b929784f4f46c324ad01daf87d956f98b3b6c5fcba95524d6e5cf9861dc96a2c8a171ea7105bb554a197455058de185fa870970c7c@138.68.123.152:30303");
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
