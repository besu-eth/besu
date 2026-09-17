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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

public class LocalPermissioningConfigurationTest {

  final EnodeURLImpl[] nodes = {
    EnodeURLImpl.fromString(
        "enode://6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@127.0.0.1:4567"),
    EnodeURLImpl.fromString(
        "enode://7f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@127.0.0.1:4568"),
    EnodeURLImpl.fromString(
        "enode://8f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@127.0.0.1:4569")
  };

  @Test
  public void defaultConfiguration() {
    final LocalPermissioningConfiguration configuration =
        LocalPermissioningConfiguration.createDefault();
    assertThat(configuration.getNodeAllowlist()).isEmpty();
    assertThat(configuration.isNodeAllowlistEnabled()).isFalse();
    assertThat(configuration.getAccountAllowlist()).isEmpty();
    assertThat(configuration.isAccountAllowlistEnabled()).isFalse();
  }

  @Test
  public void setNodeAllowlist() {
    final LocalPermissioningConfiguration configuration =
        LocalPermissioningConfiguration.createDefault();
    configuration.setNodeAllowlist(Arrays.asList(nodes));
    assertThat(configuration.getNodeAllowlist()).containsExactlyInAnyOrder(nodes);
    assertThat(configuration.isNodeAllowlistEnabled()).isTrue();
  }

  @Test
  public void setNodeAllowListPassingNull() {
    final LocalPermissioningConfiguration configuration =
        LocalPermissioningConfiguration.createDefault();
    configuration.setNodeAllowlist(null);
    assertThat(configuration.getNodeAllowlist()).isEmpty();
    assertThat(configuration.isNodeAllowlistEnabled()).isFalse();
  }

  @Test
  public void setAccountAllowlist() {
    final String[] accounts = {"1111111111111111", "2222222222222222", "ffffffffffffffff"};
    final LocalPermissioningConfiguration configuration =
        LocalPermissioningConfiguration.createDefault();
    configuration.setAccountAllowlist(Arrays.asList(accounts));
    assertThat(configuration.getAccountAllowlist()).containsExactlyInAnyOrder(accounts);
    assertThat(configuration.isAccountAllowlistEnabled()).isTrue();
  }

  @Test
  public void setAccountAllowListPassingNull() {
    final LocalPermissioningConfiguration configuration =
        LocalPermissioningConfiguration.createDefault();
    configuration.setAccountAllowlist(null);
    assertThat(configuration.getAccountAllowlist()).isEmpty();
    assertThat(configuration.isAccountAllowlistEnabled()).isFalse();
  }
}
