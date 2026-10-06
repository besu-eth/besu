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

import org.hyperledger.besu.ethereum.p2p.peers.EnodeDnsConfiguration;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.jspecify.annotations.Nullable;

public class LocalPermissioningConfiguration {
  private final List<EnodeURLImpl> nodeAllowlist = new ArrayList<>();
  private final List<String> accountAllowlist = new ArrayList<>();
  private boolean nodeAllowlistEnabled;
  private EnodeDnsConfiguration enodeDnsConfiguration = EnodeDnsConfiguration.dnsDisabled();
  private @Nullable String nodePermissioningConfigFilePath;
  private boolean accountAllowlistEnabled;
  private @Nullable String accountPermissioningConfigFilePath;

  public List<EnodeURLImpl> getNodeAllowlist() {
    return nodeAllowlist;
  }

  public static LocalPermissioningConfiguration createDefault() {
    return new LocalPermissioningConfiguration();
  }

  public void setEnodeDnsConfiguration(final EnodeDnsConfiguration enodeDnsConfiguration) {
    this.enodeDnsConfiguration = enodeDnsConfiguration;
  }

  public void setNodeAllowlist(final @Nullable Collection<EnodeURLImpl> nodeAllowlist) {
    if (nodeAllowlist != null) {
      this.nodeAllowlist.addAll(nodeAllowlist);
      this.nodeAllowlistEnabled = true;
    }
  }

  public EnodeDnsConfiguration getEnodeDnsConfiguration() {
    return enodeDnsConfiguration;
  }

  public boolean isNodeAllowlistEnabled() {
    return nodeAllowlistEnabled;
  }

  public List<String> getAccountAllowlist() {
    return accountAllowlist;
  }

  public void setAccountAllowlist(final @Nullable Collection<String> accountAllowlist) {
    if (accountAllowlist != null) {
      this.accountAllowlist.addAll(accountAllowlist);
      this.accountAllowlistEnabled = true;
    }
  }

  public boolean isAccountAllowlistEnabled() {
    return accountAllowlistEnabled;
  }

  public @Nullable String getNodePermissioningConfigFilePath() {
    return nodePermissioningConfigFilePath;
  }

  public void setNodePermissioningConfigFilePath(
      final @Nullable String nodePermissioningConfigFilePath) {
    this.nodePermissioningConfigFilePath = nodePermissioningConfigFilePath;
  }

  public @Nullable String getAccountPermissioningConfigFilePath() {
    return accountPermissioningConfigFilePath;
  }

  public void setAccountPermissioningConfigFilePath(
      final @Nullable String accountPermissioningConfigFilePath) {
    this.accountPermissioningConfigFilePath = accountPermissioningConfigFilePath;
  }
}
