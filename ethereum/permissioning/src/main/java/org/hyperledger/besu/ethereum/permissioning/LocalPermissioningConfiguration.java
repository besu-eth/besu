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

public class LocalPermissioningConfiguration {
  private List<EnodeURLImpl> nodeAllowlist;
  private List<String> accountAllowlist;
  private boolean nodeAllowlistEnabled;
  private EnodeDnsConfiguration enodeDnsConfiguration = EnodeDnsConfiguration.dnsDisabled();
  private String nodePermissioningConfigFilePath;
  private boolean accountAllowlistEnabled;
  private String accountPermissioningConfigFilePath;

  public List<EnodeURLImpl> getNodeAllowlist() {
    return nodeAllowlist;
  }

  public static LocalPermissioningConfiguration createDefault() {
    final LocalPermissioningConfiguration config = new LocalPermissioningConfiguration();
    config.nodeAllowlist = new ArrayList<>();
    config.accountAllowlist = new ArrayList<>();
    return config;
  }

  public void setEnodeDnsConfiguration(final EnodeDnsConfiguration enodeDnsConfiguration) {
    this.enodeDnsConfiguration = enodeDnsConfiguration;
  }

  public void setNodeAllowlist(final Collection<EnodeURLImpl> nodeAllowlist) {
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

  public void setAccountAllowlist(final Collection<String> accountAllowlist) {
    if (accountAllowlist != null) {
      this.accountAllowlist.addAll(accountAllowlist);
      this.accountAllowlistEnabled = true;
    }
  }

  public boolean isAccountAllowlistEnabled() {
    return accountAllowlistEnabled;
  }

  public String getNodePermissioningConfigFilePath() {
    return nodePermissioningConfigFilePath;
  }

  public void setNodePermissioningConfigFilePath(final String nodePermissioningConfigFilePath) {
    this.nodePermissioningConfigFilePath = nodePermissioningConfigFilePath;
  }

  public String getAccountPermissioningConfigFilePath() {
    return accountPermissioningConfigFilePath;
  }

  public void setAccountPermissioningConfigFilePath(
      final String accountPermissioningConfigFilePath) {
    this.accountPermissioningConfigFilePath = accountPermissioningConfigFilePath;
  }
}
