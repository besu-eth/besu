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
package org.hyperledger.besu.ethereum.permissioning.pluginadapter;

import org.hyperledger.besu.plugin.services.PermissioningService;
import org.hyperledger.besu.plugin.services.permissioning.NodeConnectionPermissioningProvider;
import org.hyperledger.besu.plugin.services.permissioning.NodeMessagePermissioningProvider;
import org.hyperledger.besu.plugin.services.permissioning.TransactionPermissioningProvider;

import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Permissioning service implementation. */
public class PermissioningServiceImpl implements PermissioningService {
  private static final Logger LOG = LoggerFactory.getLogger(PermissioningServiceImpl.class);

  private final List<NodeConnectionPermissioningProvider> connectionPermissioningProviders =
      Lists.newArrayList();
  private final List<TransactionPermissioningProvider> transactionPermissioningProviders =
      new ArrayList<>();

  /** Default Constructor. */
  @Inject
  public PermissioningServiceImpl() {}

  @Override
  public void registerNodePermissioningProvider(
      final NodeConnectionPermissioningProvider provider) {
    connectionPermissioningProviders.add(provider);
  }

  @Override
  public void registerTransactionPermissioningProvider(
      final TransactionPermissioningProvider provider) {
    transactionPermissioningProviders.add(provider);
    LOG.info("Registered new transaction permissioning provider.");
  }

  /**
   * Gets connection permissioning providers.
   *
   * @return the connection permissioning providers
   */
  public List<NodeConnectionPermissioningProvider> getConnectionPermissioningProviders() {
    return connectionPermissioningProviders;
  }

  private final List<NodeMessagePermissioningProvider> messagePermissioningProviders =
      Lists.newArrayList();

  @Override
  public void registerNodeMessagePermissioningProvider(
      final NodeMessagePermissioningProvider provider) {
    messagePermissioningProviders.add(provider);
  }

  /**
   * Gets message permissioning providers.
   *
   * @return the message permissioning providers
   */
  public List<NodeMessagePermissioningProvider> getMessagePermissioningProviders() {
    return messagePermissioningProviders;
  }

  /**
   * Gets transaction permissioning providers.
   *
   * @return the transaction permissioning providers
   */
  public List<TransactionPermissioningProvider> getTransactionPermissioningProviders() {
    return transactionPermissioningProviders;
  }
}
