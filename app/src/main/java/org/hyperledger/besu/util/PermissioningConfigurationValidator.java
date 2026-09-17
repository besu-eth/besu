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
package org.hyperledger.besu.util;

import org.hyperledger.besu.ethereum.p2p.discovery.NodeIdentifier;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.permissioning.LocalPermissioningConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/** The Permissioning configuration validator. */
public class PermissioningConfigurationValidator {
  /** Default Constructor. */
  PermissioningConfigurationValidator() {}

  /**
   * Check if all nodes are in allowlist.
   *
   * @param nodeIdentifiers the node URIs
   * @param permissioningConfiguration the permissioning configuration
   * @throws Exception In case of nodes are not in allow list
   */
  public static void areAllNodesInAllowlist(
      final Collection<? extends NodeIdentifier> nodeIdentifiers,
      final LocalPermissioningConfiguration permissioningConfiguration)
      throws Exception {

    if (permissioningConfiguration.isNodeAllowlistEnabled() && nodeIdentifiers != null) {
      final List<EnodeURLImpl> allowlistNodesWithoutQueryParam =
          permissioningConfiguration.getNodeAllowlist();

      final List<NodeIdentifier> nodesNotInAllowlist = new ArrayList<>(nodeIdentifiers);
      nodesNotInAllowlist.removeIf(
          (node) ->
              allowlistNodesWithoutQueryParam.stream()
                  .anyMatch(
                      (allowedNode) -> NodeIdentifier.isSameListeningEndpoint(node, allowedNode)));

      if (!nodesNotInAllowlist.isEmpty()) {
        throw new Exception(
            "Specified node(s) not in nodes-allowlist " + enodesAsStrings(nodesNotInAllowlist));
      }
    }
  }

  private static Collection<String> enodesAsStrings(final List<NodeIdentifier> nodeIdentifiers) {
    return nodeIdentifiers.parallelStream().map(Object::toString).collect(Collectors.toList());
  }
}
