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
package org.hyperledger.besu.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** The Discovery options. */
public class DiscoveryOptions {
  /** The constant DEFAULT. */
  public static final DiscoveryOptions DEFAULT =
      new DiscoveryOptions(JsonUtil.createEmptyObjectNode());

  private static final String ENODES_KEY = "bootnodes";
  private static final String DNS_KEY = "dns";

  private final ObjectNode discoveryConfigRoot;

  /**
   * Instantiates a new Discovery options.
   *
   * @param discoveryConfigRoot the discovery config root
   */
  public DiscoveryOptions(final ObjectNode discoveryConfigRoot) {
    this.discoveryConfigRoot = discoveryConfigRoot;
  }

  /**
   * Gets boot nodes.
   *
   * @return optional list of boot nodes
   */
  public Optional<List<String>> getBootNodes() {
    final Optional<ArrayNode> bootNodesArray =
        JsonUtil.getArrayNode(discoveryConfigRoot, ENODES_KEY);
    if (bootNodesArray.isEmpty()) {
      return Optional.empty();
    }
    final List<String> bootNodes = new ArrayList<>();
    bootNodesArray
        .get()
        .elements()
        .forEachRemaining(
            bootNodeElement -> {
              if (!bootNodeElement.isTextual()) {
                throw new IllegalArgumentException(
                    ENODES_KEY + " does not contain a string: " + bootNodeElement);
              }
              bootNodes.add(bootNodeElement.asText());
            });
    return Optional.of(bootNodes);
  }

  /**
   * Gets discovery dns url.
   *
   * @return the discovery dns url
   */
  public Optional<String> getDiscoveryDnsUrl() {
    return JsonUtil.getString(discoveryConfigRoot, DNS_KEY);
  }
}
