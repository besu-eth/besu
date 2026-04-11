/*
 * Copyright contributors to Besu.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

class DiscoveryOptionsTest {

  @Test
  void defaultHasNoBootNodes() {
    assertThat(DiscoveryOptions.DEFAULT.getBootNodes()).isEmpty();
  }

  @Test
  void defaultHasNoV5BootNodes() {
    assertThat(DiscoveryOptions.DEFAULT.getV5BootNodes()).isEmpty();
  }

  @Test
  void defaultHasNoDnsUrl() {
    assertThat(DiscoveryOptions.DEFAULT.getDiscoveryDnsUrl()).isEmpty();
  }

  @Test
  void parsesBootNodes() {
    final ObjectNode root = JsonUtil.createEmptyObjectNode();
    final ArrayNode bootnodes = root.putArray("bootnodes");
    bootnodes.add("enode://abc@127.0.0.1:30303");
    bootnodes.add("enode://def@127.0.0.1:30304");

    final DiscoveryOptions options = new DiscoveryOptions(root);
    assertThat(options.getBootNodes())
        .isPresent()
        .hasValue(List.of("enode://abc@127.0.0.1:30303", "enode://def@127.0.0.1:30304"));
  }

  @Test
  void parsesV5BootNodes() {
    final ObjectNode root = JsonUtil.createEmptyObjectNode();
    final ArrayNode v5Bootnodes = root.putArray("v5bootnodes");
    v5Bootnodes.add("enr:-abc123");
    v5Bootnodes.add("enr:-def456");

    final DiscoveryOptions options = new DiscoveryOptions(root);
    assertThat(options.getV5BootNodes())
        .isPresent()
        .hasValue(List.of("enr:-abc123", "enr:-def456"));
  }

  @Test
  void v5BootNodesEmptyWhenKeyAbsent() {
    final ObjectNode root = JsonUtil.createEmptyObjectNode();
    root.putArray("bootnodes").add("enode://abc@127.0.0.1:30303");

    final DiscoveryOptions options = new DiscoveryOptions(root);
    assertThat(options.getV5BootNodes()).isEmpty();
    assertThat(options.getBootNodes()).isPresent();
  }

  @Test
  void bootNodesAndV5BootNodesCanCoexist() {
    final ObjectNode root = JsonUtil.createEmptyObjectNode();
    root.putArray("bootnodes").add("enode://abc@127.0.0.1:30303");
    root.putArray("v5bootnodes").add("enr:-abc123");

    final DiscoveryOptions options = new DiscoveryOptions(root);
    assertThat(options.getBootNodes()).isPresent().hasValue(List.of("enode://abc@127.0.0.1:30303"));
    assertThat(options.getV5BootNodes()).isPresent().hasValue(List.of("enr:-abc123"));
  }

  @Test
  void v5BootNodesThrowsOnNonStringElement() {
    final ObjectNode root = JsonUtil.createEmptyObjectNode();
    root.putArray("v5bootnodes").add(42);

    final DiscoveryOptions options = new DiscoveryOptions(root);
    assertThatThrownBy(options::getV5BootNodes)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("v5bootnodes does not contain a string");
  }

  @Test
  void parsesDnsUrl() {
    final ObjectNode root = JsonUtil.createEmptyObjectNode();
    root.put("dns", "enrtree://test@domain");

    final DiscoveryOptions options = new DiscoveryOptions(root);
    assertThat(options.getDiscoveryDnsUrl()).isPresent().hasValue("enrtree://test@domain");
  }
}
