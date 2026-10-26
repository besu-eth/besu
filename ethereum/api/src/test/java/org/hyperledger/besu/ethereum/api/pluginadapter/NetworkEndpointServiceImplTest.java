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
package org.hyperledger.besu.ethereum.api.pluginadapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.plugin.services.NetworkEndpointService.NetworkEndpoint;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class NetworkEndpointServiceImplTest {

  private final NetworkEndpointServiceImpl service = new NetworkEndpointServiceImpl();

  @Test
  void isEmptyByDefault() {
    assertThat(service.getEndpoints()).isEmpty();
  }

  @Test
  void returnsEndpointsInRegistrationOrder() {
    final NetworkEndpoint first = endpoint("first", "first-key");
    final NetworkEndpoint second = endpoint("second", "second-key");

    service.registerEndpoint(first);
    service.registerEndpoint(second);

    assertThat(service.getEndpoints()).containsExactly(first, second);
  }

  @Test
  void returnedListIsASnapshot() {
    final NetworkEndpoint first = endpoint("first", "first-key");
    service.registerEndpoint(first);
    final var snapshot = service.getEndpoints();

    service.registerEndpoint(endpoint("second", "second-key"));

    assertThat(snapshot).containsExactly(first);
    assertThatThrownBy(() -> snapshot.add(first)).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void rejectsNullEndpoint() {
    assertThatThrownBy(() -> service.registerEndpoint(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void rejectsNullOrBlankServiceName() {
    assertThatThrownBy(() -> service.registerEndpoint(endpoint(null, "key")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("service name");
    assertThatThrownBy(() -> service.registerEndpoint(endpoint(" ", "key")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("service name");
    assertThat(service.getEndpoints()).isEmpty();
  }

  @Test
  void rejectsNullOrBlankPortsFileKey() {
    assertThatThrownBy(() -> service.registerEndpoint(endpoint("name", null)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ports file key");
    assertThatThrownBy(() -> service.registerEndpoint(endpoint("name", "")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ports file key");
    assertThat(service.getEndpoints()).isEmpty();
  }

  @Test
  void rejectsDuplicateServiceName() {
    final NetworkEndpoint first = endpoint("name", "first-key");
    service.registerEndpoint(first);

    assertThatThrownBy(() -> service.registerEndpoint(endpoint("name", "second-key")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("already registered");
    assertThat(service.getEndpoints()).containsExactly(first);
  }

  @Test
  void rejectsDuplicatePortsFileKey() {
    final NetworkEndpoint first = endpoint("first", "key");
    service.registerEndpoint(first);

    assertThatThrownBy(() -> service.registerEndpoint(endpoint("second", "key")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("already registered");
    assertThat(service.getEndpoints()).containsExactly(first);
  }

  @ParameterizedTest
  @ValueSource(strings = {"jsonrpc", "ws", "p2p", "metrics"})
  void rejectsReservedServiceName(final String name) {
    assertThatThrownBy(() -> service.registerEndpoint(endpoint(name, "plugin-key")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("reserved");
    assertThat(service.getEndpoints()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {"discovery", "p2p", "json-rpc", "ws-rpc", "metrics", "engine-json-rpc"})
  void rejectsReservedPortsFileKey(final String key) {
    assertThatThrownBy(() -> service.registerEndpoint(endpoint("pluginService", key)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("reserved");
    assertThat(service.getEndpoints()).isEmpty();
  }

  @Test
  void stillAcceptsEndpointsAfterSeal() {
    final NetworkEndpoint early = endpoint("early", "early-key");
    service.registerEndpoint(early);
    assertThat(service.isSealed()).isFalse();

    service.seal();
    final NetworkEndpoint late = endpoint("late", "late-key");
    service.registerEndpoint(late);

    assertThat(service.isSealed()).isTrue();
    assertThat(service.getEndpoints()).containsExactly(early, late);
  }

  private static NetworkEndpoint endpoint(final String name, final String key) {
    return new NetworkEndpoint() {
      @Override
      public String getServiceName() {
        return name;
      }

      @Override
      public String getPortsFileKey() {
        return key;
      }

      @Override
      public boolean isEnabled() {
        return true;
      }

      @Override
      public String getHost() {
        return "127.0.0.1";
      }

      @Override
      public int getConfiguredPort() {
        return 1234;
      }

      @Override
      public Optional<Integer> getListeningPort() {
        return Optional.of(1234);
      }
    };
  }
}
