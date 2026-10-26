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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.api.pluginadapter.NetworkEndpointServiceImpl;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.plugin.services.NetworkEndpointService;
import org.hyperledger.besu.plugin.services.NetworkEndpointService.NetworkEndpoint;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class NetServicesTest {

  private final NetworkEndpointServiceImpl networkEndpointService =
      new NetworkEndpointServiceImpl();
  private JsonRpcConfiguration jsonRpcConfiguration;

  @BeforeEach
  public void setUp() {
    jsonRpcConfiguration = JsonRpcConfiguration.createDefault();
    jsonRpcConfiguration.setEnabled(true);
    jsonRpcConfiguration.setHost("127.0.0.1");
    jsonRpcConfiguration.setPort(8545);
  }

  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(netServices().getName()).isEqualTo("net_services");
  }

  @Test
  public void shouldReportEnabledPluginEndpointsAfterBuiltInServices() {
    networkEndpointService.registerEndpoint(endpoint("pluginB", true, "0.0.0.0", 9001));
    networkEndpointService.registerEndpoint(endpoint("disabled", false, "127.0.0.1", 9002));
    networkEndpointService.registerEndpoint(endpoint("pluginA", true, "127.0.0.1", 9003));

    final Map<String, Map<String, String>> result = response();

    assertThat(result.keySet()).containsExactly("jsonrpc", "pluginB", "pluginA");
    assertThat(result.get("jsonrpc")).isEqualTo(details("127.0.0.1", "8545"));
    assertThat(result.get("pluginB")).isEqualTo(details("0.0.0.0", "9001"));
    assertThat(result.get("pluginA")).isEqualTo(details("127.0.0.1", "9003"));
  }

  @Test
  public void shouldReportPluginEndpointRightAfterMetrics() {
    final MetricsConfiguration metrics =
        MetricsConfiguration.builder().enabled(true).host("127.0.0.1").port(9545).build();
    networkEndpointService.registerEndpoint(endpoint("pluginA", true, "127.0.0.1", 9004));

    final Map<String, Map<String, String>> result = response(netServices(metrics));

    // the same format as the built-in entries, after the built-in services (here, right after
    // metrics)
    assertThat(result.keySet()).containsExactly("jsonrpc", "metrics", "pluginA");
    assertThat(result.get("pluginA")).isEqualTo(details("127.0.0.1", "9004"));
  }

  @Test
  public void throwingEndpointIsSkipped() {
    networkEndpointService.registerEndpoint(
        new DelegatingEndpoint(endpoint("broken", true, "127.0.0.1", 1)) {
          @Override
          public boolean isEnabled() {
            throw new IllegalStateException("boom");
          }
        });
    networkEndpointService.registerEndpoint(endpoint("healthy", true, "127.0.0.1", 2));

    final Map<String, Map<String, String>> result = response();

    assertThat(result.keySet()).containsExactly("jsonrpc", "healthy");
  }

  @Test
  public void endpointWithoutHostIsSkipped() {
    networkEndpointService.registerEndpoint(endpoint("noHost", true, null, 1));
    networkEndpointService.registerEndpoint(endpoint("healthy", true, "127.0.0.1", 2));

    final Map<String, Map<String, String>> result = response();

    assertThat(result.keySet()).containsExactly("jsonrpc", "healthy");
  }

  @Test
  public void builtInServiceWinsOnNameClash() {
    // the registry rejects built-in names, so serve the clashing endpoint from a service that
    // does not check them
    final NetworkEndpoint clashing = endpoint("jsonrpc", true, "10.0.0.1", 1);
    final NetworkEndpointService unchecked =
        new NetworkEndpointService() {
          @Override
          public void registerEndpoint(final NetworkEndpoint endpoint) {
            throw new UnsupportedOperationException();
          }

          @Override
          public List<NetworkEndpoint> getEndpoints() {
            return List.of(clashing);
          }
        };

    final Map<String, Map<String, String>> result =
        response(netServices(MetricsConfiguration.builder().build(), unchecked));

    assertThat(result.keySet()).containsExactly("jsonrpc");
    assertThat(result.get("jsonrpc")).isEqualTo(details("127.0.0.1", "8545"));
  }

  private Map<String, Map<String, String>> response() {
    return response(netServices());
  }

  @SuppressWarnings("unchecked")
  private Map<String, Map<String, String>> response(final NetServices netServices) {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", "net_services", new Object[] {}));
    final JsonRpcSuccessResponse response = (JsonRpcSuccessResponse) netServices.response(request);
    return (Map<String, Map<String, String>>) response.getResult();
  }

  private NetServices netServices() {
    return netServices(MetricsConfiguration.builder().build());
  }

  private NetServices netServices(final MetricsConfiguration metricsConfiguration) {
    return netServices(metricsConfiguration, networkEndpointService);
  }

  private NetServices netServices(
      final MetricsConfiguration metricsConfiguration,
      final NetworkEndpointService endpointService) {
    return new NetServices(
        jsonRpcConfiguration,
        WebSocketConfiguration.createDefault(),
        mock(P2PNetwork.class),
        metricsConfiguration,
        endpointService,
        // a built-in service that these tests leave disabled
        mock());
  }

  private static class DelegatingEndpoint implements NetworkEndpoint {
    private final NetworkEndpoint delegate;

    DelegatingEndpoint(final NetworkEndpoint delegate) {
      this.delegate = delegate;
    }

    @Override
    public String getServiceName() {
      return delegate.getServiceName();
    }

    @Override
    public String getPortsFileKey() {
      return delegate.getPortsFileKey();
    }

    @Override
    public boolean isEnabled() {
      return delegate.isEnabled();
    }

    @Override
    public String getHost() {
      return delegate.getHost();
    }

    @Override
    public int getConfiguredPort() {
      return delegate.getConfiguredPort();
    }

    @Override
    public Optional<Integer> getListeningPort() {
      return delegate.getListeningPort();
    }
  }

  private static Map<String, String> details(final String host, final String port) {
    return Map.of("host", host, "port", port);
  }

  private static NetworkEndpoint endpoint(
      final String name, final boolean enabled, final String host, final int port) {
    return new NetworkEndpoint() {
      @Override
      public String getServiceName() {
        return name;
      }

      @Override
      public String getPortsFileKey() {
        return name + "-key";
      }

      @Override
      public boolean isEnabled() {
        return enabled;
      }

      @Override
      public String getHost() {
        return host;
      }

      @Override
      public int getConfiguredPort() {
        return port;
      }

      @Override
      public Optional<Integer> getListeningPort() {
        return Optional.empty();
      }
    };
  }
}
