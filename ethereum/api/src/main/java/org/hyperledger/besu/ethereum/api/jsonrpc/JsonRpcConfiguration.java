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
package org.hyperledger.besu.ethereum.api.jsonrpc;

import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis.DEFAULT_RPC_APIS;

import org.hyperledger.besu.ethereum.api.handlers.TimeoutOptions;
import org.hyperledger.besu.ethereum.api.jsonrpc.authentication.JwtAlgorithm;
import org.hyperledger.besu.ethereum.api.tls.TlsConfiguration;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.google.common.base.MoreObjects;

public class JsonRpcConfiguration {
  public static final String DEFAULT_JSON_RPC_HOST = "127.0.0.1";
  public static final int DEFAULT_JSON_RPC_PORT = 8545;
  public static final int DEFAULT_ENGINE_JSON_RPC_PORT = 8551;
  public static final int DEFAULT_MAX_ACTIVE_CONNECTIONS = 80;
  public static final int DEFAULT_MAX_BATCH_SIZE = 1024;
  public static final long DEFAULT_HTTP_TIMEOUT_SEC =
      TimeoutOptions.defaultOptions().getTimeoutSeconds();
  public static final long DEFAULT_HTTP_STREAMING_TIMEOUT_SEC = 600; // 10 minutes
  public static final long DEFAULT_MAX_REQUEST_CONTENT_LENGTH = 128 * 1024 * 1024; // 128MB
  public static final boolean DEFAULT_PRETTY_JSON_ENABLED = false;

  private boolean enabled;
  private int port;
  private String host;
  private List<String> corsAllowedDomains = Collections.emptyList();
  private List<String> rpcApis;
  private List<String> noAuthRpcApis = Collections.emptyList();
  private List<String> hostsAllowlist = Arrays.asList("localhost", "127.0.0.1");
  private boolean authenticationEnabled = false;
  private String authenticationCredentialsFile;
  private JwtAlgorithm authenticationAlgorithm = JwtAlgorithm.RS256;
  private File authenticationPublicKeyFile;
  private Optional<TlsConfiguration> tlsConfiguration = Optional.empty();
  private long httpTimeoutSec = DEFAULT_HTTP_TIMEOUT_SEC;
  private long httpStreamingTimeoutSec = DEFAULT_HTTP_STREAMING_TIMEOUT_SEC;
  private int maxActiveConnections;
  private int maxBatchSize;
  private long maxRequestContentLength;
  private boolean prettyJsonEnabled;

  public static JsonRpcConfiguration createDefault() {
    final JsonRpcConfiguration config = new JsonRpcConfiguration();
    config.setEnabled(false);
    config.setPort(DEFAULT_JSON_RPC_PORT);
    config.setHost(DEFAULT_JSON_RPC_HOST);
    config.setRpcApis(DEFAULT_RPC_APIS);
    config.httpTimeoutSec = TimeoutOptions.defaultOptions().getTimeoutSeconds();
    config.setMaxActiveConnections(DEFAULT_MAX_ACTIVE_CONNECTIONS);
    config.setMaxBatchSize(DEFAULT_MAX_BATCH_SIZE);
    config.setMaxRequestContentLength(DEFAULT_MAX_REQUEST_CONTENT_LENGTH);
    config.setPrettyJsonEnabled(DEFAULT_PRETTY_JSON_ENABLED);
    return config;
  }

  public static JsonRpcConfiguration createEngineDefault() {
    final JsonRpcConfiguration config = createDefault();
    config.setEnabled(false);
    config.setPort(DEFAULT_ENGINE_JSON_RPC_PORT);
    List<String> engineMethodGroup = new ArrayList<>(2);
    engineMethodGroup.add(RpcApis.ENGINE.name());
    engineMethodGroup.add(RpcApis.ETH.name());
    config.setRpcApis(engineMethodGroup);
    config.setAuthenticationEnabled(true);
    config.setAuthenticationAlgorithm(JwtAlgorithm.HS256);
    config.setAuthenticationPublicKeyFile(null); // ephemeral key will be generated on startup.
    return config;
  }

  private JsonRpcConfiguration() {}

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(final boolean enabled) {
    this.enabled = enabled;
  }

  public int getPort() {
    return port;
  }

  public void setPort(final int port) {
    this.port = port;
  }

  public String getHost() {
    return host;
  }

  public void setHost(final String host) {
    this.host = host;
  }

  public Collection<String> getCorsAllowedDomains() {
    return corsAllowedDomains;
  }

  public void setCorsAllowedDomains(final List<String> corsAllowedDomains) {
    if (corsAllowedDomains != null) {
      this.corsAllowedDomains = corsAllowedDomains;
    }
  }

  public Collection<String> getRpcApis() {
    return rpcApis;
  }

  public void setRpcApis(final List<String> rpcApis) {
    this.rpcApis = rpcApis;
  }

  public Collection<String> getNoAuthRpcApis() {
    return this.noAuthRpcApis;
  }

  public void setNoAuthRpcApis(final List<String> rpcApis) {
    this.noAuthRpcApis = rpcApis;
  }

  public void addRpcApi(final String rpcApi) {
    this.rpcApis = new ArrayList<>(rpcApis);
    rpcApis.add(rpcApi);
  }

  public Collection<String> getHostsAllowlist() {
    return Collections.unmodifiableCollection(this.hostsAllowlist);
  }

  public void setHostsAllowlist(final List<String> hostsAllowlist) {
    this.hostsAllowlist = hostsAllowlist;
  }

  public boolean isAuthenticationEnabled() {
    return authenticationEnabled;
  }

  public void setAuthenticationEnabled(final boolean authenticationEnabled) {
    this.authenticationEnabled = authenticationEnabled;
  }

  public void setAuthenticationCredentialsFile(final String authenticationCredentialsFile) {
    this.authenticationCredentialsFile = authenticationCredentialsFile;
  }

  public String getAuthenticationCredentialsFile() {
    return authenticationCredentialsFile;
  }

  public File getAuthenticationPublicKeyFile() {
    return authenticationPublicKeyFile;
  }

  public void setAuthenticationPublicKeyFile(final File authenticationPublicKeyFile) {
    this.authenticationPublicKeyFile = authenticationPublicKeyFile;
  }

  public JwtAlgorithm getAuthenticationAlgorithm() {
    return authenticationAlgorithm;
  }

  public void setAuthenticationAlgorithm(final JwtAlgorithm authenticationAlgorithm) {
    this.authenticationAlgorithm = authenticationAlgorithm;
  }

  public Optional<TlsConfiguration> getTlsConfiguration() {
    return tlsConfiguration;
  }

  public void setTlsConfiguration(final Optional<TlsConfiguration> tlsConfiguration) {
    this.tlsConfiguration = tlsConfiguration;
  }

  public long getHttpTimeoutSec() {
    return httpTimeoutSec;
  }

  public void setHttpTimeoutSec(final long httpTimeoutSec) {
    this.httpTimeoutSec = httpTimeoutSec;
  }

  public long getHttpStreamingTimeoutSec() {
    return httpStreamingTimeoutSec;
  }

  public void setHttpStreamingTimeoutSec(final long httpStreamingTimeoutSec) {
    this.httpStreamingTimeoutSec = httpStreamingTimeoutSec;
  }

  public boolean isPrettyJsonEnabled() {
    return prettyJsonEnabled;
  }

  public void setPrettyJsonEnabled(final boolean prettyJsonEnabled) {
    this.prettyJsonEnabled = prettyJsonEnabled;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("enabled", enabled)
        .add("port", port)
        .add("host", host)
        .add("corsAllowedDomains", corsAllowedDomains)
        .add("hostsAllowlist", hostsAllowlist)
        .add("rpcApis", rpcApis)
        .add("authenticationEnabled", authenticationEnabled)
        .add("authenticationCredentialsFile", authenticationCredentialsFile)
        .add("authenticationPublicKeyFile", authenticationPublicKeyFile)
        .add("tlsConfiguration", tlsConfiguration)
        .add("httpTimeoutSec", httpTimeoutSec)
        .add("maxActiveConnections", maxActiveConnections)
        .add("maxBatchSize", maxBatchSize)
        .toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final JsonRpcConfiguration that = (JsonRpcConfiguration) o;
    return enabled == that.enabled
        && port == that.port
        && authenticationEnabled == that.authenticationEnabled
        && Objects.equals(host, that.host)
        && Objects.equals(corsAllowedDomains, that.corsAllowedDomains)
        && Objects.equals(rpcApis, that.rpcApis)
        && Objects.equals(hostsAllowlist, that.hostsAllowlist)
        && Objects.equals(authenticationCredentialsFile, that.authenticationCredentialsFile)
        && Objects.equals(authenticationPublicKeyFile, that.authenticationPublicKeyFile)
        && maxBatchSize == that.maxBatchSize;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        enabled,
        port,
        host,
        corsAllowedDomains,
        rpcApis,
        hostsAllowlist,
        authenticationEnabled,
        authenticationCredentialsFile,
        authenticationPublicKeyFile,
        maxBatchSize);
  }

  public int getMaxActiveConnections() {
    return maxActiveConnections;
  }

  public void setMaxActiveConnections(final int maxActiveConnections) {
    this.maxActiveConnections = maxActiveConnections;
  }

  public int getMaxBatchSize() {
    return maxBatchSize;
  }

  public void setMaxBatchSize(final int maxBatchSize) {
    this.maxBatchSize = maxBatchSize;
  }

  public long getMaxRequestContentLength() {
    return maxRequestContentLength;
  }

  public void setMaxRequestContentLength(final long maxRequestContentLength) {
    this.maxRequestContentLength = maxRequestContentLength;
  }
}
