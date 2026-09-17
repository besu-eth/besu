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
package org.hyperledger.besu.ethstats.util;

import static com.google.common.base.Preconditions.checkArgument;

import java.net.URI;
import java.nio.file.Path;

import org.immutables.value.Value;
import org.jspecify.annotations.Nullable;
import org.slf4j.LoggerFactory;

/**
 * This interface represents the connection options for Ethereum statistics. It provides methods to
 * get the scheme, node name, secret, host, port, contact, and CA certificate.
 */
@Value.Immutable
public interface EthStatsConnectOptions {
  /**
   * Gets the scheme of the connection.
   *
   * @return the scheme of the connection.
   */
  @Nullable String getScheme();

  /**
   * Gets the node name of the connection.
   *
   * @return the node name of the connection.
   */
  String getNodeName();

  /**
   * Gets the secret of the connection.
   *
   * @return the secret of the connection.
   */
  String getSecret();

  /**
   * Gets the host of the connection.
   *
   * @return the host of the connection.
   */
  String getHost();

  /**
   * Gets the port of the connection.
   *
   * @return the port of the connection.
   */
  Integer getPort();

  /**
   * Gets the contact of the connection.
   *
   * @return the contact of the connection.
   */
  String getContact();

  /**
   * Gets the CA certificate of the connection.
   *
   * @return the CA certificate of the connection.
   */
  @Nullable Path getCaCert();

  /**
   * Gets the ethstats report interval.
   *
   * @return the ethstats report interval
   */
  Integer getEthStatsReportInterval();

  /**
   * Creates an EthStatsConnectOptions from the given parameters with default report interval.
   *
   * @param url the url of the connection
   * @param contact the contact of the connection
   * @param caCert the CA certificate of the connection
   * @return the EthStatsConnectOptions
   */
  static EthStatsConnectOptions fromParams(
      final String url, final String contact, final Path caCert) {
    return fromParams(url, contact, caCert, 5); // Default to 5 seconds
  }

  /**
   * Creates an EthStatsConnectOptions from the given parameters.
   *
   * @param url the url of the connection
   * @param contact the contact of the connection
   * @param caCert the CA certificate of the connection
   * @param ethStatsReportInterval the ethstats report interval
   * @return the EthStatsConnectOptions
   */
  static EthStatsConnectOptions fromParams(
      final String url,
      final String contact,
      final Path caCert,
      final Integer ethStatsReportInterval) {
    try {
      checkArgument(url != null && !url.trim().isEmpty(), "Invalid empty value.");

      // if scheme is not specified in the URI, user info (nodename) gets converted to scheme.
      final URI uri;
      final String scheme;
      if (url.matches("^.*://.*$")) {
        // construct URI
        uri = URI.create(url);
        scheme = uri.getScheme();
      } else {
        // prepend ws:// to make a valid URI while keeping scheme as null
        uri = URI.create("ws://" + url);
        scheme = null;
      }

      if (scheme != null) {
        // make sure that scheme is either ws or wss
        if (!scheme.equalsIgnoreCase("ws") && !scheme.equalsIgnoreCase("wss")) {
          throw new IllegalArgumentException("Ethstats URI only support ws:// or wss:// scheme.");
        }
      }

      final String userInfo = uri.getUserInfo();

      // make sure user info is specified
      if (userInfo == null || !userInfo.contains(":")) {
        throw new IllegalArgumentException("Ethstats URI missing user info.");
      }
      final String nodeName = userInfo.substring(0, userInfo.indexOf(":"));
      final String secret = userInfo.substring(userInfo.indexOf(":") + 1);

      return ImmutableEthStatsConnectOptions.builder()
          .scheme(scheme)
          .nodeName(nodeName)
          .secret(secret)
          .host(uri.getHost())
          .port(uri.getPort())
          .contact(contact)
          .caCert(caCert)
          .ethStatsReportInterval(ethStatsReportInterval)
          .build();
    } catch (IllegalArgumentException e) {
      LoggerFactory.getLogger(EthStatsConnectOptions.class).error(e.getMessage());
    }
    throw new IllegalArgumentException(
        "Invalid ethstats URL syntax. Ethstats URL should have the following format '[ws://|wss://]nodename:secret@host[:port]'.");
  }
}
