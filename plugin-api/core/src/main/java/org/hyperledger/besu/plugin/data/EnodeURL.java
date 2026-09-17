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
package org.hyperledger.besu.plugin.data;

import java.net.InetAddress;
import java.net.URI;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/** The interface Enode url. */
public interface EnodeURL {
  /**
   * Enode to uri without discovery port.
   *
   * @return the uri
   */
  URI toURIWithoutDiscoveryPort();

  /**
   * Gets node id.
   *
   * @return the node id
   */
  Bytes getNodeId();

  /**
   * Gets ip.
   *
   * @return the ip
   */
  InetAddress getIp();

  /**
   * Gets listening port.
   *
   * @return the listening port
   */
  Optional<Integer> getListeningPort();

  /**
   * Gets listening port or zero.
   *
   * @return the listening port or zero
   */
  int getListeningPortOrZero();

  /**
   * Enode To URI.
   *
   * @return the uri
   */
  URI toURI();

  /**
   * Gets discovery port.
   *
   * @return the discovery port
   */
  Optional<Integer> getDiscoveryPort();

  /**
   * Is listening.
   *
   * @return the boolean
   */
  boolean isListening();

  /**
   * Is running discovery.
   *
   * @return the boolean
   */
  boolean isRunningDiscovery();

  /**
   * Gets ip as string.
   *
   * @return the ip as string
   */
  String getIpAsString();

  /**
   * Gets discovery port or zero.
   *
   * @return the discovery port or zero
   */
  int getDiscoveryPortOrZero();

  /**
   * Gets the host.
   *
   * @return the host
   */
  String getHost();
}
