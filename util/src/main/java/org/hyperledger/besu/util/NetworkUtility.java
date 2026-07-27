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

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.net.InetAddresses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Network utility. */
public class NetworkUtility {
  /** The constant INADDR_ANY. */
  public static final String INADDR_ANY = "0.0.0.0";

  /** The constant INADDR_NONE. */
  public static final String INADDR_NONE = "255.255.255.255";

  /** The constant INADDR6_ANY. */
  public static final String INADDR6_ANY = "0:0:0:0:0:0:0:0";

  /** The constant INADDR6_NONE. */
  public static final String INADDR6_NONE = "::";

  /** The constant INADDR_LOCALHOST. */
  public static final String INADDR_LOCALHOST = "127.0.0.1";

  /** The constant INADDR6_LOCALHOST. */
  public static final String INADDR6_LOCALHOST = "::1";

  private static final Logger LOG = LoggerFactory.getLogger(NetworkUtility.class);

  private NetworkUtility() {}

  private static final Supplier<Boolean> ipv6Available =
      Suppliers.memoize(NetworkUtility::checkIpv6Availability);

  /**
   * Is IPv6 available?
   *
   * @return Returns true if the machine reports having any IPv6 addresses.
   */
  public static boolean isIPv6Available() {
    return ipv6Available.get();
  }

  /**
   * The standard for IPv6 availability is if the machine has any IPv6 addresses.
   *
   * @return Returns true if any IPv6 addresses are iterable via {@link NetworkInterface}.
   */
  private static Boolean checkIpv6Availability() {
    try {
      return NetworkInterface.networkInterfaces()
          .flatMap(NetworkInterface::inetAddresses)
          .anyMatch(addr -> addr instanceof Inet6Address);
    } catch (final Exception ignore) {
      // Any exception means we treat it as not available.
    }
    return false;
  }

  /**
   * Checks the port is not null and is in the valid range port (1-65536).
   *
   * @param port The port to check.
   * @return True if the port is valid, false otherwise.
   */
  public static boolean isValidPort(final int port) {
    return port > 0 && port < 65536;
  }

  /**
   * Url for socket address.
   *
   * @param scheme the scheme
   * @param address the address
   * @return the url
   */
  public static String urlForSocketAddress(final String scheme, final InetSocketAddress address) {
    String hostName = address.getHostName();
    if (isUnspecifiedAddress(hostName)) {
      hostName = InetAddress.getLoopbackAddress().getHostName();
    }
    if (hostName.contains(":")) {
      hostName = "[" + hostName + "]";
    }
    return scheme + "://" + hostName + ":" + address.getPort();
  }

  /**
   * Is network interface available.
   *
   * @param ipAddress the ip address
   * @return the boolean
   * @throws SocketException the socket exception
   * @throws UnknownHostException the unknown host exception
   */
  public static boolean isNetworkInterfaceAvailable(final String ipAddress)
      throws SocketException, UnknownHostException {
    if (isUnspecifiedAddress(ipAddress)) {
      return true;
    }
    return NetworkInterface.getByInetAddress(InetAddress.getByName(ipAddress)) != null;
  }

  /**
   * Is unspecified address.
   *
   * @param ipAddress the ip address
   * @return the boolean
   */
  public static boolean isUnspecifiedAddress(final String ipAddress) {
    return INADDR_ANY.equals(ipAddress)
        || INADDR6_ANY.equals(ipAddress)
        || INADDR_NONE.equals(ipAddress)
        || INADDR6_NONE.equals(ipAddress);
  }

  /**
   * Returns whether host address string is local host address.
   *
   * @param ipAddress the host address as a string
   * @return true if the host address is a local host address
   */
  public static boolean isLocalhostAddress(final String ipAddress) {
    return INADDR_LOCALHOST.equals(ipAddress) || INADDR6_LOCALHOST.equals(ipAddress);
  }

  /**
   * Check port.
   *
   * @param port the port
   * @param portTypeName the port type name
   */
  public static void checkPort(final int port, final String portTypeName) {
    if (!isValidPort(port)) {
      throw new IllegalPortException(
          String.format(
              "%s port requires a value between 1 and 65535. Got %d.", portTypeName, port));
    }
  }

  /**
   * Is port unavailable for tcp.
   *
   * @param port the port
   * @return true if the port is unavailable for TCP
   */
  public static boolean isPortUnavailableForTcp(final int port) {
    try (final ServerSocket serverSocket = new ServerSocket()) {
      serverSocket.setReuseAddress(true);
      serverSocket.bind(new InetSocketAddress(port));
      serverSocket.close();
      return false;
    } catch (IOException ex) {
      LOG.trace(String.format("Failed to open port %d for TCP", port), ex);
    }
    return true;
  }

  /**
   * Is port unavailable for udp.
   *
   * @param port the port
   * @return true if the port is unavailable for UDP
   */
  public static boolean isPortUnavailableForUdp(final int port) {
    try (final DatagramSocket datagramSocket = new DatagramSocket(null)) {
      datagramSocket.setReuseAddress(true);
      datagramSocket.bind(new InetSocketAddress(port));
      datagramSocket.close();
      return false;
    } catch (IOException ex) {
      LOG.trace(String.format("failed to open port %d for UDP", port), ex);
    }
    return true;
  }

  /**
   * Is hostAddress string an ip v4 address
   *
   * @param hostAddress the host address as a string
   * @return true if the host address is an ip v4 address
   */
  public static boolean isIpV4Address(final String hostAddress) {
    try {
      return InetAddresses.forString(hostAddress) instanceof Inet4Address;
    } catch (final IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Is hostAddress string an ip v6 address
   *
   * @param hostAddress the host address as a string
   * @return true if the host address is an ip v6 address
   */
  public static boolean isIpV6Address(final String hostAddress) {
    try {
      return InetAddresses.forString(hostAddress) instanceof Inet6Address;
    } catch (final IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Returns whether an IPv4 and IPv6 bind (host, port) pair should be served by a single dual-stack
   * socket rather than two independent per-family sockets: true only when both are the wildcard
   * address for their family and use the same port.
   *
   * <p>Two independent per-family sockets on the same port reliably {@code BindException} on Linux
   * regardless of bind order, so this case must bind one dual-stack IPv6 socket instead - verified
   * to correctly attribute both IPv4 and IPv6 traffic.
   *
   * @param ipv4Host the IPv4 bind host
   * @param ipv4Port the IPv4 bind port
   * @param ipv6Host the IPv6 bind host
   * @param ipv6Port the IPv6 bind port
   * @return true if both should be served by a single dual-stack socket
   */
  public static boolean isMergeableDualStackBind(
      final String ipv4Host, final int ipv4Port, final String ipv6Host, final int ipv6Port) {
    return ipv4Port == ipv6Port && isUnspecifiedAddress(ipv4Host) && isUnspecifiedAddress(ipv6Host);
  }
}
