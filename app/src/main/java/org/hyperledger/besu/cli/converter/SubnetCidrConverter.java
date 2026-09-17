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
package org.hyperledger.besu.cli.converter;

import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import picocli.CommandLine;

/** Converts CIDR notation strings to IPAddress prefix blocks. Supports both IPv4 and IPv6. */
public class SubnetCidrConverter implements CommandLine.ITypeConverter<IPAddress> {
  /** Default Constructor. */
  public SubnetCidrConverter() {}

  /**
   * Converts an IP address with CIDR notation into an IPAddress prefix block.
   *
   * @param value The IP address with CIDR notation (e.g. "192.168.1.0/24" or "fd00::/64").
   * @return the IPAddress prefix block
   */
  @Override
  public IPAddress convert(final String value) {
    final IPAddressString addrString = new IPAddressString(value);
    if (!addrString.isValid() || addrString.getNetworkPrefixLength() == null) {
      throw new CommandLine.TypeConversionException(
          "Invalid CIDR notation: " + value + ". Expected format: <ip>/<prefix-length>");
    }
    return addrString.getAddress().toPrefixBlock();
  }
}
