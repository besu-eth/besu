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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import inet.ipaddr.IPAddress;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

public class SubnetCidrConverterTest {

  @Test
  void testCreateIpRestrictionHandlerWithValidSubnets() {
    String subnet = "192.168.1.0/24";
    IPAddress result = parseSubnetRules(subnet);
    assertThat(result.toString()).isEqualTo(subnet);
  }

  @Test
  void testCreateIpRestrictionHandlerWithValidIpv6Subnet() {
    IPAddress result = parseSubnetRules("fd00::/64");
    assertThat(result.toString()).isEqualTo("fd00::/64");
  }

  @Test
  void testCreateIpRestrictionHandlerWithInvalidSubnet() {
    assertThatThrownBy(() -> parseSubnetRules("abc"))
        .isInstanceOf(CommandLine.TypeConversionException.class);
  }

  @Test
  void testCreateIpRestrictionHandlerMissingCIDR() {
    assertThatThrownBy(() -> parseSubnetRules("192.168.1.0"))
        .isInstanceOf(CommandLine.TypeConversionException.class);
  }

  @Test
  void testCreateIpRestrictionHandlerBigCIDR() {
    assertThatThrownBy(() -> parseSubnetRules("192.168.1.0:25"))
        .isInstanceOf(CommandLine.TypeConversionException.class);
  }

  @Test
  void testCreateIpRestrictionHandlerWithInvalidCIDR() {
    assertThatThrownBy(() -> parseSubnetRules("192.168.1.0/abc"))
        .isInstanceOf(CommandLine.TypeConversionException.class);
  }

  @Test
  void testCreateIpRestrictionHandlerWithEmptyString() {
    assertThatThrownBy(() -> parseSubnetRules(""))
        .isInstanceOf(CommandLine.TypeConversionException.class);
  }

  private IPAddress parseSubnetRules(final String subnet) {
    return new SubnetCidrConverter().convert(subnet);
  }
}
