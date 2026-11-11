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
package org.hyperledger.besu.ethereum.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;

import java.math.BigInteger;

import org.junit.jupiter.api.Test;

class SimulationCodeDelegationTest {

  @Test
  void detachedCopyKeepsTheGivenAuthority() {
    final Address authority = Address.fromHexString("0xabcd");
    final SimulationCodeDelegation delegation =
        new SimulationCodeDelegation(
            BigInteger.ONE, Address.fromHexString("0x1234"), "0x05", authority);

    final SimulationCodeDelegation copy = delegation.detachedCopy();

    assertThat(copy.authorizer()).contains(authority);
    assertThat(copy.chainId()).isEqualTo(BigInteger.ONE);
    assertThat(copy.address()).isEqualTo(Address.fromHexString("0x1234"));
    assertThat(copy.nonce()).isEqualTo(5L);
  }
}
