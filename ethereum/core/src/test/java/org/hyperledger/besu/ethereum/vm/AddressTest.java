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
package org.hyperledger.besu.ethereum.vm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class AddressTest {

  @Test
  public void accountAddressToString() {
    final Address addr =
        Address.wrap(Bytes.fromHexString("0x0000000000000000000000000000000000101010"));
    assertThat(addr.toString()).isEqualTo("0x0000000000000000000000000000000000101010");
  }

  @Test
  public void accountAddressEquals() {
    final Address addr =
        Address.wrap(Bytes.fromHexString("0x0000000000000000000000000000000000101010"));
    final Address addr2 =
        Address.wrap(Bytes.fromHexString("0x0000000000000000000000000000000000101010"));

    assertThat(addr2).isEqualByComparingTo(addr);
  }

  @Test
  public void accountAddressHashCode() {
    final Address addr =
        Address.wrap(Bytes.fromHexString("0x0000000000000000000000000000000000101010"));
    final Address addr2 =
        Address.wrap(Bytes.fromHexString("0x0000000000000000000000000000000000101010"));

    assertThat(addr2.hashCode()).isEqualTo(addr.hashCode());
  }

  @Test
  public void invalidAccountAddress() {
    final Bytes bytes = Bytes.fromHexString("0x00101010");
    assertThatThrownBy(() -> Address.wrap(bytes)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void tooShortAccountAddress() {
    assertThatThrownBy(() -> Address.fromHexStringStrict("0x00101010"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void nullAccountAddress() {
    assertThatThrownBy(() -> Address.fromHexStringStrict(null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void addressHashReturnsCorrectValue() {
    final Address address = Address.fromHexString("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
    assertThat(address.addressHash()).isEqualTo(Hash.hash(address.getBytes()));
  }

  @Test
  public void addressHashIsCached() {
    final Address address = Address.fromHexString("0x1234567890123456789012345678901234567890");
    assertThat(address.addressHash()).isSameAs(address.addressHash());
  }
}
