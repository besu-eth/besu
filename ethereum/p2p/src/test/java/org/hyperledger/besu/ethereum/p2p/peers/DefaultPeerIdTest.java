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
package org.hyperledger.besu.ethereum.p2p.peers;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class DefaultPeerIdTest {

  private static final Bytes ID_1 = Bytes.fromHexString("0x1234");
  private static final Bytes ID_2 = Bytes.fromHexString("0x5678");

  @Test
  public void equals_sameId_returnsTrue() {
    final DefaultPeerId a = new DefaultPeerId(ID_1);
    final DefaultPeerId b = new DefaultPeerId(ID_1);
    assertThat(a).isEqualTo(b);
  }

  @Test
  public void equals_differentId_returnsFalse() {
    final DefaultPeerId a = new DefaultPeerId(ID_1);
    final DefaultPeerId b = new DefaultPeerId(ID_2);
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  public void equals_null_returnsFalse() {
    final DefaultPeerId a = new DefaultPeerId(ID_1);
    assertThat(a).isNotEqualTo(null);
  }

  @Test
  public void equals_differentType_returnsFalse() {
    final DefaultPeerId a = new DefaultPeerId(ID_1);
    assertThat(a).isNotEqualTo("not-a-peer-id");
  }

  @Test
  public void hashCode_sameId_returnsSameHash() {
    final DefaultPeerId a = new DefaultPeerId(ID_1);
    final DefaultPeerId b = new DefaultPeerId(ID_1);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }
}
