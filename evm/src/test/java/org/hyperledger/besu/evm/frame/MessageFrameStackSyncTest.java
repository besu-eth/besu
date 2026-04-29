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
package org.hyperledger.besu.evm.frame;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

/** Tests for MessageFrame.syncStackV2ToV1 and syncStackV1ToV2. */
class MessageFrameStackSyncTest {

  private MessageFrame emptyV2Frame() {
    return new TestMessageFrameBuilderV2().build();
  }

  /** Push a UInt256 value onto the V2 stack at the current top slot. */
  private void pushV2(final MessageFrame frame, final UInt256 value) {
    final long[] s = frame.stackDataV2();
    final int dst = frame.stackTopV2() << 2;
    s[dst] = value.u3();
    s[dst + 1] = value.u2();
    s[dst + 2] = value.u1();
    s[dst + 3] = value.u0();
    frame.setTopV2(frame.stackTopV2() + 1);
  }

  /** Read V2 slot i (0 = bottom) as a UInt256. */
  private UInt256 readV2Slot(final MessageFrame frame, final int i) {
    final long[] s = frame.stackDataV2();
    final int base = i << 2;
    return new UInt256(s[base], s[base + 1], s[base + 2], s[base + 3]);
  }

  // ── syncStackV2ToV1 ────────────────────────────────────────────────────────

  @Test
  void syncV2ToV1_empty_doesNotThrow() {
    final MessageFrame frame = emptyV2Frame();
    frame.syncStackV2ToV1();
    assertThat(frame.stackSize()).isZero();
  }

  @Test
  void syncV2ToV1_singleItem_appearsOnV1Top() {
    final MessageFrame frame = emptyV2Frame();
    final UInt256 val =
        UInt256.fromBytesBE(
            Bytes32.fromHexString(
                    "0x000000000000000000000000000000000000000000000000000000000000002a")
                .toArrayUnsafe());
    pushV2(frame, val);

    frame.syncStackV2ToV1();

    assertThat(frame.stackSize()).isEqualTo(1);
    assertThat(frame.getStackItem(0)).isEqualTo(Bytes.wrap(val.toBytesBE()));
  }

  @Test
  void syncV2ToV1_ordering_bottomBecomesV1Bottom_topBecomesV1Top() {
    final MessageFrame frame = emptyV2Frame();
    final UInt256 bottom = new UInt256(0, 0, 0, 1); // pushed first → V2 slot 0
    final UInt256 top = new UInt256(0, 0, 0, 2); // pushed second → V2 slot 1 (TOS)
    pushV2(frame, bottom);
    pushV2(frame, top);

    frame.syncStackV2ToV1();

    assertThat(frame.stackSize()).isEqualTo(2);
    // V1 index 0 = TOS → should be the V2 top value
    assertThat(frame.getStackItem(0)).isEqualTo(Bytes.wrap(top.toBytesBE()));
    // V1 index 1 = bottom → should be the V2 bottom value
    assertThat(frame.getStackItem(1)).isEqualTo(Bytes.wrap(bottom.toBytesBE()));
  }

  @Test
  void syncV2ToV1_clearsExistingV1Items() {
    final MessageFrame frame = emptyV2Frame();
    frame.pushStackItem(Bytes.wrap(new UInt256(0, 0, 0, 99).toBytesBE())); // pre-populate V1
    pushV2(frame, new UInt256(0, 0, 0, 7));

    frame.syncStackV2ToV1();

    assertThat(frame.stackSize()).isEqualTo(1);
    assertThat(Bytes.wrap(new UInt256(0, 0, 0, 7).toBytesBE())).isEqualTo(frame.getStackItem(0));
  }

  @Test
  void syncV1ToV2_empty_stackTopV2IsZero() {
    final MessageFrame frame = emptyV2Frame();
    frame.syncStackV1ToV2();
    assertThat(frame.stackTopV2()).isZero();
  }

  @Test
  void syncV1ToV2_singleFullItem_appearsAtV2TopSlot() {
    final MessageFrame frame = emptyV2Frame();
    final UInt256 val = new UInt256(0xDEAD_BEEF_00000000L, 0, 0, 0x1234L);
    frame.pushStackItem(Bytes.wrap(val.toBytesBE()));

    frame.syncStackV1ToV2();

    assertThat(frame.stackTopV2()).isEqualTo(1);
    assertThat(readV2Slot(frame, 0)).isEqualTo(val);
  }

  @Test
  void syncV1ToV2_shortBytes_leftPadded() {
    final MessageFrame frame = emptyV2Frame();
    // Simulates LEGACY_SUCCESS_STACK_ITEM = Bytes.of(1)
    frame.pushStackItem(Bytes.of(1));

    frame.syncStackV1ToV2();

    assertThat(frame.stackTopV2()).isEqualTo(1);
    assertThat(readV2Slot(frame, 0)).isEqualTo(UInt256.ONE);
  }

  @Test
  void syncV1ToV2_ordering_V1TopBecomesV2TopSlot() {
    final MessageFrame frame = emptyV2Frame();
    final UInt256 bottom = new UInt256(0, 0, 0, 1);
    final UInt256 top = new UInt256(0, 0, 0, 2);
    // V1 push: bottom goes in first (becomes V1 index 1), top goes in second (V1 index 0 = TOS)
    frame.pushStackItem(Bytes.wrap(bottom.toBytesBE()));
    frame.pushStackItem(Bytes.wrap(top.toBytesBE()));

    frame.syncStackV1ToV2();

    assertThat(frame.stackTopV2()).isEqualTo(2);
    assertThat(readV2Slot(frame, 0)).isEqualTo(bottom); // V2 slot 0 = bottom
    assertThat(readV2Slot(frame, 1)).isEqualTo(top); // V2 slot 1 = TOS
  }

  @Test
  void roundTrip_V2ToV1ToV2_preservesValues() {
    final MessageFrame frame = emptyV2Frame();
    final UInt256 a = new UInt256(0xCAFE_BABE_0000_0000L, 0xDEAD_BEEF_0000_0000L, 0, 1);
    final UInt256 b = new UInt256(0, 0, 0xFF_FF_FF_FFL, 42);
    final UInt256 c = UInt256.ONE;
    pushV2(frame, a);
    pushV2(frame, b);
    pushV2(frame, c);

    frame.syncStackV2ToV1();
    frame.syncStackV1ToV2();

    assertThat(frame.stackTopV2()).isEqualTo(3);
    assertThat(readV2Slot(frame, 0)).isEqualTo(a);
    assertThat(readV2Slot(frame, 1)).isEqualTo(b);
    assertThat(readV2Slot(frame, 2)).isEqualTo(c);
  }
}
