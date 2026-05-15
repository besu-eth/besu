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
package org.hyperledger.besu.ethereum.eth.messages.snap;

import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public final class StorageRangeMessageTest {

  @Test
  public void roundTripTest() {

    final ArrayDeque<NavigableMap<Bytes32, Bytes>> keys = new ArrayDeque<>();
    final TreeMap<Bytes32, Bytes> storage = new TreeMap<>();
    storage.put(Bytes32.leftPad(Bytes.of(1)), Bytes32.random());
    keys.add(storage);

    final List<Bytes> proofs = new ArrayList<>();
    proofs.add(Bytes32.random());

    // Perform round-trip transformation
    final MessageData initialMessage = StorageRangeMessage.create(keys, proofs);
    final MessageData raw = new RawMessage(SnapV1.STORAGE_RANGE, initialMessage.getData());

    final StorageRangeMessage message = StorageRangeMessage.readFrom(raw);

    // check match originals.
    final StorageRangeMessage.SlotRangeData range = message.slotsData(false);
    Assertions.assertThat(range.slots()).isEqualTo(keys);
    Assertions.assertThat(range.proofs()).isEqualTo(proofs);
  }

  @Test
  public void wrapRoundTripTest() {
    final ArrayDeque<NavigableMap<Bytes32, Bytes>> keys = new ArrayDeque<>();
    final TreeMap<Bytes32, Bytes> storage = new TreeMap<>();
    storage.put(Bytes32.leftPad(Bytes.of(1)), Bytes32.random());
    keys.add(storage);

    final List<Bytes> proofs = new ArrayList<>();
    proofs.add(Bytes32.random());

    final StorageRangeMessage initialMessage = StorageRangeMessage.create(keys, proofs);
    final MessageData wrapped = initialMessage.wrapMessageData(BigInteger.valueOf(42));
    final MessageData raw = new RawMessage(SnapV1.STORAGE_RANGE, wrapped.getData());

    final StorageRangeMessage message = StorageRangeMessage.readFrom(raw);

    final StorageRangeMessage.SlotRangeData range = message.slotsData(true);
    Assertions.assertThat(range.slots()).isEqualTo(keys);
    Assertions.assertThat(range.proofs()).isEqualTo(proofs);
  }
}
