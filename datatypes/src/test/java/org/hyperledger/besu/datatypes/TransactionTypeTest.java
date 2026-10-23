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
package org.hyperledger.besu.datatypes;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class TransactionTypeTest {

  @Test
  void frameIsType0x06() {
    assertThat(TransactionType.FRAME.getSerializedType()).isEqualTo((byte) 0x06);
    assertThat(TransactionType.FRAME.getEthSerializedType()).isEqualTo((byte) 0x06);
    assertThat(TransactionType.fromOpaque((byte) 0x06)).contains(TransactionType.FRAME);
    assertThat(TransactionType.fromEthSerializedType((byte) 0x06)).contains(TransactionType.FRAME);
  }

  /** The serialized types are sparse: nothing is assigned to 0x05, and nothing past FRAME. */
  @ParameterizedTest(name = "type byte {0}")
  @ValueSource(ints = {0x05, 0x07, 0x7f})
  void unassignedTypeBytesHaveNoType(final int typeByte) {
    assertThat(TransactionType.fromOpaque((byte) typeByte)).isEmpty();
    assertThat(TransactionType.fromEthSerializedType((byte) typeByte)).isEmpty();
  }

  @Test
  void negativeEthSerializedTypesHaveNoType() {
    assertThat(TransactionType.fromEthSerializedType((byte) 0xf8)).isEmpty();
    assertThat(TransactionType.fromEthSerializedType((byte) 0xff)).isEmpty();
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(TransactionType.class)
  void everyTypeRoundTripsThroughItsSerializedTypes(final TransactionType type) {
    assertThat(TransactionType.fromEthSerializedType(type.getEthSerializedType())).contains(type);
    assertThat(TransactionType.fromOpaque(type.getSerializedType())).contains(type);
  }

  @Test
  void legacyOpaqueBytesStillMapToFrontier() {
    assertThat(TransactionType.fromOpaque((byte) 0xc0)).contains(TransactionType.FRONTIER);
    assertThat(TransactionType.fromOpaque((byte) 0xf8)).contains(TransactionType.FRONTIER);
    assertThat(TransactionType.fromEthSerializedType((byte) 0x00))
        .contains(TransactionType.FRONTIER);
  }

  @Test
  void onlyFrameSupportsFrames() {
    assertThat(Arrays.stream(TransactionType.values()).filter(TransactionType::supportsFrames))
        .containsExactly(TransactionType.FRAME);
  }

  /** A frame transaction may carry blobs, so FRAME joins BLOB as a blob-capable type. */
  @Test
  void blobAndFrameSupportBlobs() {
    assertThat(Arrays.stream(TransactionType.values()).filter(TransactionType::supportsBlob))
        .containsExactly(TransactionType.BLOB, TransactionType.FRAME);
  }

  @Test
  void frameCapabilities() {
    final TransactionType frame = TransactionType.FRAME;
    assertThat(frame.requiresChainId()).isTrue();
    assertThat(frame.supports1559FeeMarket()).isTrue();
    assertThat(frame.supportsAccessList()).isFalse();
    assertThat(frame.supportsDelegateCode()).isFalse();
  }

  @Test
  void existingTypesKeepTheirCapabilities() {
    for (final TransactionType type :
        new TransactionType[] {
          TransactionType.FRONTIER,
          TransactionType.ACCESS_LIST,
          TransactionType.EIP1559,
          TransactionType.BLOB,
          TransactionType.DELEGATE_CODE
        }) {
      assertThat(type.supportsFrames()).as("%s supportsFrames", type).isFalse();
      assertThat(type.supportsBlob())
          .as("%s supportsBlob", type)
          .isEqualTo(type == TransactionType.BLOB);
      assertThat(type.supportsDelegateCode())
          .as("%s supportsDelegateCode", type)
          .isEqualTo(type == TransactionType.DELEGATE_CODE);
      assertThat(type.requiresChainId())
          .as("%s requiresChainId", type)
          .isEqualTo(type != TransactionType.FRONTIER);
      assertThat(type.supportsAccessList())
          .as("%s supportsAccessList", type)
          .isEqualTo(type != TransactionType.FRONTIER);
      assertThat(type.supports1559FeeMarket())
          .as("%s supports1559FeeMarket", type)
          .isEqualTo(type != TransactionType.FRONTIER && type != TransactionType.ACCESS_LIST);
    }
  }
}
