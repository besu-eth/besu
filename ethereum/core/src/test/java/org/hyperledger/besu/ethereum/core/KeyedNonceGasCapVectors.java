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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionDecoder;

import org.apache.tuweni.bytes.Bytes;

/**
 * The frame transactions of execution-specs devnets/bogota/1
 * test_gas_limit_cap_counts_keyed_nonce_access_transaction (tests-bogota-devnet@v1.1.1): a VERIFY
 * frame from sender 0x2129..7e17 with the 16 nonce keys 1..16, whose 16 x 12,100 = 193,600 keyed
 * nonce access gas takes max(intrinsic + execution gas, calldata floor) to the EIP-7825 cap of 2^24
 * or just past it. Without the keyed nonce access cost all four would sit 193,600 lower.
 *
 * <p>The floor variants carry a second frame of zero bytes, spliced in here rather than spelled
 * out.
 */
public final class KeyedNonceGasCapVectors {

  /** The EIP-7825 transaction gas limit cap the vectors are built against. */
  public static final long CAP = 16_777_216L;

  private static final String AT_CAP_STANDARD =
      "0x06f83e01d00102030405060708090a0b0c0d0e0f1080942129976602d58c0b4bd97278eb25aeeec63a7e17cf"
          + "ce010380c883fcd9e58317e8008080c0c3800780c0";

  private static final String ABOVE_CAP_STANDARD =
      "0x06f83e01d00102030405060708090a0b0c0d0e0f1080942129976602d58c0b4bd97278eb25aeeec63a7e17cf"
          + "ce010380c883fcd9e68317e8008080c0c3800780c0";

  private static final String AT_CAP_FLOOR_HEAD =
      "0x06fa03f3a501d00102030405060708090a0b0c0d0e0f1080942129976602d58c0b4bd97278eb25aeeec63a7e17"
          + "fa03f373ce010380c8830186a08317e8008080fa03f360808080c58083061a8080ba03f352";

  private static final String ABOVE_CAP_FLOOR_HEAD =
      "0x06fa03f3a601d00102030405060708090a0b0c0d0e0f1080942129976602d58c0b4bd97278eb25aeeec63a7e17"
          + "fa03f374ce010380c8830186a08317e8008080fa03f361808080c58083061a8080ba03f353";

  /** The empty signature list, the fees and the empty blob hash list that close a vector. */
  private static final String FLOOR_TAIL = "0xc0c3800780c0";

  private KeyedNonceGasCapVectors() {}

  /**
   * Intrinsic plus execution gas is exactly the cap; valid.
   *
   * @return the transaction
   */
  public static Transaction atCapStandard() {
    return decode(Bytes.fromHexString(AT_CAP_STANDARD));
  }

  /**
   * One more unit of frame execution gas than {@link #atCapStandard()}; over the cap.
   *
   * @return the transaction
   */
  public static Transaction aboveCapStandard() {
    return decode(Bytes.fromHexString(ABOVE_CAP_STANDARD));
  }

  /**
   * The calldata floor binds, 42 under the cap; valid.
   *
   * @return the transaction
   */
  public static Transaction atCapFloor() {
    return decode(floorVariant(AT_CAP_FLOOR_HEAD, 258_898));
  }

  /**
   * One more zero byte than {@link #atCapFloor()}, which takes the calldata floor 22 over the cap.
   *
   * @return the transaction
   */
  public static Transaction aboveCapFloor() {
    return decode(floorVariant(ABOVE_CAP_FLOOR_HEAD, 258_899));
  }

  private static Bytes floorVariant(final String head, final int zeroBytes) {
    return Bytes.concatenate(
        Bytes.fromHexString(head),
        Bytes.wrap(new byte[zeroBytes]),
        Bytes.fromHexString(FLOOR_TAIL));
  }

  private static Transaction decode(final Bytes encoded) {
    return TransactionDecoder.decodeOpaqueBytes(encoded, EncodingContext.BLOCK_BODY);
  }
}
