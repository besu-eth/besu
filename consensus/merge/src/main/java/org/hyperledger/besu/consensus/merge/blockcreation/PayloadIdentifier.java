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
package org.hyperledger.besu.consensus.merge.blockcreation;

import org.hyperledger.besu.datatypes.Quantity;
import org.hyperledger.besu.ethereum.core.Withdrawal;

import java.math.BigInteger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.tuweni.units.bigints.UInt64;

/** The Payload identifier. */
public class PayloadIdentifier implements Quantity {
  private final UInt64 val;

  /**
   * Instantiates a new Payload identifier.
   *
   * @param payloadId the payload id
   */
  @JsonCreator
  public PayloadIdentifier(final String payloadId) {
    this(Long.decode(payloadId));
  }

  /**
   * Instantiates a new Payload identifier.
   *
   * @param payloadId the payload id
   */
  public PayloadIdentifier(final Long payloadId) {
    this.val = UInt64.valueOf(Math.abs(payloadId));
  }

  /**
   * Create payload identifier for payload params. This is a deterministic hash of all payload
   * parameters that aims to avoid collisions
   *
   * @param preparePayloadArgs the payload parameters
   * @return the payload identifier
   */
  public static PayloadIdentifier forPayloadParams(
      final MergeMiningCoordinator.PreparePayloadArgs preparePayloadArgs) {

    // normally timestamp and parentHash should be enough to uniquely identify a payload
    // but in special cases, reorgs, CL configuration changes (feeRecipient), or other edge case
    // reasons CL may change other params, so for extra safety we include all the fields in
    // the payload generation process

    final long parentBeaconBlockRootPart =
        preparePayloadArgs
            .parentBeaconBlockRoot()
            .map(b32 -> (long) b32.hashCode())
            .orElse(Long.MAX_VALUE);

    // for withdrawals the order in the list is not important so we sum all the hashCode
    final long withdrawalPart =
        preparePayloadArgs
            .withdrawals()
            .map(ws -> ws.stream().mapToLong(Withdrawal::hashCode).sum())
            .orElse(-1L);

    final long slotNumberPart = preparePayloadArgs.slotNumber().orElse(-1L);

    // we finally spread all the values over 64bit, rotating only values where the shift could lose
    // bits
    return new PayloadIdentifier(
        preparePayloadArgs.timestamp()
            ^ ((long) preparePayloadArgs.parentHeader().getHash().getBytes().hashCode()) << 8
            ^ ((long) preparePayloadArgs.prevRandao().hashCode()) << 16
            ^ ((long) preparePayloadArgs.feeRecipient().getBytes().hashCode()) << 24
            ^ parentBeaconBlockRootPart << 32
            ^ slotNumberPart << 40
            ^ slotNumberPart >> 24
            ^ withdrawalPart << 48
            ^ withdrawalPart >> 16);
  }

  @Override
  public BigInteger getAsBigInteger() {
    return val.toBigInteger();
  }

  @Override
  public String toHexString() {
    return val.toHexString();
  }

  @Override
  public String toShortHexString() {
    var shortHex = val.toShortHexString();
    if (shortHex.length() % 2 != 0) {
      shortHex = "0x0" + shortHex.substring(2);
    }
    return shortHex;
  }

  /**
   * Serialize to hex string.
   *
   * @return the string
   */
  @JsonValue
  public String serialize() {
    return toShortHexString();
  }

  @Override
  public boolean equals(final Object o) {
    if (o instanceof PayloadIdentifier) {
      return getAsBigInteger().equals(((PayloadIdentifier) o).getAsBigInteger());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return val.hashCode();
  }

  @Override
  public String toString() {
    return toHexString();
  }
}
