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
package org.hyperledger.besu.consensus.common.bft.payload;

import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.math.BigInteger;
import java.util.Objects;
import java.util.StringJoiner;

import org.apache.tuweni.bytes.Bytes;

/**
 * The type Signed data.
 *
 * @param <M> the type parameter of Payload
 */
public class SignedData<M extends Payload> implements Authored {

  private static final BigInteger HALF_CURVE_ORDER =
      SignatureAlgorithmFactory.getInstance().getHalfCurveOrder();

  private final Address sender;
  private final SECPSignature signature;
  private final M unsignedPayload;

  /**
   * Create signed data.
   *
   * @param <T> the type parameter
   * @param payload the payload
   * @param signature the signature
   * @return the signed data
   * @throws IllegalArgumentException if the signature is invalid or does not recover to an address
   */
  public static <T extends Payload> SignedData<T> create(
      final T payload, final SECPSignature signature) {
    final Hash msgHash = payload.hashForSignature();
    return new SignedData<>(payload, Util.signatureToAddress(signature, msgHash), signature);
  }

  private SignedData(final M unsignedPayload, final Address sender, final SECPSignature signature) {
    if (!(signature.getS().compareTo(HALF_CURVE_ORDER) <= 0)) {
      throw new IllegalArgumentException("Signature is invalid");
    }
    // Util.signatureToAddress returns null when the signature does not recover. Signed entries
    // embedded in other messages (prepares, round changes) are not covered by the outer signature,
    // and the author is later looked up in natural-ordering sets that throw on null, so reject it
    // here, while the message is decoded and before it is validated or gossiped.
    if (sender == null) {
      throw new IllegalArgumentException("Signature does not recover to an address");
    }
    this.unsignedPayload = unsignedPayload;
    this.sender = sender;
    this.signature = signature;
  }

  @Override
  public Address getAuthor() {
    return sender;
  }

  /**
   * Gets payload.
   *
   * @return the payload
   */
  public M getPayload() {
    return unsignedPayload;
  }

  /**
   * Write to.
   *
   * @param output the output
   */
  public void writeTo(final RLPOutput output) {
    output.startList();
    unsignedPayload.writeTo(output);
    output.writeBytes(signature.encodedBytes());
    output.endList();
  }

  /**
   * Encode.
   *
   * @return the bytes
   */
  public Bytes encode() {
    final BytesValueRLPOutput rlpEncode = new BytesValueRLPOutput();
    writeTo(rlpEncode);
    return rlpEncode.encoded();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SignedData<?> that = (SignedData<?>) o;
    return Objects.equals(sender, that.sender)
        && Objects.equals(signature, that.signature)
        && Objects.equals(unsignedPayload, that.unsignedPayload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sender, signature, unsignedPayload);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", SignedData.class.getSimpleName() + "[", "]")
        .add("sender=" + sender)
        .add("signature=" + signature)
        .add("unsignedPayload=" + unsignedPayload)
        .toString();
  }
}
