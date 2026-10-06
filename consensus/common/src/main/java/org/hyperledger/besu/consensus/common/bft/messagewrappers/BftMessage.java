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
package org.hyperledger.besu.consensus.common.bft.messagewrappers;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.payload.Authored;
import org.hyperledger.besu.consensus.common.bft.payload.Payload;
import org.hyperledger.besu.consensus.common.bft.payload.RoundSpecific;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;

/**
 * The Bft message.
 *
 * @param <P> the type of Payload
 */
public class BftMessage<P extends Payload> implements Authored, RoundSpecific {

  /**
   * Maximum number of entries permitted in any embedded list (round-changes, prepares) when
   * decoding a BFT message.
   */
  public static final int MAX_LIST_ENTRIES = 512;

  private final SignedData<P> payload;

  /**
   * Instantiates a new Bft message.
   *
   * @param payload the payload
   */
  public BftMessage(final SignedData<P> payload) {
    this.payload = payload;
  }

  @Override
  public Address getAuthor() {
    return payload.getAuthor();
  }

  @Override
  public ConsensusRoundIdentifier getRoundIdentifier() {
    return payload.getPayload().getRoundIdentifier();
  }

  /**
   * Encode.
   *
   * @return the bytes
   */
  public Bytes encode() {
    final BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    payload.writeTo(rlpOut);
    return rlpOut.encoded();
  }

  /**
   * Gets signed payload.
   *
   * @return the signed payload
   */
  public SignedData<P> getSignedPayload() {
    return payload;
  }

  /**
   * Gets message type.
   *
   * @return the message type
   */
  public int getMessageType() {
    return payload.getPayload().getMessageType();
  }

  /**
   * Gets payload.
   *
   * @return the payload
   */
  protected P getPayload() {
    return payload.getPayload();
  }

  /**
   * Read payload.
   *
   * @param <T> the type parameter of Payload
   * @param rlpInput the rlp input
   * @param decoder the decoder
   * @return the signed data
   */
  protected static <T extends Payload> SignedData<T> readPayload(
      final RLPInput rlpInput, final Function<RLPInput, T> decoder) {
    rlpInput.enterList();
    final T unsignedMessageData = decoder.apply(rlpInput);
    final SECPSignature signature =
        rlpInput.readBytes((SignatureAlgorithmFactory.getInstance()::decodeSignature));
    rlpInput.leaveList();

    return SignedData.create(unsignedMessageData, signature);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", BftMessage.class.getSimpleName() + "[", "]")
        .add("payload=" + payload)
        .toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final BftMessage<?> that = (BftMessage<?>) o;
    return Objects.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(payload);
  }
}
