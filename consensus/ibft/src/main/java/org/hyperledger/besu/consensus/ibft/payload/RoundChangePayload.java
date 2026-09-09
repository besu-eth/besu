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
package org.hyperledger.besu.consensus.ibft.payload;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.ibft.messagedata.IbftV2;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

/** The Round change payload. */
public class RoundChangePayload extends IbftPayload {
  private static final int TYPE = IbftV2.ROUND_CHANGE;
  private final ConsensusRoundIdentifier roundChangeIdentifier;

  // The validator may not hae any prepared certificate
  private final Optional<PreparedCertificate> preparedCertificate;

  /**
   * Instantiates a new Round change payload.
   *
   * @param roundIdentifier the round identifier
   * @param preparedCertificate the prepared certificate
   */
  public RoundChangePayload(
      final ConsensusRoundIdentifier roundIdentifier,
      final Optional<PreparedCertificate> preparedCertificate) {
    this.roundChangeIdentifier = roundIdentifier;
    this.preparedCertificate = preparedCertificate;
  }

  @Override
  public ConsensusRoundIdentifier getRoundIdentifier() {
    return roundChangeIdentifier;
  }

  /**
   * Gets prepared certificate.
   *
   * @return the prepared certificate
   */
  public Optional<PreparedCertificate> getPreparedCertificate() {
    return preparedCertificate;
  }

  @Override
  public void writeTo(final RLPOutput rlpOutput) {
    // RLP encode of the message data content (round identifier and prepared certificate)
    rlpOutput.startList();
    roundChangeIdentifier.writeTo(rlpOutput);

    if (preparedCertificate.isPresent()) {
      preparedCertificate.get().writeTo(rlpOutput);
    } else {
      rlpOutput.writeNull();
    }
    rlpOutput.endList();
  }

  /**
   * Read from rlp input and return round change payload limiting decoding to supplied decode
   * budget.
   *
   * @param rlpInput the rlp input
   * @param decodeBudget the per-message decode budget
   * @return the round change payload
   */
  public static RoundChangePayload readFrom(
      final RLPInput rlpInput, final DecodeBudget decodeBudget) {
    rlpInput.enterList();
    final ConsensusRoundIdentifier roundIdentifier = ConsensusRoundIdentifier.readFrom(rlpInput);

    final Optional<PreparedCertificate> preparedCertificate;

    if (rlpInput.nextIsNull()) {
      rlpInput.skipNext();
      preparedCertificate = Optional.empty();
    } else {
      preparedCertificate = Optional.of(PreparedCertificate.readFrom(rlpInput, decodeBudget));
    }
    rlpInput.leaveList();

    return new RoundChangePayload(roundIdentifier, preparedCertificate);
  }

  @Override
  public int getMessageType() {
    return TYPE;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final RoundChangePayload that = (RoundChangePayload) o;
    return Objects.equals(roundChangeIdentifier, that.roundChangeIdentifier)
        && Objects.equals(preparedCertificate, that.preparedCertificate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(roundChangeIdentifier, preparedCertificate);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", RoundChangePayload.class.getSimpleName() + "[", "]")
        .add("roundChangeIdentifier=" + roundChangeIdentifier)
        .add("preparedCertificate=" + preparedCertificate)
        .toString();
  }
}
