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
package org.hyperledger.besu.consensus.ibftlegacy;

import org.hyperledger.besu.consensus.common.bft.BftExtraData;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.datatypes.Address;

import java.util.Collection;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.jspecify.annotations.Nullable;

/** The Ibft Legacy extra data. */
public class IbftLegacyExtraData extends BftExtraData {

  private final @Nullable SECPSignature proposerSeal;

  /**
   * Instantiates a new Bft extra data.
   *
   * @param vanityData the vanity data
   * @param seals the seals
   * @param proposerSeal the proposer seal, or {@code null} when proposer seal is not present (for
   *     example pre-seal/unsigned header contexts)
   * @param validators the validators
   */
  public IbftLegacyExtraData(
      final Bytes vanityData,
      final Collection<SECPSignature> seals,
      final @Nullable SECPSignature proposerSeal,
      final Collection<Address> validators) {
    super(vanityData, seals, Optional.empty(), 0, validators);
    this.proposerSeal = proposerSeal;
  }

  /**
   * Gets proposer seal.
   *
   * @return the proposer seal, or {@code null} when the header does not carry a proposer seal (for
   *     example pre-seal/unsigned header contexts); callers should null-check before use
   */
  public @Nullable SECPSignature getProposerSeal() {
    return proposerSeal;
  }

  @Override
  public String toString() {
    return "IbftLegacyExtraData{" + super.toString() + ", proposerSeal=" + proposerSeal + '}';
  }
}
