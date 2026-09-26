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
package org.hyperledger.besu.consensus.qbft.core.messagewrappers;

import org.hyperledger.besu.consensus.common.bft.messagewrappers.BftMessage;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.qbft.core.payload.PreparePayload;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import org.apache.tuweni.bytes.Bytes;

/** The Prepare payload message. */
public class Prepare extends BftMessage<PreparePayload> {

  /**
   * Instantiates a new Prepare.
   *
   * @param payload the payload
   */
  public Prepare(final SignedData<PreparePayload> payload) {
    super(payload);
  }

  /**
   * Gets digest.
   *
   * @return the digest
   */
  public Hash getDigest() {
    return getPayload().getDigest();
  }

  /**
   * Decode.
   *
   * @param data the data
   * @return the Prepare payload message
   */
  public static Prepare decode(final Bytes data) {
    final RLPInput rlpIn = RLP.input(data);
    return new Prepare(readPayload(rlpIn, PreparePayload::readFrom));
  }

  /**
   * Reads only the sequence number (block height) from the encoded message without full decode.
   *
   * @param data the raw encoded message bytes
   * @return the sequence number
   */
  public static long decodeSequence(final Bytes data) {
    final RLPInput rlp = RLP.input(data);
    rlp.enterList(); // signed-data wrapper
    rlp.enterList(); // PreparePayload
    return rlp.readLongScalar();
  }
}
