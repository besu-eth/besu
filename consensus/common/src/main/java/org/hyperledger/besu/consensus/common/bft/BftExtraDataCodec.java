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
package org.hyperledger.besu.consensus.common.bft;

import org.hyperledger.besu.ethereum.core.BlockHeader;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Bft extra data codec. */
public abstract class BftExtraDataCodec {

  /** The enum Encoding type. */
  protected enum EncodingType {
    /** All encoding type. */
    ALL,
    /** Exclude commit seals encoding type. */
    EXCLUDE_COMMIT_SEALS,
    /** Exclude commit seals and round number encoding type. */
    EXCLUDE_COMMIT_SEALS_AND_ROUND_NUMBER
  }

  private static final Logger LOG = LoggerFactory.getLogger(BftExtraDataCodec.class);

  /** The constant EXTRA_VANITY_LENGTH. */
  public static int EXTRA_VANITY_LENGTH = 32;

  /** Default constructor. */
  public BftExtraDataCodec() {}

  /**
   * Encode.
   *
   * @param bftExtraData the bft extra data
   * @return the bytes
   */
  public Bytes encode(final BftExtraData bftExtraData) {
    return encode(bftExtraData, EncodingType.ALL);
  }

  /**
   * Encode without commit seals.
   *
   * @param bftExtraData the bft extra data
   * @return the bytes
   */
  public Bytes encodeWithoutCommitSeals(final BftExtraData bftExtraData) {
    return encode(bftExtraData, EncodingType.EXCLUDE_COMMIT_SEALS);
  }

  /**
   * Encode without commit seals and round number.
   *
   * @param bftExtraData the bft extra data
   * @return the bytes
   */
  public Bytes encodeWithoutCommitSealsAndRoundNumber(final BftExtraData bftExtraData) {
    return encode(bftExtraData, EncodingType.EXCLUDE_COMMIT_SEALS_AND_ROUND_NUMBER);
  }

  /**
   * Encode.
   *
   * @param bftExtraData the bft extra data
   * @param encodingType the encoding type
   * @return the bytes
   */
  protected abstract Bytes encode(final BftExtraData bftExtraData, final EncodingType encodingType);

  /**
   * Decode.
   *
   * @param blockHeader the block header
   * @return the bft extra data
   */
  public BftExtraData decode(final BlockHeader blockHeader) {
    final Object inputExtraData = blockHeader.getParsedExtraData();
    if (inputExtraData instanceof BftExtraData bftExtraData) {
      return bftExtraData;
    }
    LOG.warn(
        "Expected a BftExtraData instance but got {}. Reparsing required.",
        inputExtraData != null ? inputExtraData.getClass().getName() : "null");
    return decodeRaw(blockHeader.getExtraData());
  }

  /**
   * Decode raw.
   *
   * @param bytes the bytes
   * @return the bft extra data
   */
  public abstract BftExtraData decodeRaw(Bytes bytes);
}
