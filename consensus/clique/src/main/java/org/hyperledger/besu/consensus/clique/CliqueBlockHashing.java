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
package org.hyperledger.besu.consensus.clique;

import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;

/** The Clique block hashing. */
public class CliqueBlockHashing {
  /** Default constructor. */
  private CliqueBlockHashing() {}

  /**
   * Constructs a hash of the block header, suitable for use when creating the proposer seal. The
   * extra data is modified to have a null proposer seal and empty list of committed seals.
   *
   * @param header The header for which a proposer seal is to be calculated
   * @param cliqueExtraData The extra data block which is to be inserted to the header once seal is
   *     calculated
   * @return the hash of the header suitable for signing as the proposer seal
   */
  public static Hash calculateDataHashForProposerSeal(
      final BlockHeader header, final CliqueExtraData cliqueExtraData) {
    final Bytes headerRlp = serializeHeaderWithoutProposerSeal(header, cliqueExtraData);
    return Hash.hash(headerRlp); // Proposer hash is the hash of the RLP
  }

  /**
   * Recovers the proposer's {@link Address} from the proposer seal.
   *
   * @param header the block header that was signed by the proposer seal
   * @param cliqueExtraData the parsed CliqueExtraData from the header
   * @return the proposer address
   */
  public static Address recoverProposerAddress(
      final BlockHeader header, final CliqueExtraData cliqueExtraData) {
    final SECPSignature proposerSeal =
        cliqueExtraData
            .getProposerSeal()
            .orElseGet(
                () -> {
                  if (header.getNumber() == BlockHeader.GENESIS_BLOCK_NUMBER) {
                    return null;
                  }
                  throw new IllegalArgumentException(
                      "Supplied cliqueExtraData does not include a proposer seal");
                });
    if (proposerSeal == null) {
      return Address.ZERO;
    }
    final Hash proposerHash = calculateDataHashForProposerSeal(header, cliqueExtraData);
    return Util.signatureToAddress(proposerSeal, proposerHash);
  }

  private static Bytes serializeHeaderWithoutProposerSeal(
      final BlockHeader header, final CliqueExtraData cliqueExtraData) {
    return serializeHeader(header, () -> encodeExtraDataWithoutProposerSeal(cliqueExtraData));
  }

  private static Bytes encodeExtraDataWithoutProposerSeal(final CliqueExtraData cliqueExtraData) {
    final Bytes extraDataBytes = cliqueExtraData.encode();
    // Always trim off final 65 bytes (which maybe zeros)
    return extraDataBytes.slice(0, extraDataBytes.size() - SECPSignature.BYTES_REQUIRED);
  }

  private static Bytes serializeHeader(
      final BlockHeader header, final Supplier<Bytes> extraDataSerializer) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();

    out.writeBytes(header.getParentHash().getBytes());
    out.writeBytes(header.getOmmersHash().getBytes());
    out.writeBytes(header.getCoinbase().getBytes());
    out.writeBytes(header.getStateRoot().getBytes());
    out.writeBytes(header.getTransactionsRoot().getBytes());
    out.writeBytes(header.getReceiptsRoot().getBytes());
    out.writeBytes(header.getLogsBloom().getBytes());
    out.writeBytes(header.getDifficulty().toMinimalBytes());
    out.writeLongScalar(header.getNumber());
    out.writeLongScalar(header.getGasLimit());
    out.writeLongScalar(header.getGasUsed());
    out.writeLongScalar(header.getTimestamp());
    out.writeBytes(extraDataSerializer.get());
    out.writeBytes(header.getMixHash().getBytes());
    out.writeLong(header.getNonce());
    header.getBaseFee().ifPresent(out::writeUInt256Scalar);
    out.endList();
    return out.encoded();
  }
}
