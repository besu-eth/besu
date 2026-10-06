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
package org.hyperledger.besu.ethereum.chain.pluginadapter;

import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptDecoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.plugin.data.BlockBody;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.data.TransactionReceipt;
import org.hyperledger.besu.plugin.services.rlp.RlpConverterService;

import org.apache.tuweni.bytes.Bytes;

/** RLP Serialization/Deserialization service. */
public class RlpConverterServiceImpl implements RlpConverterService {

  private final BlockHeaderFunctions blockHeaderFunctions;

  /**
   * Constructor for RlpConverterServiceImpl.
   *
   * @param protocolSchedule the protocol schedule.
   */
  public RlpConverterServiceImpl(final ProtocolSchedule protocolSchedule) {
    this.blockHeaderFunctions = ScheduleBasedBlockHeaderFunctions.create(protocolSchedule);
  }

  @Override
  public BlockHeader buildHeaderFromRlp(final Bytes rlp) {
    return org.hyperledger.besu.ethereum.core.BlockHeader.readFrom(
        RLP.input(rlp), blockHeaderFunctions);
  }

  @Override
  public BlockBody buildBodyFromRlp(final Bytes rlp) {
    return org.hyperledger.besu.ethereum.core.BlockBody.readWrappedBodyFrom(
        RLP.input(rlp), blockHeaderFunctions);
  }

  @Override
  public TransactionReceipt buildReceiptFromRlp(final Bytes rlp) {
    return TransactionReceiptDecoder.readFrom(RLP.input(rlp), true);
  }

  @Override
  public Bytes buildRlpFromHeader(final BlockHeader blockHeader) {
    return RLP.encode(
        org.hyperledger.besu.ethereum.core.BlockHeader.convertPluginBlockHeader(
                blockHeader, blockHeaderFunctions)
            ::writeTo);
  }

  @Override
  public Bytes buildRlpFromBody(final BlockBody blockBody) {
    return RLP.encode(
        rlpOutput ->
            ((org.hyperledger.besu.ethereum.core.BlockBody) blockBody)
                .writeWrappedBodyTo(rlpOutput));
  }

  @Override
  public Bytes buildRlpFromReceipt(final TransactionReceipt receipt) {
    return RLP.encode(
        rlpOutput ->
            TransactionReceiptEncoder.writeTo(
                (org.hyperledger.besu.ethereum.core.TransactionReceipt) receipt,
                rlpOutput,
                TransactionReceiptEncodingConfiguration.DEFAULT));
  }
}
