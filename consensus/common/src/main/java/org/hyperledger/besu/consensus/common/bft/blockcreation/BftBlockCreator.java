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
package org.hyperledger.besu.consensus.common.bft.blockcreation;

import org.hyperledger.besu.config.BftConfigOptions;
import org.hyperledger.besu.consensus.common.ForksSchedule;
import org.hyperledger.besu.consensus.common.bft.BftBlockHeaderFunctions;
import org.hyperledger.besu.consensus.common.bft.BftExtraDataCodec;
import org.hyperledger.besu.consensus.common.bft.BftHelpers;
import org.hyperledger.besu.consensus.common.bft.BftProtocolSchedule;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.blockcreation.AbstractBlockCreator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.SealableBlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;

import java.util.Collections;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;

/** The Bft block creator. */
// This class is responsible for creating a block without committer seals (basically it was just
// too hard to coordinate with the state machine).
public class BftBlockCreator extends AbstractBlockCreator {

  private final BftExtraDataCodec bftExtraDataCodec;

  /**
   * Instantiates a new Bft block creator.
   *
   * @param miningConfiguration the mining parameters
   * @param forksSchedule the forks schedule
   * @param localAddress the local address
   * @param extraDataCalculator the extra data calculator
   * @param transactionPool the pending transactions
   * @param protocolContext the protocol context
   * @param protocolSchedule the protocol schedule
   * @param bftExtraDataCodec the bft extra data codec
   * @param ethScheduler the scheduler for asynchronous block creation tasks
   */
  public BftBlockCreator(
      final MiningConfiguration miningConfiguration,
      final ForksSchedule<? extends BftConfigOptions> forksSchedule,
      final Address localAddress,
      final ExtraDataCalculator extraDataCalculator,
      final TransactionPool transactionPool,
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final BftExtraDataCodec bftExtraDataCodec,
      final EthScheduler ethScheduler) {
    super(
        miningConfiguration.setCoinbase(localAddress),
        miningBeneficiaryCalculator(protocolSchedule),
        extraDataCalculator,
        transactionPool,
        protocolContext,
        protocolSchedule,
        ethScheduler);
    this.bftExtraDataCodec = bftExtraDataCodec;
  }

  @Override
  public BlockCreationResult createBlock(final long timestamp, final BlockHeader parentHeader) {
    ProtocolSpec protocolSpec =
        ((BftProtocolSchedule) protocolSchedule)
            .getByBlockNumberOrTimestamp(parentHeader.getNumber() + 1, timestamp);

    if (protocolSpec.getWithdrawalsProcessor().isPresent()) {
      return createEmptyWithdrawalsBlock(timestamp, parentHeader);
    } else {
      return createBlock(
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.of(Bytes32.wrap(BftHelpers.EXPECTED_MIX_HASH.getBytes())),
          Optional.empty(),
          Optional.empty(),
          timestamp,
          true,
          parentHeader);
    }
  }

  /**
   * Construct a mining beneficiary calculator using the given protocolSchedule
   *
   * @param protocolSchedule protocol schedule
   * @return mining beneficiary calculator
   */
  public static MiningBeneficiaryCalculator miningBeneficiaryCalculator(
      final ProtocolSchedule protocolSchedule) {
    return (blockTimestamp, pendingHeader) -> {
      BlockHeader newBlockHeader =
          BlockHeaderBuilder.createDefault()
              .coinbase(pendingHeader.getCoinbase())
              .buildBlockHeader();
      ProtocolSpec protocolSpec =
          ((BftProtocolSchedule) protocolSchedule)
              .getByBlockNumberOrTimestamp(pendingHeader.getNumber(), blockTimestamp);
      return protocolSpec.getMiningBeneficiaryCalculator().calculateBeneficiary(newBlockHeader);
    };
  }

  @Override
  public BlockCreationResult createEmptyWithdrawalsBlock(
      final long timestamp, final BlockHeader parentHeader) {
    return createBlock(
        Optional.empty(),
        Optional.empty(),
        Optional.of(Collections.emptyList()),
        Optional.of(Bytes32.wrap(BftHelpers.EXPECTED_MIX_HASH.getBytes())),
        Optional.empty(),
        Optional.empty(),
        timestamp,
        true,
        parentHeader);
  }

  @Override
  protected BlockHeader createFinalBlockHeader(final SealableBlockHeader sealableBlockHeader) {
    final BlockHeaderBuilder builder =
        BlockHeaderBuilder.create()
            .populateFrom(sealableBlockHeader)
            .mixHash(BftHelpers.EXPECTED_MIX_HASH)
            .nonce(0L)
            .blockHeaderFunctions(BftBlockHeaderFunctions.forCommittedSeal(bftExtraDataCodec));

    return builder.buildBlockHeader();
  }
}
