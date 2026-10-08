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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.flat;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.Trace;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.BlockRewardProcessor;
import org.hyperledger.besu.ethereum.mainnet.MiningBeneficiaryCalculator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class RewardTraceGenerator {

  private static final String REWARD_LABEL = "reward";
  private static final String BLOCK_LABEL = "block";
  private static final String UNCLE_LABEL = "uncle";

  /**
   * Generates a stream of reward {@link Trace} from the passed {@link Block} data.
   *
   * @param protocolSchedule the {@link ProtocolSchedule} to use
   * @param block the current {@link Block} to use
   * @return a stream of generated reward traces {@link Trace}
   */
  public static Stream<Trace> generateFromBlock(
      final ProtocolSchedule protocolSchedule, final Block block) {

    final List<Trace> flatTraces = new ArrayList<>();

    final BlockHeader blockHeader = block.getHeader();
    final List<BlockHeader> ommers = block.getBody().getOmmers();
    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(blockHeader);
    final BlockRewardProcessor blockRewardProcessor = protocolSpec.getBlockRewardProcessor();
    final MiningBeneficiaryCalculator miningBeneficiaryCalculator =
        protocolSpec.getMiningBeneficiaryCalculator();

    final Wei coinbaseReward = blockRewardProcessor.getCoinbaseReward(ommers.size());

    // add uncle reward traces
    ommers.forEach(
        ommerBlockHeader -> {
          final Wei ommerReward =
              blockRewardProcessor.getOmmerReward(
                  blockHeader.getNumber(), ommerBlockHeader.getNumber());
          final Action.Builder uncleActionBuilder =
              Action.builder()
                  .author(
                      miningBeneficiaryCalculator
                          .calculateBeneficiary(ommerBlockHeader)
                          .getBytes()
                          .toHexString())
                  .rewardType(UNCLE_LABEL)
                  .value(ommerReward.toShortHexString());
          flatTraces.add(
              RewardTrace.builder()
                  .actionBuilder(uncleActionBuilder)
                  .blockHash(block.getHash().getBytes().toHexString())
                  .blockNumber(blockHeader.getNumber())
                  .type(REWARD_LABEL)
                  .build());
        });

    // add block reward trace
    final Action.Builder blockActionBuilder =
        Action.builder()
            .author(
                miningBeneficiaryCalculator
                    .calculateBeneficiary(blockHeader)
                    .getBytes()
                    .toHexString())
            .rewardType(BLOCK_LABEL)
            .value(coinbaseReward.toShortHexString());
    flatTraces.add(
        0,
        RewardTrace.builder()
            .actionBuilder(blockActionBuilder)
            .blockHash(block.getHash().getBytes().toHexString())
            .blockNumber(blockHeader.getNumber())
            .type(REWARD_LABEL)
            .build());

    return flatTraces.stream();
  }
}
