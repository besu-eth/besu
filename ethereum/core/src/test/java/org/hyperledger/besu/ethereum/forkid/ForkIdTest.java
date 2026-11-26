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
package org.hyperledger.besu.ethereum.forkid;

import static com.google.common.primitives.Longs.asList;
import static java.util.Collections.emptyList;
import static java.util.Optional.empty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.forkid.ForkIdTestUtil.mockBlockchain;
import static org.hyperledger.besu.ethereum.forkid.ForkIdTestUtil.wantPeerCheck;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.forkid.ForkIdTestUtil.ForkIds;
import org.hyperledger.besu.ethereum.forkid.ForkIdTestUtil.GenesisHash;
import org.hyperledger.besu.ethereum.forkid.ForkIdTestUtil.Network;
import org.hyperledger.besu.ethereum.forkid.ForkIdTestUtil.PeerCheckCase;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForkIdTest {
  private static final Logger LOG = LoggerFactory.getLogger(ForkIdTest.class);

  public static Stream<Arguments> data() {
    return Stream.of(
        // Mainnet test cases
        Arguments.of(
            "Mainnet // Unsynced",
            Network.MAINNET,
            0L,
            0L,
            ForkIdTestUtil.wantForkId("0xfc64ec04", 1150000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // First Homestead block",
            Network.MAINNET,
            1150000L,
            0L,
            ForkIdTestUtil.wantForkId("0x97c2c34c", 1920000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // Last Homestead block",
            Network.MAINNET,
            1919999L,
            0L,
            ForkIdTestUtil.wantForkId("0x97c2c34c", 1920000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // First DAO block",
            Network.MAINNET,
            1920000L,
            0L,
            ForkIdTestUtil.wantForkId("0x91d1f948", 2463000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // Last DAO block",
            Network.MAINNET,
            2462999L,
            0L,
            ForkIdTestUtil.wantForkId("0x91d1f948", 2463000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // First Tangerine block",
            Network.MAINNET,
            2463000L,
            0L,
            ForkIdTestUtil.wantForkId("0x7a64da13", 2675000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // Last Tangerine block",
            Network.MAINNET,
            2674999L,
            0L,
            ForkIdTestUtil.wantForkId("0x7a64da13", 2675000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // First Spurious block",
            Network.MAINNET,
            2675000L,
            0L,
            ForkIdTestUtil.wantForkId("0x3edd5b10", 4370000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // Last Spurious block",
            Network.MAINNET,
            4369999L,
            0L,
            ForkIdTestUtil.wantForkId("0x3edd5b10", 4370000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // First Byzantium block",
            Network.MAINNET,
            4370000L,
            0L,
            ForkIdTestUtil.wantForkId("0xa00bc324", 7280000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // Last Byzantium block",
            Network.MAINNET,
            7279999L,
            0L,
            ForkIdTestUtil.wantForkId("0xa00bc324", 7280000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // First and last Constantinople, first Petersburg block",
            Network.MAINNET,
            7280000L,
            0L,
            ForkIdTestUtil.wantForkId("0x668db0af", 9069000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // Last Petersburg block",
            Network.MAINNET,
            9068999L,
            0L,
            ForkIdTestUtil.wantForkId("0x668db0af", 9069000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // First Istanbul block",
            Network.MAINNET,
            9069000L,
            0L,
            ForkIdTestUtil.wantForkId("0x879d6e30", 9200000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // Last Istanbul block",
            Network.MAINNET,
            9199999L,
            0L,
            ForkIdTestUtil.wantForkId("0x879d6e30", 9200000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // First Muir Glacier block",
            Network.MAINNET,
            9200000L,
            0L,
            ForkIdTestUtil.wantForkId("0xe029e991", 12244000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // Last Muir Glacier block",
            Network.MAINNET,
            12243999L,
            0L,
            ForkIdTestUtil.wantForkId("0xe029e991", 12244000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // First Berlin block",
            Network.MAINNET,
            12244000L,
            0L,
            ForkIdTestUtil.wantForkId("0x0eb440f6", 12965000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // Last Berlin block",
            Network.MAINNET,
            12964999L,
            0L,
            ForkIdTestUtil.wantForkId("0x0eb440f6", 12965000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // First London block",
            Network.MAINNET,
            12965000L,
            0L,
            ForkIdTestUtil.wantForkId("0xb715077d", 13773000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // Last London block",
            Network.MAINNET,
            13772999L,
            0L,
            ForkIdTestUtil.wantForkId("0xb715077d", 13773000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // First Arrow Glacier block",
            Network.MAINNET,
            13773000L,
            0L,
            ForkIdTestUtil.wantForkId("0x20c327fc", 15050000L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // First Gray Glacier block",
            Network.MAINNET,
            15050000L,
            0L,
            ForkIdTestUtil.wantForkId("0xf0afd0e3", 0L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        Arguments.of(
            "Mainnet // Future Gray Glacier block",
            Network.MAINNET,
            20000000L,
            0L,
            ForkIdTestUtil.wantForkId("0xf0afd0e3", 0L),
            Optional.of(ForkIds.MAINNET),
            empty()),
        // Fork ID test cases with block number and timestamp based forks
        // Withdrawals test cases
        Arguments.of(
            "Mainnet Withdrawals // First Merge Start block",
            Network.MAINNET_WITH_SHANGHAI,
            18000000L,
            0L,
            ForkIdTestUtil.wantForkId("0x4fb8a872", 1668000000L),
            Optional.of(ForkIdTestUtil.ForkIds.WITHDRAWALS),
            empty()),
        Arguments.of(
            "Mainnet Withdrawals // Last Merge Start block",
            Network.MAINNET_WITH_SHANGHAI,
            20000000L,
            0L,
            ForkIdTestUtil.wantForkId("0x4fb8a872", 1668000000L),
            Optional.of(ForkIdTestUtil.ForkIds.WITHDRAWALS),
            empty()),
        Arguments.of(
            "Mainnet Withdrawals // First Shanghai block",
            Network.MAINNET_WITH_SHANGHAI,
            20000000L,
            1668000000L,
            ForkIdTestUtil.wantForkId("0xc1fdf181", 0L),
            Optional.of(ForkIdTestUtil.ForkIds.WITHDRAWALS),
            empty()),
        Arguments.of(
            "Mainnet Withdrawals // Last Shanghai block",
            Network.MAINNET_WITH_SHANGHAI,
            20100000L,
            2669000000L,
            ForkIdTestUtil.wantForkId("0xc1fdf181", 0L),
            Optional.of(ForkIdTestUtil.ForkIds.WITHDRAWALS),
            empty()),
        // sepolia
        Arguments.of(
            "Sepolia // mergenetsplit block",
            Network.SEPOLIA,
            0L,
            0L,
            ForkIdTestUtil.wantForkId("0xfe3366e7", 1735371L),
            Optional.of(ForkIds.SEPOLIA),
            empty()),
        Arguments.of(
            "Sepolia // Shanghai",
            Network.SEPOLIA,
            1735371L,
            0L,
            ForkIdTestUtil.wantForkId("0xb96cbd13", 1677557088L),
            Optional.of(ForkIds.SEPOLIA),
            empty()),
        Arguments.of(
            "Sepolia // Future",
            Network.SEPOLIA,
            1735372L,
            1677557088L,
            ForkIdTestUtil.wantForkId("0xf7f9bc08", 0L),
            Optional.of(ForkIds.SEPOLIA),
            empty()),
        // Hoodi
        Arguments.of(
            "Hoodi // Prague",
            Network.HOODI,
            0L,
            1742999832L,
            ForkIdTestUtil.wantForkId("0x0929e24e", 0L),
            Optional.of(ForkIds.HOODI),
            empty()),
        // private
        Arguments.of(
            "Private // Unsynced",
            Network.PRIVATE,
            0L,
            0L,
            ForkIdTestUtil.wantForkId("0x190a55ad", 0L),
            empty(),
            empty()),
        Arguments.of(
            "Private // First block",
            Network.PRIVATE,
            1L,
            0L,
            ForkIdTestUtil.wantForkId("0x190a55ad", 0L),
            empty(),
            empty()),
        Arguments.of(
            "Private // Future block",
            Network.PRIVATE,
            1000000L,
            0L,
            ForkIdTestUtil.wantForkId("0x190a55ad", 0L),
            empty(),
            empty()),
        // peer check
        Arguments.of(
            "check1PetersburgWithRemoteAnnouncingTheSame",
            Network.MAINNET,
            7987396L,
            0L,
            empty(),
            empty(),
            wantPeerCheck("0x668db0af", 0L, true)),
        Arguments.of(
            "check2PetersburgWithRemoteAnnouncingTheSameAndNextFork",
            Network.MAINNET,
            7987396L,
            0L,
            empty(),
            empty(),
            wantPeerCheck("0x668db0af", Long.MAX_VALUE, true)),
        Arguments.of(
            "check3ByzantiumAwareOfPetersburgRemoteUnawareOfPetersburg",
            Network.MAINNET,
            7279999L,
            0L,
            empty(),
            empty(),
            wantPeerCheck("0xa00bc324", 0L, true)),
        Arguments.of(
            "check4ByzantiumAwareOfPetersburgRemoteAwareOfPetersburg",
            Network.MAINNET,
            7987396L,
            0L,
            empty(),
            empty(),
            wantPeerCheck("0xa00bc324", 7280000L, true)),
        Arguments.of(
            "check5ByzantiumAwareOfPetersburgRemoteAnnouncingUnknownFork",
            Network.MAINNET,
            7279999L,
            0L,
            empty(),
            empty(),
            wantPeerCheck("0xa00bc324", Long.MAX_VALUE, true)),
        Arguments.of(
            "check6PetersburgWithRemoteAnnouncingByzantiumAwareOfPetersburg",
            Network.MAINNET,
            7987396L,
            0L,
            empty(),
            empty(),
            wantPeerCheck("0x668db0af", 7280000L, true)),
        Arguments.of(
            "check7PetersburgWithRemoteAnnouncingSpuriousAwareOfByzantiumRemoteMayNeedUpdate",
            Network.MAINNET,
            7987396L,
            0L,
            empty(),
            empty(),
            wantPeerCheck("0x3edd5b10", 4370000L, true)),
        Arguments.of(
            "check8ByzantiumWithRemoteAnnouncingPetersburgLocalOutOfSync",
            Network.MAINNET,
            727999L,
            0L,
            empty(),
            empty(),
            wantPeerCheck("0x668db0af", 0L, true)),
        Arguments.of(
            "check9SpuriousWithRemoteAnnouncingByzantiumRemoteUnawareOfPetersburg",
            Network.MAINNET,
            4369999L,
            0L,
            empty(),
            empty(),
            wantPeerCheck("0xa00bc324", 0L, true)),
        Arguments.of(
            "check10PetersburgWithRemoteAnnouncingByzantiumRemoteUnawareOfAdditionalForks",
            Network.network(
                GenesisHash.MAINNET,
                asList(1150000L, 1920000L, 2463000L, 2675000L, 4370000L, 7280000L),
                emptyList()),
            7987396L,
            0L,
            empty(),
            empty(),
            wantPeerCheck("0xa00bc324", 0L, false)),
        Arguments.of(
            "check11PetersburgWithRemoteAnnouncingPetersburgAndFutureForkLocalNeedsUpdate",
            Network.network(
                GenesisHash.MAINNET,
                asList(1150000L, 1920000L, 2463000L, 2675000L, 4370000L, 7280000L),
                emptyList()),
            7987396L,
            0L,
            empty(),
            empty(),
            wantPeerCheck("0x5cddc0e1", 0L, false)),
        Arguments.of(
            "check12ByzantiumWithRemoteAnnouncingPetersburgAndFutureForkLocalNeedsUpdate",
            ForkIdTestUtil.Network.network(
                GenesisHash.MAINNET,
                asList(1150000L, 1920000L, 2463000L, 2675000L, 4370000L, 7280000L),
                emptyList()),
            7279999L,
            0L,
            empty(),
            empty(),
            wantPeerCheck("0x5cddc0e1", 0L, false)),
        // Timestamp based peer check cases adapted from EIP-6122 test cases
        Arguments.of(
            "withdrawalsCheck1ShanghaiWithRemoteAnnouncingTheSame",
            Network.MAINNET_WITH_SHANGHAI,
            20000000L,
            1668000001L,
            empty(),
            empty(),
            wantPeerCheck("0xc1fdf181", 0L, true)),
        Arguments.of(
            "withdrawalsCheck2ShanghaiWithRemoteAnnouncingSameAndNextFork",
            Network.MAINNET_WITH_SHANGHAI,
            20000000L,
            1668000001L,
            empty(),
            empty(),
            wantPeerCheck("0xc1fdf181", Long.MAX_VALUE, true)),
        Arguments.of(
            "withdrawalsCheck3ByzantiumWithRemoteAnnouncingByzantiumNotAwareOfPetersburg",
            Network.MAINNET_WITH_SHANGHAI,
            7279999L,
            1667999999L,
            empty(),
            empty(),
            wantPeerCheck("0xa00bc324", 0L, true)),
        Arguments.of(
            "withdrawalsCheck4ByzantiumWithRemoteAnnouncingByzantiumAwareOfPetersburg",
            Network.MAINNET_WITH_SHANGHAI,
            7279999L,
            1667999999L,
            empty(),
            empty(),
            wantPeerCheck("0xa00bc324", 7280000L, true)),
        Arguments.of(
            "withdrawalsCheck5ByzantiumWithRemoteAnnouncingByzantiumAwareOfUnknownFork",
            Network.MAINNET_WITH_SHANGHAI,
            7279999L,
            1667999999L,
            empty(),
            empty(),
            wantPeerCheck("0xa00bc324", Long.MAX_VALUE, true)),
        Arguments.of(
            "withdrawalsCheck6ExactlyShanghaiWithRemoteAnnouncingByzantiumAwareOfPetersburgRemoteOutOfSync",
            Network.MAINNET_WITH_SHANGHAI,
            20000000L,
            1668000000L,
            empty(),
            empty(),
            wantPeerCheck("0xa00bc324", 7280000L, true)),
        Arguments.of(
            "withdrawalsCheck7ShanghaiWithRemoteAnnouncingByzantiumAwareOfPetersburgRemoteOutOfSync",
            Network.MAINNET_WITH_SHANGHAI,
            20000000L,
            1668000001L,
            empty(),
            empty(),
            wantPeerCheck("0xa00bc324", 7280000L, true)),
        Arguments.of(
            "withdrawalsCheck8ShanghaiWithRemoteAnnouncingSpuriousAwareOfByzantium",
            Network.MAINNET_WITH_SHANGHAI,
            20000000L,
            1668000001L,
            empty(),
            empty(),
            wantPeerCheck("0x3edd5b10", 4370000, true)),
        Arguments.of(
            "withdrawalsCheck9ByzantiumWithRemoteAnnouncingPetersburgLocalOutOfSync",
            Network.MAINNET_WITH_SHANGHAI,
            7279999L,
            1667999999L,
            empty(),
            empty(),
            wantPeerCheck("0x668db0af", 4370000, true)),
        Arguments.of(
            "withdrawalsCheck10SpuriousWithRemoteAnnouncingByzantiumNotAwareOfPetersburgLocalOutOfSync",
            Network.MAINNET_WITH_SHANGHAI,
            4369999L,
            1667999999L,
            empty(),
            empty(),
            wantPeerCheck("0xa00bc324", 0L, true)),
        Arguments.of(
            "withdrawalsCheck11ShanghaiWithRemoteAnnouncingByzantiumUnawareOfAdditionalForksRemoteNeedsUpdate",
            Network.MAINNET_WITH_SHANGHAI,
            20000000L,
            1668000001L,
            empty(),
            empty(),
            wantPeerCheck("0xa00bc324", 0L, false)),
        Arguments.of(
            "withdrawalsCheck12ShanghaiAndNotAwareOfAdditionalForksWithRemoteAnnouncingPetersburgAndUnknownFork",
            Network.MAINNET_WITH_SHANGHAI,
            20000000L,
            1668000001L,
            empty(),
            empty(),
            wantPeerCheck("0x5cddc0e1", 0L, false)),
        Arguments.of(
            "withdrawalsCheck14ShanghaiWithRemoteAnnouncingUnknownFork",
            Network.MAINNET_WITH_SHANGHAI,
            88888888L,
            1668000001L,
            empty(),
            empty(),
            wantPeerCheck("0xf0afd0e3", 88888888L, false)),
        Arguments.of(
            "withdrawalsCheck15ShanghaiWithRemoteInByzantiumAnnouncingUnknownFork",
            Network.MAINNET_WITH_SHANGHAI,
            88888888L,
            1668000001L,
            empty(),
            empty(),
            wantPeerCheck("0xa00bc324", 7279999L, false)));
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("data")
  public void test(
      final String name,
      final ForkIdTestUtil.Network network,
      final long head,
      final long time,
      final Optional<ForkId> wantForkId,
      final Optional<List<ForkId>> wantForkIds,
      final Optional<PeerCheckCase> wantPeerCheckCase) {
    LOG.info("Running test case {}", name);
    final ForkIdManager forkIdManager =
        new ForkIdManager(
            mockBlockchain(network.hash, head, time), network.blockForks, network.timestampForks);
    wantForkId.ifPresent(
        forkId -> assertThat(forkIdManager.getForkIdForChainHead()).isEqualTo(forkId));
    wantForkIds.ifPresent(
        forkIds -> assertThat(forkIdManager.getAllForkIds()).containsExactlyElementsOf(forkIds));
    wantPeerCheckCase.ifPresent(
        peerCheckCase ->
            assertThat(
                    forkIdManager.peerCheck(
                        new ForkId(
                            Bytes.fromHexString(peerCheckCase.forkIdHash),
                            peerCheckCase.forkIdNext)))
                .isEqualTo(peerCheckCase.want));
  }

  @Test
  public void testGenesisTimestampEqualToShanghaiTimestamp() {
    final ForkIdManager forkIdManager =
        new ForkIdManager(
            mockBlockchain(Hash.ZERO.getBytes().toHexString(), 10L, 0L),
            Collections.emptyList(),
            List.of(1L, 2L));
    assertThat(forkIdManager.getAllForkIds().size()).isEqualTo(2);
    assertThat(forkIdManager.getAllForkIds().get(0).getNext()).isEqualTo(2L);
    assertThat(forkIdManager.getAllForkIds().get(1).getNext()).isEqualTo(0L);
  }

  @Test
  public void testNoBlockNoForksAndNoTimestampForksGreaterThanGenesisTimestamp() {
    final ForkIdManager forkIdManager =
        new ForkIdManager(
            mockBlockchain(Hash.ZERO.getBytes().toHexString(), 10L, 1L),
            Collections.emptyList(),
            List.of(1L));
    assertThat(forkIdManager.getAllForkIds().size()).isEqualTo(0);
    // There is no ForkId, so peerCheck always has to return true
    assertThat(forkIdManager.peerCheck(new ForkId(Bytes.fromHexString("0xdeadbeef"), 100L)))
        .isEqualTo(true);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "0x010203040506070809",
        "0x0102030405060708090a0b0c0d0e0f10",
        "0x00",
        "0x00118c30",
        "0x000000000000000000118c30"
      })
  public void fromRawForkIdRejectsForkNextThatIsNotACanonicalUint64(final String next) {
    final List<List<Bytes>> rawForkId =
        List.of(List.of(Bytes.fromHexString("0xfc64ec04"), Bytes.fromHexString(next)));
    assertThat(ForkId.fromRawForkId(rawForkId)).isEmpty();
  }

  @ParameterizedTest
  @CsvSource({"0x, 0", "0x118c30, 1150000", "0xffffffffffffffff, -1"})
  public void fromRawForkIdAcceptsForkNextThatFitsUint64(final String next, final long expected) {
    final List<List<Bytes>> rawForkId =
        List.of(List.of(Bytes.fromHexString("0xfc64ec04"), Bytes.fromHexString(next)));
    final Optional<ForkId> forkId = ForkId.fromRawForkId(rawForkId);
    assertThat(forkId).isPresent();
    assertThat(forkId.get().getNext()).isEqualTo(expected);
  }
}
