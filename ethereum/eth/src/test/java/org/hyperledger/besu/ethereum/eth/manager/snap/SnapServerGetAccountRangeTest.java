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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.messages.snap.AccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetAccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.ImmutableSnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.math.BigInteger;
import java.util.NavigableMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SnapServerGetAccountRangeTest {
  private Hash rootHash;
  public Bytes32 firstAccount;
  public Bytes32 secondAccount;
  private SnapServer snapServer;
  private static ProtocolContext protocolContext;

  @BeforeAll
  public static void setup() {
    // Setup local blockchain for testing
    BlockchainSetupUtil localBlockchainSetup =
        BlockchainSetupUtil.forSnapTesting(DataStorageFormat.BONSAI);
    localBlockchainSetup.importAllBlocks(
        HeaderValidationMode.LIGHT_DETACHED_ONLY, HeaderValidationMode.LIGHT);

    protocolContext = localBlockchainSetup.getProtocolContext();
  }

  @BeforeEach
  public void setupTest() {
    WorldStateStorageCoordinator worldStateStorageCoordinator =
        new WorldStateStorageCoordinator(
            ((BonsaiWorldStateProvider) protocolContext.getWorldStateArchive())
                .getWorldStateKeyValueStorage());

    SnapSyncConfiguration snapSyncConfiguration =
        ImmutableSnapSyncConfiguration.builder().isSnapServerEnabled(true).build();
    snapServer =
        new SnapServer(
                snapSyncConfiguration,
                new EthMessages(),
                worldStateStorageCoordinator,
                protocolContext,
                mock(Synchronizer.class))
            .start();
    initAccounts();
  }

  /**
   * In this test, we request the entire state range, but limit the response to 4000 bytes.
   * Expected: 86 accounts.
   */
  @Test
  public void test0_RequestEntireStateRangeWith4000BytesLimit() {
    testAccountRangeRequest(
        new AccountRangeRequestParams.Builder()
            .rootHash(rootHash)
            .responseBytes(4000)
            .expectedAccounts(86)
            .expectedFirstAccount(firstAccount)
            .expectedLastAccount(
                Bytes32.fromHexString(
                    "0x445cb5c1278fdce2f9cbdb681bdd76c52f8e50e41dbd9e220242a69ba99ac099"))
            .build());
  }

  /**
   * In this test, we request the entire state range, but limit the response to 3000 bytes.
   * Expected: 65 accounts.
   */
  @Test
  public void test1_RequestEntireStateRangeWith3000BytesLimit() {
    testAccountRangeRequest(
        new AccountRangeRequestParams.Builder()
            .rootHash(rootHash)
            .responseBytes(3000)
            .expectedAccounts(65)
            .expectedFirstAccount(firstAccount)
            .expectedLastAccount(
                Bytes32.fromHexString(
                    "0x2e6fe1362b3e388184fd7bf08e99e74170b26361624ffd1c5f646da7067b58b6"))
            .build());
  }

  /**
   * In this test, we request the entire state range, but limit the response to 2000 bytes.
   * Expected: 44 accounts.
   */
  @Test
  public void test2_RequestEntireStateRangeWith2000BytesLimit() {
    testAccountRangeRequest(
        new AccountRangeRequestParams.Builder()
            .rootHash(rootHash)
            .responseBytes(2000)
            .expectedAccounts(44)
            .expectedFirstAccount(firstAccount)
            .expectedLastAccount(
                Bytes32.fromHexString(
                    "0x1c3f74249a4892081ba0634a819aec9ed25f34c7653f5719b9098487e65ab595"))
            .build());
  }

  /**
   * In this test, we request the entire state range, but limit the response to 1 byte. The server
   * should return the first account of the state. Expected: 1 account.
   */
  @Test
  public void test3_RequestEntireStateRangeWith1ByteLimit() {
    testAccountRangeRequest(
        new AccountRangeRequestParams.Builder()
            .rootHash(rootHash)
            .responseBytes(1)
            .expectedAccounts(1)
            .expectedFirstAccount(firstAccount)
            .expectedLastAccount(firstAccount)
            .build());
  }

  /**
   * Here we request with a responseBytes limit of zero. The server should return one account.
   * Expected: 1 account.
   */
  @Test
  public void test4_RequestEntireStateRangeWithZeroBytesLimit() {
    testAccountRangeRequest(
        new AccountRangeRequestParams.Builder()
            .rootHash(rootHash)
            .responseBytes(0)
            .expectedAccounts(1)
            .expectedFirstAccount(firstAccount)
            .expectedLastAccount(firstAccount)
            .build());
  }

  /**
   * In this test, we request a range where startingHash is before the first available account key,
   * and limitHash is after. The server should return the first and second account of the state
   * (because the second account is the 'next available'). Expected: 2 accounts.
   */
  @Test
  public void test5_RequestRangeBeforeFirstAccountKey() {
    testAccountRangeRequest(
        new AccountRangeRequestParams.Builder()
            .rootHash(rootHash)
            .startHash(hashAdd(firstAccount, -500))
            .limitHash(hashAdd(firstAccount, 1))
            .expectedAccounts(2)
            .expectedFirstAccount(firstAccount)
            .expectedLastAccount(secondAccount)
            .build());
  }

  /**
   * Here we request range where both bounds are before the first available account key. This should
   * return the first account (even though it's out of bounds). Expected: 1 account.
   */
  @Test
  public void test6_RequestRangeBothBoundsBeforeFirstAccountKey() {
    testAccountRangeRequest(
        new AccountRangeRequestParams.Builder()
            .rootHash(rootHash)
            .startHash(hashAdd(firstAccount, -500))
            .limitHash(hashAdd(firstAccount, -400))
            .expectedAccounts(1)
            .expectedFirstAccount(firstAccount)
            .expectedLastAccount(firstAccount)
            .build());
  }

  /**
   * In this test, both startingHash and limitHash are zero. The server should return the first
   * available account. Expected: 1 account.
   */
  @Test
  public void test7_RequestBothBoundsZero() {
    testAccountRangeRequest(
        new AccountRangeRequestParams.Builder()
            .rootHash(rootHash)
            .startHash(Bytes32.wrap(Hash.ZERO.getBytes()))
            .limitHash(Bytes32.wrap(Hash.ZERO.getBytes()))
            .expectedAccounts(1)
            .expectedFirstAccount(firstAccount)
            .expectedLastAccount(firstAccount)
            .build());
  }

  /**
   * In this test, startingHash is exactly the first available account key. The server should return
   * the first available account of the state as the first item. Expected: 86 accounts.
   */
  @Test
  public void test8_RequestStartingHashFirstAvailableAccountKey() {
    testAccountRangeRequest(
        new AccountRangeRequestParams.Builder()
            .rootHash(rootHash)
            .startHash(firstAccount)
            .responseBytes(4000)
            .expectedAccounts(86)
            .expectedFirstAccount(firstAccount)
            .expectedLastAccount(
                Bytes32.fromHexString(
                    "0x445cb5c1278fdce2f9cbdb681bdd76c52f8e50e41dbd9e220242a69ba99ac099"))
            .build());
  }

  /**
   * In this test, startingHash is after the first available key. The server should return the
   * second account of the state as the first item. Expected: 86 accounts.
   */
  @Test
  public void test9_RequestStartingHashAfterFirstAvailableKey() {
    testAccountRangeRequest(
        new AccountRangeRequestParams.Builder()
            .rootHash(rootHash)
            .startHash(secondAccount)
            .responseBytes(4000)
            .expectedAccounts(86)
            .expectedFirstAccount(secondAccount)
            .expectedLastAccount(
                Bytes32.fromHexString(
                    "0x4615e5f5df5b25349a00ad313c6cd0436b6c08ee5826e33a018661997f85ebaa"))
            .build());
  }

  /** This test requests a non-existent state root. Expected: 0 accounts. */
  @Test
  public void test10_RequestNonExistentStateRoot() {
    Hash rootHash =
        Hash.fromHexString("1337000000000000000000000000000000000000000000000000000000000000");
    testAccountRangeRequest(
        new AccountRangeRequestParams.Builder().rootHash(rootHash).expectedAccounts(0).build());
  }

  /**
   * This test requests data at a state root that is 127 blocks old. We expect the server to have
   * this state available. Expected: 84 accounts.
   */
  @Test
  public void test12_RequestStateRoot127BlocksOld() {

    Hash rootHash =
        protocolContext
            .getBlockchain()
            .getBlockHeader((protocolContext.getBlockchain().getChainHeadBlockNumber() - 127))
            .orElseThrow()
            .getStateRoot();
    testAccountRangeRequest(
        new AccountRangeRequestParams.Builder()
            .rootHash(rootHash)
            .expectedAccounts(84)
            .responseBytes(4000)
            .expectedFirstAccount(firstAccount)
            .expectedLastAccount(
                Bytes32.fromHexString(
                    "0x580aa878e2f92d113a12c0a3ce3c21972b03dbe80786858d49a72097e2c491a3"))
            .build());
  }

  /**
   * This test requests data at a state root that is actually the storage root of an existing
   * account. The server is supposed to ignore this request. Expected: 0 accounts.
   */
  @Test
  public void test13_RequestStateRootIsStorageRoot() {
    Hash rootHash =
        Hash.fromHexString("df97f94bc47471870606f626fb7a0b42eed2d45fcc84dc1200ce62f7831da990");
    testAccountRangeRequest(
        new AccountRangeRequestParams.Builder().rootHash(rootHash).expectedAccounts(0).build());
  }

  /**
   * In this test, the startingHash is after limitHash (wrong order). The server should ignore this
   * invalid request. Expected: 0 accounts.
   */
  @Test
  public void test14_RequestStartingHashAfterLimitHash() {
    testAccountRangeRequest(
        new AccountRangeRequestParams.Builder()
            .rootHash(rootHash)
            .startHash(Bytes32.wrap(Hash.LAST.getBytes()))
            .limitHash(Bytes32.wrap(Hash.ZERO.getBytes()))
            .expectedAccounts(0)
            .build());
  }

  /**
   * In this test, the startingHash is the first available key, and limitHash is a key before
   * startingHash (wrong order). The server should return the first available key. Expected: 1
   * account.
   */
  @Test
  public void test15_RequestStartingHashFirstAvailableKeyAndLimitHashBefore() {
    testAccountRangeRequest(
        new AccountRangeRequestParams.Builder()
            .rootHash(rootHash)
            .startHash(firstAccount)
            .limitHash(hashAdd(firstAccount, -1))
            .expectedAccounts(1)
            .expectedFirstAccount(firstAccount)
            .expectedLastAccount(firstAccount)
            .build());
  }

  /**
   * In this test, the startingHash is the first available key and limitHash is zero. (wrong order).
   * The server should return the first available key. Expected: 1 account.
   */
  @Test
  public void test16_RequestStartingHashFirstAvailableKeyAndLimitHashZero() {
    testAccountRangeRequest(
        new AccountRangeRequestParams.Builder()
            .rootHash(rootHash)
            .startHash(firstAccount)
            .limitHash(Bytes32.wrap(Hash.ZERO.getBytes()))
            .expectedAccounts(1)
            .expectedFirstAccount(firstAccount)
            .expectedLastAccount(firstAccount)
            .build());
  }

  private void testAccountRangeRequest(final AccountRangeRequestParams params) {
    NavigableMap<Bytes32, Bytes> accounts = getAccountRange(params);
    assertThat(accounts.size()).isEqualTo(params.getExpectedAccounts());

    if (params.getExpectedAccounts() > 0) {
      assertThat(accounts.firstKey()).isEqualTo(params.getExpectedFirstAccount());
      assertThat(accounts.lastKey()).isEqualTo(params.getExpectedLastAccount());
    }
  }

  private NavigableMap<Bytes32, Bytes> getAccountRange(final AccountRangeRequestParams params) {
    Hash rootHash = params.getRootHash();
    Bytes32 startHash = params.getStartHash();
    Bytes32 limitHash = params.getLimitHash();
    BigInteger sizeRequest = BigInteger.valueOf(params.getResponseBytes());

    GetAccountRangeMessage requestMessage =
        GetAccountRangeMessage.create(rootHash, startHash, limitHash, sizeRequest);

    AccountRangeMessage resultMessage =
        AccountRangeMessage.readFrom(
            snapServer.constructGetAccountRangeResponse(
                requestMessage.wrapMessageData(BigInteger.ONE)));
    NavigableMap<Bytes32, Bytes> accounts = resultMessage.accountData(false).accounts();
    return accounts;
  }

  @SuppressWarnings("UnusedVariable")
  private void initAccounts() {
    rootHash = protocolContext.getWorldStateArchive().getWorldState().rootHash();
    GetAccountRangeMessage requestMessage =
        GetAccountRangeMessage.create(
            rootHash,
            Bytes32.wrap(Hash.ZERO.getBytes()),
            Bytes32.wrap(Hash.LAST.getBytes()),
            BigInteger.valueOf(4000));
    AccountRangeMessage resultMessage =
        AccountRangeMessage.readFrom(
            snapServer.constructGetAccountRangeResponse(
                requestMessage.wrapMessageData(BigInteger.ONE)));
    NavigableMap<Bytes32, Bytes> accounts = resultMessage.accountData(false).accounts();
    firstAccount = accounts.firstEntry().getKey();
    secondAccount =
        accounts.entrySet().stream().skip(1).findFirst().orElse(accounts.firstEntry()).getKey();
  }

  private Bytes32 hashAdd(final Bytes32 hash, final int value) {
    var result = Hash.wrap(hash).getBytes().toBigInteger().add(BigInteger.valueOf(value));
    Bytes resultBytes = Bytes.wrap(result.toByteArray());
    return Bytes32.leftPad(resultBytes);
  }

  public static class AccountRangeRequestParams {
    private final Hash rootHash;
    private final Bytes32 startHash;
    private final Bytes32 limitHash;
    private final int responseBytes;
    private final int expectedAccounts;
    private final Bytes32 expectedFirstAccount;
    private final Bytes32 expectedLastAccount;

    private AccountRangeRequestParams(final Builder builder) {
      this.rootHash = builder.rootHash;
      this.startHash = builder.startHash;
      this.limitHash = builder.limitHash;
      this.responseBytes = builder.responseBytes;
      this.expectedAccounts = builder.expectedAccounts;
      this.expectedFirstAccount = builder.expectedFirstAccount;
      this.expectedLastAccount = builder.expectedLastAccount;
    }

    public static class Builder {
      private Hash rootHash = null;
      private Bytes32 startHash = Bytes32.ZERO;
      private Bytes32 limitHash = Bytes32.wrap(Hash.LAST.getBytes());
      private int responseBytes = Integer.MAX_VALUE;
      private int expectedAccounts = 0;
      private Bytes32 expectedFirstAccount = null;
      private Bytes32 expectedLastAccount = null;

      public Builder rootHash(final Hash rootHashHex) {
        this.rootHash = rootHashHex;
        return this;
      }

      public Builder startHash(final Bytes32 startHashHex) {
        this.startHash = startHashHex;
        return this;
      }

      public Builder limitHash(final Bytes32 limitHashHex) {
        this.limitHash = limitHashHex;
        return this;
      }

      public Builder responseBytes(final int responseBytes) {
        this.responseBytes = responseBytes;
        return this;
      }

      public Builder expectedAccounts(final int expectedAccounts) {
        this.expectedAccounts = expectedAccounts;
        return this;
      }

      public Builder expectedFirstAccount(final Bytes32 expectedFirstAccount) {
        this.expectedFirstAccount = expectedFirstAccount;
        return this;
      }

      public Builder expectedLastAccount(final Bytes32 expectedLastAccount) {
        this.expectedLastAccount = expectedLastAccount;
        return this;
      }

      public AccountRangeRequestParams build() {
        return new AccountRangeRequestParams(this);
      }
    }

    // Getters for each field
    public Hash getRootHash() {
      return rootHash;
    }

    public Bytes32 getStartHash() {
      return startHash;
    }

    public Bytes32 getLimitHash() {
      return limitHash;
    }

    public int getResponseBytes() {
      return responseBytes;
    }

    public int getExpectedAccounts() {
      return expectedAccounts;
    }

    public Bytes32 getExpectedFirstAccount() {
      return expectedFirstAccount;
    }

    public Bytes32 getExpectedLastAccount() {
      return expectedLastAccount;
    }
  }
}
