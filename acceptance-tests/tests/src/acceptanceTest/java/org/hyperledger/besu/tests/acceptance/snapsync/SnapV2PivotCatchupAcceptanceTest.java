/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.tests.acceptance.snapsync;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.account.Accounts;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.snapsync.AmsterdamEngineApi.BuiltBlock;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.web3j.crypto.Credentials;
import org.web3j.crypto.RawTransaction;

/**
 * Snap/2 regular pivot catch-up, end to end: a node snap-syncs toward a pivot, and while its
 * world-state download is still running the chain head advances far enough to refresh the pivot, so
 * the catch-up applies the intervening blocks' BALs to the partially downloaded state — no reorg,
 * no final healing phase. The node must finish on the new head with every scenario category
 * corrected, and the synced state must be able to execute new blocks.
 *
 * <p>The catch-up transactions (blocks 15-16) are arranged to hit every regular catch-up category;
 * see {@link #assertPostCatchupWorldState()} for the per-category list and assertions.
 *
 * <p>Final-state assertions are path-independent: an account skipped by the BAL applier because its
 * range was not yet downloaded is downloaded fresh at the new pivot with the same canonical value.
 * The console assertions pin that the BAL-application path actually ran and that the final trie was
 * reconstructed; see {@link #assertCatchupConsole()}.
 *
 * <p>Flaky-test note: the catch-up only exercises the BAL applier if the pivot switches while the
 * world-state download is still running. The pivot switch is triggered as soon as the download
 * starts; the chain-side catch-up (header/BAL download for the new pivot) then delays BAL
 * application by a second or two, by which time hundreds of the 300 catch-up-touched contracts are
 * statistically guaranteed to be persisted. If this starts timing out on a very fast CI agent (the
 * download completing before the catch-up lands), retune {@link #HEAVY_BLOCKS}/{@link
 * #CONTRACTS_PER_HEAVY_BLOCK}.
 */
public class SnapV2PivotCatchupAcceptanceTest extends AbstractSnapV2AcceptanceTest {

  // Fork geometry: PivotSelectorAtHead anchors the pivot at head - 1, so phase 1 (head 14) syncs
  // toward pivot 13 and phase 2 (head 25) refreshes it to 24: head 25 is 12 ahead of pivot 13 (>=
  // PIVOT_BLOCK_WINDOW_VALIDITY), triggering exactly one catch-up over BALs 14..24.
  private static final int PHASE1_HEAD = 14;
  private static final int PHASE1_PIVOT = PHASE1_HEAD - 1;
  private static final int PHASE2_HEAD = 25;
  private static final int PHASE2_PIVOT = PHASE2_HEAD - 1;
  private static final String PIVOT_RANGE = PHASE1_PIVOT + " -> " + PHASE2_PIVOT;

  // Blocks 2-7 deploy 1000 bulk contracts each, so the throttled world-state download runs for
  // several seconds: the window in which the pivot is switched mid-download.
  private static final int HEAVY_BLOCK_1 = 2;
  private static final int HEAVY_BLOCKS = 6;
  private static final int CONTRACTS_PER_HEAVY_BLOCK = 1000;
  private static final int FUNDING_BLOCK = 8;
  private static final int CATCHUP_BLOCK_1 = 15;
  private static final int CATCHUP_BLOCK_2 = 16;

  // Scenario layout. Bulk contracts write slot j = j + 1 in their constructors.
  private static final int UPDATED_CONTRACT_COUNT = 300; // bulk contracts 0-299: slot 0 -> 7
  private static final int ZEROED_CONTRACT_1 = 300; // slot 1 -> 0
  private static final int ZEROED_CONTRACT_2 = 301; // slot 1 -> 0
  private static final int MULTI_CONTRACT = 302; // slot 0: 1 -> 5 (block 15) -> 6 (block 16)
  private static final int UNTOUCHED_CONTRACT = 303;
  private static final int SPLIT_SLOTS = 24; // large-storage contract
  private static final int SPLIT_UPDATED_SLOTS = 6;
  private static final int NEW_DEPLOY_COUNT = 5;

  private static final int UPDATED_VALUE = 7;
  private static final int MULTI_INTERMEDIATE_VALUE = 5;
  private static final int MULTI_FINAL_VALUE = 6;
  private static final int SPLIT_UPDATED_VALUE = 9;
  private static final int MULTI_DEPLOY_FINAL_VALUE = 10;

  private static final long UPDATE_GAS_LIMIT = 100_000L;

  // Runtime stored by every scenario contract: value = calldata word 0, slot = calldata word 1.
  private static final String SETTER_RUNTIME = "0x6000356020355500";

  private static final Credentials ACCOUNT_TWO =
      Credentials.create(Accounts.GENESIS_ACCOUNT_TWO_PRIVATE_KEY);
  private static final Credentials ACCOUNT_THREE =
      Credentials.create(Accounts.GENESIS_ACCOUNT_THREE_PRIVATE_KEY);
  private static final String ACCOUNT_TWO_ADDRESS = ACCOUNT_TWO.getAddress();
  private static final String ACCOUNT_THREE_ADDRESS = ACCOUNT_THREE.getAddress();

  private static final String FEE_RECIPIENT = "0x1111111111111111111111111111111111111111";

  // Fresh addresses that no genesis alloc or deployed contract can collide with.
  private static final String FRESH_RECIPIENT_1 = "0x1000000000000000000000000000000000000011";
  private static final String FRESH_RECIPIENT_2 = "0x1000000000000000000000000000000000000012";
  private static final String POST_SYNC_RECIPIENT = "0x1000000000000000000000000000000000000013";

  private static final BigInteger FUND_TWO_WEI = TRANSFER_WEI.multiply(BigInteger.valueOf(100));
  private static final BigInteger FUND_THREE_WEI = TRANSFER_WEI.multiply(BigInteger.valueOf(50));
  private static final BigInteger ACCOUNT_TWO_TRANSFER_WEI = TRANSFER_WEI.multiply(BigInteger.TWO);

  private BesuNode miner;

  // Sender nonces.
  private long benefactorNonce = 0;
  private long accountTwoNonce = 0;

  // Scenario contracts recorded when their deploy is submitted, so the catch-up blocks and the
  // assertions refer to them by role instead of deriving benefactor nonces.
  private Account splitContract;
  private final List<Account> newDeploys = new ArrayList<>();
  private Account multiDeploy;

  @Test
  public void appliesBalsAtPivotCatchupDuringSnapV2Sync() throws Exception {
    startNodes();
    final List<BuiltBlock> chain = buildChain();
    final String phase1HeadHash = chainBlockHash(chain, PHASE1_HEAD);
    final String phase2HeadHash = chainBlockHash(chain, PHASE2_HEAD);

    // Pre-cache the phase-2 payloads on the sync node so its pivot selector can resolve the new
    // pivot from cache later. Inert until the forkchoice points at block 25.
    for (int height = PHASE1_HEAD + 1; height <= PHASE2_HEAD; height++) {
      engineApi.cachePayload(syncNode, chainBlock(chain, height));
    }

    // Capture from here on: the log lines we key off are emitted by the sync node.
    noDiscoveryCluster.startConsoleCapture();

    // Phase 1: snap-sync toward block 14, connected only to the miner.
    syncNode.execute(adminTransactions.addPeer(miner.enodeUrl()));
    syncNode.verify(net.awaitPeerCount(1));
    engineApi.cachePayload(syncNode, chainBlock(chain, PHASE1_HEAD));
    awaitLog(
        "Header import progress 100.00%",
        Duration.ofMinutes(3), Duration.ofMillis(50), phase1HeadHash);

    // Phase 2: as soon as the world-state download is running at pivot 13, advance the head to
    // block 25. The pivot refreshes to 24 mid-download and the catch-up applies the BALs of
    // blocks 14..24 to the partially downloaded world state.
    awaitLog(
        "Downloading snap/2 world state from peers for static pivot block " + PHASE1_PIVOT,
        Duration.ofMinutes(3),
        Duration.ofMillis(100),
        phase1HeadHash);
    awaitLog(
        "snap/2 pivot catch-up complete: " + PIVOT_RANGE,
        Duration.ofMinutes(4),
        Duration.ofMillis(250),
        phase2HeadHash);

    // The remaining ranges download at pivot 24.
    awaitLog(
        "snap/2 world state root verified at pivot block " + PHASE2_PIVOT,
        Duration.ofMinutes(6),
        Duration.ofSeconds(1),
        phase2HeadHash);
    awaitHead(phase2HeadHash, PHASE2_HEAD);

    assertCatchupConsole();
    assertPostCatchupWorldState();
    assertSyncedStateExecutesNewBlocks();
  }

  /** Console assertions pinning the regular (non-reorg) catch-up path. */
  private void assertCatchupConsole() throws IOException {
    final String console = noDiscoveryCluster.peekConsoleContents();
    assertThat(console)
        .contains(
            "snap/2 pivot catch-up initiated: " + PIVOT_RANGE,
            "Preparing snap/2 pivot catch-up from block " + PHASE1_PIVOT + " to " + PHASE2_PIVOT,
            "snap/2 chain catch-up completed for pivot block " + PHASE2_PIVOT,
            "snap/2 applying BALs: pivot " + PIVOT_RANGE,
            // the BAL applier had persisted accounts to work on (not "No persisted accounts")
            "Applied snap/2 BALs:",
            "snap/2 pivot catch-up complete: " + PIVOT_RANGE,
            "snap/2 world state root verified at pivot block " + PHASE2_PIVOT)
        .doesNotContain(
            "snap/2 chain reorg detected at pivot catch-up",
            "snap/2 pivot catch-up failed",
            "snap/2 state root verification failed",
            "No persisted accounts affected by BALs");
    // the catch-up BALs were fetched from the peer and persisted locally
    assertThat(engineApi.hasBlockAccessList(syncNode, "0xf"))
        .as("catch-up BAL for block 15 persisted")
        .isTrue();
  }

  /** State assertions on the synced world state, one per regular catch-up category. */
  private void assertPostCatchupWorldState() {
    // Existing contracts updated between pivots: the new slot value wins; siblings are untouched.
    assertStorage(contractAt(0), 0, UPDATED_VALUE);
    assertStorage(contractAt(0), 1, 2);
    assertStorage(contractAt(UPDATED_CONTRACT_COUNT / 2), 0, UPDATED_VALUE);
    assertStorage(contractAt(UPDATED_CONTRACT_COUNT - 1), 0, UPDATED_VALUE);

    // Slots zeroed between pivots are deleted; siblings untouched.
    assertStorage(contractAt(ZEROED_CONTRACT_1), 1, 0);
    assertStorage(contractAt(ZEROED_CONTRACT_1), 0, 1);
    assertStorage(contractAt(ZEROED_CONTRACT_2), 1, 0);
    assertStorage(contractAt(ZEROED_CONTRACT_2), 0, 1);

    // Updated in both catch-up blocks with different values: the latest one wins.
    assertStorage(contractAt(MULTI_CONTRACT), 0, MULTI_FINAL_VALUE);

    // Untouched contracts keep their original code and storage.
    assertCode(contractAt(UNTOUCHED_CONTRACT));
    assertStorage(contractAt(UNTOUCHED_CONTRACT), 0, 1);
    assertStorage(contractAt(UNTOUCHED_CONTRACT), 1, 2);
    final Account lastBulk = contractAt(HEAVY_BLOCKS * CONTRACTS_PER_HEAVY_BLOCK - 1);
    assertStorage(lastBulk, 0, 1);
    assertStorage(lastBulk, 1, 2);

    // Large-storage contract partially updated: exercises selective per-slot application plus
    // storage root patching. Updated slots read the new value, the rest keep slot j = j + 1.
    for (int slot = 0; slot < SPLIT_UPDATED_SLOTS; slot++) {
      assertStorage(splitContract, slot, SPLIT_UPDATED_VALUE);
    }
    assertStorage(splitContract, SPLIT_UPDATED_SLOTS, SPLIT_UPDATED_SLOTS + 1);
    assertStorage(splitContract, SPLIT_SLOTS / 2, SPLIT_SLOTS / 2 + 1);
    assertStorage(splitContract, SPLIT_SLOTS - 1, SPLIT_SLOTS);

    // Contracts deployed between pivots: code and storage created from the BAL.
    for (final Account newDeploy : newDeploys) {
      assertCode(newDeploy);
      assertStorage(newDeploy, 0, 1);
      assertStorage(newDeploy, 1, 2);
    }

    // Deployed in the first catch-up block, updated in the second: the latest value wins.
    assertStorage(multiDeploy, 0, MULTI_DEPLOY_FINAL_VALUE);
    assertStorage(multiDeploy, 1, 2);

    // Fresh EOAs created by transfers between pivots.
    assertBalance(FRESH_RECIPIENT_1, TRANSFER_WEI);
    assertBalance(FRESH_RECIPIENT_2, ACCOUNT_TWO_TRANSFER_WEI);

    // Existing EOA funded before the pivot and topped up between pivots: 50 + 1 ETH.
    assertBalance(ACCOUNT_THREE_ADDRESS, FUND_THREE_WEI.add(TRANSFER_WEI));

    // The second funded EOA sent exactly one transaction between pivots: nonce 1.
    assertThat(syncNode.execute(ethTransactions.getTransactionCount(ACCOUNT_TWO_ADDRESS)))
        .isEqualTo(BigInteger.ONE);

    // The highest-churn accounts (the benefactor sends every transaction, the fee recipient is
    // paid by every non-empty block) must match the miner exactly.
    assertSameBalanceAsMiner(BENEFACTOR_ADDRESS.toHexString());
    assertSameBalanceAsMiner(FEE_RECIPIENT);
  }

  /**
   * A fresh block built on the new head must validate on the sync node: the synced world state can
   * execute new blocks. Its transfer creates one more fresh recipient.
   */
  private void assertSyncedStateExecutesNewBlocks() throws IOException {
    submitTransfer(benefactorNonce++, POST_SYNC_RECIPIENT, TRANSFER_WEI);
    final BuiltBlock postSyncBlock = engineApi.buildBlock(miner, FEE_RECIPIENT, 1, PHASE2_HEAD + 1);
    engineApi.assertValidPayload(syncNode, postSyncBlock);
    engineApi.setHead(syncNode, postSyncBlock.blockHash());
    syncNode.verify(blockchain.currentHeight(PHASE2_HEAD + 1));
    assertBalance(POST_SYNC_RECIPIENT, TRANSFER_WEI);
  }

  private void startNodes() throws IOException {
    final String genesis = loadAmsterdamGenesis();
    miner = createMiner("miner", genesis);
    syncNode = createSyncNode("syncNode", genesis);
    startCluster(miner, syncNode);
  }

  /** Builds the whole 25-block chain on the miner before the sync node connects. */
  private List<BuiltBlock> buildChain() throws IOException {
    final List<BuiltBlock> blocks = new ArrayList<>();
    for (int height = 1; height <= PHASE2_HEAD; height++) {
      blocks.add(engineApi.buildBlock(miner, FEE_RECIPIENT, submitScenarioTxs(height), height));
    }
    miner.verify(blockchain.currentHeight(PHASE2_HEAD));
    return blocks;
  }

  /** Submits the scenario transactions for one block; returns how many were submitted. */
  private int submitScenarioTxs(final int height) {
    final long nonceBefore = benefactorNonce + accountTwoNonce;
    if (isHeavyBlock(height)) {
      submitHeavyBlockTxs();
    } else if (height == FUNDING_BLOCK) {
      submitFundingBlockTxs();
    } else if (height == CATCHUP_BLOCK_1) {
      submitCatchupBlock1Txs();
    } else if (height == CATCHUP_BLOCK_2) {
      submitCatchupBlock2Txs();
    }
    // the submitted count is exactly the nonces consumed
    return (int) (benefactorNonce + accountTwoNonce - nonceBefore);
  }

  private static boolean isHeavyBlock(final int height) {
    return height >= HEAVY_BLOCK_1 && height < HEAVY_BLOCK_1 + HEAVY_BLOCKS;
  }

  /** 1000 bulk deploys per heavy block. */
  private void submitHeavyBlockTxs() {
    for (int i = 0; i < CONTRACTS_PER_HEAVY_BLOCK; i++) {
      submitDeploy(benefactorNonce++, 2, DEPLOY_GAS_LIMIT);
    }
  }

  /** The large-storage contract, and the two EOAs funded ahead of the catch-up blocks. */
  private void submitFundingBlockTxs() {
    splitContract = submitDeploy(benefactorNonce++, SPLIT_SLOTS, SPLIT_DEPLOY_GAS_LIMIT);
    submitTransfer(benefactorNonce++, ACCOUNT_TWO_ADDRESS, FUND_TWO_WEI);
    submitTransfer(benefactorNonce++, ACCOUNT_THREE_ADDRESS, FUND_THREE_WEI);
  }

  /** The first catch-up block: one transaction per regular catch-up category. */
  private void submitCatchupBlock1Txs() {
    for (int i = 0; i < UPDATED_CONTRACT_COUNT; i++) {
      submitStorageUpdate(benefactorNonce++, contractAt(i), 0, UPDATED_VALUE);
    }
    submitStorageUpdate(benefactorNonce++, contractAt(ZEROED_CONTRACT_1), 1, 0);
    submitStorageUpdate(benefactorNonce++, contractAt(ZEROED_CONTRACT_2), 1, 0);
    submitStorageUpdate(benefactorNonce++, contractAt(MULTI_CONTRACT), 0, MULTI_INTERMEDIATE_VALUE);
    for (int slot = 0; slot < SPLIT_UPDATED_SLOTS; slot++) {
      submitStorageUpdate(benefactorNonce++, splitContract, slot, SPLIT_UPDATED_VALUE);
    }
    for (int i = 0; i < NEW_DEPLOY_COUNT; i++) {
      newDeploys.add(submitDeploy(benefactorNonce++, 2, DEPLOY_GAS_LIMIT));
    }
    multiDeploy = submitDeploy(benefactorNonce++, 2, DEPLOY_GAS_LIMIT);
    submitTransfer(benefactorNonce++, FRESH_RECIPIENT_1, TRANSFER_WEI);
    submitTransfer(benefactorNonce++, ACCOUNT_THREE_ADDRESS, TRANSFER_WEI);
    // the second funded EOA spends from its own funds
    sendRaw(
        miner,
        RawTransaction.createEtherTransaction(
            BigInteger.valueOf(accountTwoNonce++),
            GAS_PRICE,
            BigInteger.valueOf(TRANSFER_GAS_LIMIT),
            FRESH_RECIPIENT_2,
            ACCOUNT_TWO_TRANSFER_WEI),
        ACCOUNT_TWO);
  }

  /** The second catch-up block: the same accounts updated again, so the latest value must win. */
  private void submitCatchupBlock2Txs() {
    submitStorageUpdate(benefactorNonce++, contractAt(MULTI_CONTRACT), 0, MULTI_FINAL_VALUE);
    submitStorageUpdate(benefactorNonce++, multiDeploy, 0, MULTI_DEPLOY_FINAL_VALUE);
  }

  /** Submits a setter-contract deploy; returns the contract account it will land at. */
  private Account submitDeploy(final long nonce, final int slotCount, final long gasLimit) {
    sendRaw(
        miner,
        RawTransaction.createContractTransaction(
            BigInteger.valueOf(nonce),
            GAS_PRICE,
            BigInteger.valueOf(gasLimit),
            BigInteger.ZERO,
            setterContractInitCode(slotCount)),
        BENEFACTOR);
    return contractAt(nonce);
  }

  private void submitStorageUpdate(
      final long nonce, final Account contract, final int slot, final int value) {
    sendRaw(
        miner,
        RawTransaction.createTransaction(
            BigInteger.valueOf(nonce),
            GAS_PRICE,
            BigInteger.valueOf(UPDATE_GAS_LIMIT),
            contract.getAddress(),
            BigInteger.ZERO,
            updateCalldata(slot, value)),
        BENEFACTOR);
  }

  private void submitTransfer(final long nonce, final String recipient, final BigInteger wei) {
    sendRaw(
        miner,
        RawTransaction.createEtherTransaction(
            BigInteger.valueOf(nonce),
            GAS_PRICE,
            BigInteger.valueOf(TRANSFER_GAS_LIMIT),
            recipient,
            wei),
        BENEFACTOR);
  }

  /**
   * EVM init code that writes {@code slotCount} slots in the constructor, then returns the 8-byte
   * {@link #SETTER_RUNTIME}. Slot indices, values, and the runtime offset must fit in one byte
   * (PUSH1 operands).
   */
  private static String setterContractInitCode(final int slotCount) {
    final StringBuilder code = new StringBuilder("0x");
    for (int j = 0; j < slotCount; j++) {
      // PUSH1 (j+1) ; PUSH1 j ; SSTORE
      code.append("60")
          .append(String.format("%02x", j + 1))
          .append("60")
          .append(String.format("%02x", j))
          .append("55");
    }
    final int runtimeOffset = slotCount * 5 + 7 + 5; // constructor + CODECOPY + RETURN prologues
    // PUSH1 8 ; PUSH1 runtimeOffset ; PUSH1 0 ; CODECOPY ; PUSH1 8 ; PUSH1 0 ; RETURN
    code.append("600860")
        .append(String.format("%02x", runtimeOffset))
        .append("600039")
        .append("60086000f3")
        .append(SETTER_RUNTIME.substring(2));
    return code.toString();
  }

  /** Calldata for {@link #SETTER_RUNTIME}. */
  private static String updateCalldata(final int slot, final int value) {
    return "0x" + wordHex(value) + wordHex(slot);
  }

  /** The built block at a 1-based chain height (the list is 0-based). */
  private static BuiltBlock chainBlock(final List<BuiltBlock> chain, final int height) {
    return chain.get(height - 1);
  }

  private static String chainBlockHash(final List<BuiltBlock> chain, final int height) {
    return chainBlock(chain, height).blockHash();
  }

  private void assertCode(final Account contract) {
    assertThat(syncNode.execute(ethTransactions.getCode(contract)))
        .isEqualTo(Bytes.fromHexString(SETTER_RUNTIME));
  }

  private void assertSameBalanceAsMiner(final String address) {
    final Account account = accountAt(Address.fromHexString(address));
    assertThat(syncNode.execute(ethTransactions.getBalance(account)))
        .isEqualTo(miner.execute(ethTransactions.getBalance(account)));
  }
}
