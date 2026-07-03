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
package org.hyperledger.besu.tests.acceptance.ethereum;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.crypto.SECP256K1;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.WaitUtils;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.transaction.net.CustomRequestFactory.BlockWithSlotNumber;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.web3j.protocol.core.methods.response.TransactionReceipt;

/**
 * Acceptance test for SLOTNUM opcode (EIP-7843).
 *
 * <p>This test verifies that the SLOTNUM opcode (0x4b) correctly returns the slot number from the
 * block header by sending real transactions to a contract that stores the slot number, then
 * verifying storage via {@code eth_getStorageAt} and verifying the block header via {@code
 * eth_getBlockByNumber}.
 */
public class EIP7843SlotNumOpcodeAcceptanceTest extends AcceptanceTestBase {
  private static final String GENESIS_FILE = "/dev/dev_amsterdam.json";
  private static final SECP256K1 secp256k1 = new SECP256K1();

  /**
   * Address of the pre-deployed SLOTNUM test contract. Bytecode: SLOTNUM PUSH0 SSTORE STOP
   * (0x4b5f5500) — stores the current slot number at storage key 0.
   */
  private static final Address SLOTNUM_CONTRACT_ADDRESS =
      Address.fromHexStringStrict("0x000000000000000000000000000000000000784b");

  public static final Bytes SENDER_PRIVATE_KEY =
      Bytes.fromHexString("3a4ff6d22d7502ef2452368165422861c01a0f72f851793b372b87888dc3c453");

  private BesuNode besuNode;
  private AmsterdamAcceptanceTestHelper testHelper;
  private Account slotnumContract;

  @BeforeEach
  void setUp() throws IOException {
    besuNode = besu.createExecutionEngineGenesisNode("besuNode", GENESIS_FILE);
    cluster.start(besuNode);
    testHelper = new AmsterdamAcceptanceTestHelper(besuNode, ethTransactions);
    slotnumContract = Account.create(ethTransactions, SLOTNUM_CONTRACT_ADDRESS);
  }

  @AfterEach
  void tearDown() {
    besuNode.close();
  }

  /**
   * Test that the SLOTNUM opcode returns the correct slot number from the block header.
   *
   * <p>This test sends transactions to the SLOTNUM contract across two blocks and verifies that the
   * stored slot number matches the expected value for each block. It also verifies that the block
   * header contains the correct slotNumber field.
   */
  @Test
  public void slotNumReturnsCorrectSlotNumber() throws IOException {
    // Build first block (slot 1) with a transaction calling the SLOTNUM contract
    final Transaction tx1 = createContractCallTransaction(0);
    final String txHash1 =
        besuNode.execute(ethTransactions.sendRawTransaction(tx1.encoded().toHexString()));
    testHelper.buildNewBlock();

    final TransactionReceipt receipt1 = waitForReceipt(txHash1);
    assertThat(receipt1.getStatus()).isEqualTo("0x1");

    // Verify storage slot 0 contains the slot number (1)
    final String storageValue1 =
        besuNode.execute(ethTransactions.getStorageAt(slotnumContract, BigInteger.ZERO));
    assertThat(storageValue1)
        .isEqualTo("0x0000000000000000000000000000000000000000000000000000000000000001");

    // Verify block header contains slotNumber field
    verifyBlockHeaderSlotNumber("0x1", 1L);

    // Build second block (slot 2) with another transaction
    final Transaction tx2 = createContractCallTransaction(1);
    final String txHash2 =
        besuNode.execute(ethTransactions.sendRawTransaction(tx2.encoded().toHexString()));
    testHelper.buildNewBlock();

    final TransactionReceipt receipt2 = waitForReceipt(txHash2);
    assertThat(receipt2.getStatus()).isEqualTo("0x1");

    // Verify storage slot 0 now contains 2
    final String storageValue2 =
        besuNode.execute(ethTransactions.getStorageAt(slotnumContract, BigInteger.ZERO));
    assertThat(storageValue2)
        .isEqualTo("0x0000000000000000000000000000000000000000000000000000000000000002");

    // Verify block header contains slotNumber field
    verifyBlockHeaderSlotNumber("0x2", 2L);
  }

  /**
   * Test that the SLOTNUM opcode increments correctly across multiple blocks.
   *
   * <p>This test builds several blocks, each containing a transaction that stores the slot number,
   * and verifies that the slot number increments with each block. Also verifies the block header
   * slotNumber field for each block.
   */
  @Test
  public void slotNumIncrementsBetweenBlocks() throws IOException {
    for (int expectedSlot = 1; expectedSlot <= 5; expectedSlot++) {
      final Transaction tx = createContractCallTransaction(expectedSlot - 1);
      final String txHash =
          besuNode.execute(ethTransactions.sendRawTransaction(tx.encoded().toHexString()));
      testHelper.buildNewBlock();

      final TransactionReceipt receipt = waitForReceipt(txHash);
      assertThat(receipt.getStatus()).isEqualTo("0x1");

      final String storageValue =
          besuNode.execute(ethTransactions.getStorageAt(slotnumContract, BigInteger.ZERO));
      final String expectedHex = String.format("0x%064x", BigInteger.valueOf(expectedSlot));
      assertThat(storageValue)
          .withFailMessage("Expected slot number %d but got %s", expectedSlot, storageValue)
          .isEqualTo(expectedHex);

      // Verify block header contains the correct slotNumber
      final String blockNumHex = "0x" + Long.toHexString(expectedSlot);
      verifyBlockHeaderSlotNumber(blockNumHex, expectedSlot);
    }
  }

  private void verifyBlockHeaderSlotNumber(
      final String blockNumber, final long expectedSlotNumber) {
    final Optional<BlockWithSlotNumber> maybeBlock =
        besuNode.execute(ethTransactions.getBlockWithSlotNumber(blockNumber));

    assertThat(maybeBlock).withFailMessage("Block %s not found", blockNumber).isPresent();

    final BlockWithSlotNumber block = maybeBlock.get();
    assertThat(block.getSlotNumber())
        .withFailMessage("Block %s header missing slotNumber field", blockNumber)
        .isNotNull();
    assertThat(Long.decode(block.getSlotNumber()))
        .withFailMessage(
            "Expected slotNumber %d in block %s header but got %s",
            expectedSlotNumber, blockNumber, block.getSlotNumber())
        .isEqualTo(expectedSlotNumber);
  }

  private Transaction createContractCallTransaction(final int nonce) {
    return Transaction.builder()
        .type(TransactionType.EIP1559)
        .chainId(BigInteger.valueOf(20211))
        .nonce(nonce)
        .maxPriorityFeePerGas(Wei.of(1_000_000_000))
        .maxFeePerGas(Wei.fromHexString("0x02540BE400"))
        .gasLimit(300_000)
        .to(SLOTNUM_CONTRACT_ADDRESS)
        .value(Wei.ZERO)
        .payload(Bytes.EMPTY)
        .signAndBuild(
            secp256k1.createKeyPair(
                secp256k1.createPrivateKey(SENDER_PRIVATE_KEY.toUnsignedBigInteger())));
  }

  private TransactionReceipt waitForReceipt(final String txHash) {
    final AtomicReference<Optional<TransactionReceipt>> maybeReceiptHolder =
        new AtomicReference<>(Optional.empty());
    WaitUtils.waitFor(
        60,
        () -> {
          maybeReceiptHolder.set(besuNode.execute(ethTransactions.getTransactionReceipt(txHash)));
          assertThat(maybeReceiptHolder.get()).isPresent();
        });
    return maybeReceiptHolder.get().orElseThrow();
  }
}
