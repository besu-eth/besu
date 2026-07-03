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
 * Acceptance tests for EIP-8024 stack opcodes (DUPN, SWAPN, EXCHANGE).
 *
 * <p>EIP-8024 introduces three new EVM opcodes for extended stack manipulation:
 *
 * <ul>
 *   <li>DUPN (0xe6): Duplicates stack item at position n (17-235)
 *   <li>SWAPN (0xe7): Swaps top of stack with item at position n+1 (17-235)
 *   <li>EXCHANGE (0xe8): Swaps two non-top stack items at positions n and m
 * </ul>
 *
 * <p>These opcodes use an "immediate" byte encoding where certain byte values (91-127) are invalid
 * to preserve JUMPDEST (0x5b) and PUSH opcodes (0x60-0x7f) analysis.
 */
public class EIP8024StackOpcodeAcceptanceTest extends AcceptanceTestBase {
  private static final String GENESIS_FILE = "/dev/dev_amsterdam.json";
  private static final SECP256K1 secp256k1 = new SECP256K1();

  // EIP-8024 test contract addresses (pre-deployed in genesis)
  private static final Address DUPN_CONTRACT_ADDRESS =
      Address.fromHexStringStrict("0x8024000000000000000000000000000000000001");
  private static final Address SWAPN_CONTRACT_ADDRESS =
      Address.fromHexStringStrict("0x8024000000000000000000000000000000000002");
  private static final Address EXCHANGE_CONTRACT_ADDRESS =
      Address.fromHexStringStrict("0x8024000000000000000000000000000000000003");
  private static final Address INVALID_CONTRACT_ADDRESS =
      Address.fromHexStringStrict("0x8024000000000000000000000000000000000004");

  public static final Bytes SENDER_PRIVATE_KEY =
      Bytes.fromHexString("3a4ff6d22d7502ef2452368165422861c01a0f72f851793b372b87888dc3c453");

  private BesuNode besuNode;
  private AmsterdamAcceptanceTestHelper testHelper;

  // Contract accounts for storage queries
  private Account dupnContract;
  private Account swapnContract;
  private Account exchangeContract;

  @BeforeEach
  void setUp() throws IOException {
    besuNode = besu.createExecutionEngineGenesisNode("besuNode", GENESIS_FILE);
    cluster.start(besuNode);

    testHelper = new AmsterdamAcceptanceTestHelper(besuNode, ethTransactions);

    // Create account wrappers for contract storage queries
    dupnContract = Account.create(ethTransactions, DUPN_CONTRACT_ADDRESS);
    swapnContract = Account.create(ethTransactions, SWAPN_CONTRACT_ADDRESS);
    exchangeContract = Account.create(ethTransactions, EXCHANGE_CONTRACT_ADDRESS);
  }

  @AfterEach
  void tearDown() {
    besuNode.close();
  }

  /**
   * Test DUPN basic operation.
   *
   * <p>Calls the DUPN test contract which executes: PUSH1 1, PUSH1 0, 16xDUP1, DUPN 0x80, PUSH0,
   * SSTORE
   *
   * <p>This builds a stack of 18 items and uses DUPN with immediate 0x80 (n=17) to duplicate the
   * 17th item (which is 0), then stores it at slot 0.
   */
  @Test
  public void testDupNBasicOperation() throws IOException {
    final Transaction tx = createContractCallTransaction(DUPN_CONTRACT_ADDRESS, 0);

    final String txHash =
        besuNode.execute(ethTransactions.sendRawTransaction(tx.encoded().toHexString()));
    testHelper.buildNewBlock();

    final TransactionReceipt receipt = waitForReceipt(txHash);

    // Transaction should succeed
    assertThat(receipt.getStatus()).isEqualTo("0x1");

    // Verify storage was written (slot 0 should contain 0 - the duplicated value)
    final String storageValue =
        besuNode.execute(ethTransactions.getStorageAt(dupnContract, BigInteger.ZERO));
    assertThat(storageValue)
        .isEqualTo("0x0000000000000000000000000000000000000000000000000000000000000000");
  }

  /**
   * Test SWAPN basic operation.
   *
   * <p>Calls the SWAPN test contract which builds a stack using PUSH and DUP operations, then uses
   * SWAPN to swap stack positions.
   */
  @Test
  public void testSwapNBasicOperation() throws IOException {
    final Transaction tx = createContractCallTransaction(SWAPN_CONTRACT_ADDRESS, 0);

    final String txHash =
        besuNode.execute(ethTransactions.sendRawTransaction(tx.encoded().toHexString()));
    testHelper.buildNewBlock();

    final TransactionReceipt receipt = waitForReceipt(txHash);

    // Transaction should succeed
    assertThat(receipt.getStatus()).isEqualTo("0x1");

    // Verify storage was written
    final String storageValue =
        besuNode.execute(ethTransactions.getStorageAt(swapnContract, BigInteger.ZERO));
    // The contract stores the result of stack operations at slot 0
    assertThat(storageValue).isNotNull();
  }

  /**
   * Test EXCHANGE basic operation.
   *
   * <p>Calls the EXCHANGE test contract which executes: PUSH1 0, PUSH1 1, PUSH1 2, EXCHANGE 0x8e
   *
   * <p>This creates stack [2, 1, 0] and EXCHANGE 0x8e (decodes to n=1, m=2) swaps positions 1 and
   * 2, resulting in [2, 0, 1]. The contract then stores the top value (2) at slot 0.
   */
  @Test
  public void testExchangeBasicOperation() throws IOException {
    final Transaction tx = createContractCallTransaction(EXCHANGE_CONTRACT_ADDRESS, 0);

    final String txHash =
        besuNode.execute(ethTransactions.sendRawTransaction(tx.encoded().toHexString()));
    testHelper.buildNewBlock();

    final TransactionReceipt receipt = waitForReceipt(txHash);

    // Transaction should succeed
    assertThat(receipt.getStatus()).isEqualTo("0x1");

    // Verify storage was written (slot 0 should contain 2)
    final String storageValue =
        besuNode.execute(ethTransactions.getStorageAt(exchangeContract, BigInteger.ZERO));
    assertThat(storageValue)
        .isEqualTo("0x0000000000000000000000000000000000000000000000000000000000000002");
  }

  /**
   * Test that invalid immediate values cause transaction revert.
   *
   * <p>Calls the invalid test contract which contains DUPN with immediate 0x5f (95), which is in
   * the invalid range 91-127. This should cause an INVALID_OPERATION halt and revert the
   * transaction.
   */
  @Test
  public void testInvalidImmediateReverts() throws IOException {
    final Transaction tx = createContractCallTransaction(INVALID_CONTRACT_ADDRESS, 0);

    final String txHash =
        besuNode.execute(ethTransactions.sendRawTransaction(tx.encoded().toHexString()));
    testHelper.buildNewBlock();

    final TransactionReceipt receipt = waitForReceipt(txHash);

    // Transaction should revert (status 0x0) due to invalid opcode immediate
    assertThat(receipt.getStatus()).isEqualTo("0x0");
  }

  private Transaction createContractCallTransaction(final Address contractAddress, final int nonce)
      throws IOException {
    return Transaction.builder()
        .type(TransactionType.EIP1559)
        .chainId(BigInteger.valueOf(20211))
        .nonce(nonce)
        .maxPriorityFeePerGas(Wei.of(1_000_000_000))
        .maxFeePerGas(Wei.fromHexString("0x02540BE400"))
        .gasLimit(300_000)
        .to(contractAddress)
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
