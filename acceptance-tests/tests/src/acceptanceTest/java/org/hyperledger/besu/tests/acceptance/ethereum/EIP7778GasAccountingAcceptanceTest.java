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
 * Acceptance tests for EIP-7778: Block gas accounting without refunds.
 *
 * <p>EIP-7778 changes block gas accounting to use pre-refund gas for block gas limit enforcement,
 * preventing circumvention of the block gas limit via refunds.
 *
 * <p>Key changes in EIP-7778:
 *
 * <ul>
 *   <li>Block gas limit is enforced using pre-refund gas
 *   <li>Transactions cannot be packed more tightly by exploiting refunds
 *   <li>User economics are unchanged - users still receive refunds (receipt cumulativeGasUsed
 *       reflects post-refund gas)
 * </ul>
 */
public class EIP7778GasAccountingAcceptanceTest extends AcceptanceTestBase {
  private static final String GENESIS_FILE = "/dev/dev_amsterdam.json";
  private static final SECP256K1 secp256k1 = new SECP256K1();

  public static final Bytes SENDER_PRIVATE_KEY =
      Bytes.fromHexString("3a4ff6d22d7502ef2452368165422861c01a0f72f851793b372b87888dc3c453");

  private final Account recipient = accounts.createAccount("recipient");

  private BesuNode besuNode;
  private AmsterdamAcceptanceTestHelper testHelper;

  @BeforeEach
  void setUp() throws IOException {
    besuNode = besu.createExecutionEngineGenesisNode("besuNode", GENESIS_FILE);
    cluster.start(besuNode);

    testHelper = new AmsterdamAcceptanceTestHelper(besuNode, ethTransactions);
  }

  @AfterEach
  void tearDown() {
    besuNode.close();
  }

  /** Tests that simple ETH transfers work correctly with EIP-7778 block gas accounting. */
  @Test
  public void shouldProcessSimpleTransferSuccessfully() throws IOException {
    final Wei transferAmount = Wei.of(1_000_000_000_000_000L); // 0.001 ETH

    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.valueOf(20211))
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(1_000_000_000))
            .maxFeePerGas(Wei.fromHexString("0x02540BE400"))
            .gasLimit(300_000)
            .to(Address.fromHexStringStrict(recipient.getAddress()))
            .value(transferAmount)
            .payload(Bytes.EMPTY)
            .signAndBuild(
                secp256k1.createKeyPair(
                    secp256k1.createPrivateKey(SENDER_PRIVATE_KEY.toUnsignedBigInteger())));

    final String txHash =
        besuNode.execute(ethTransactions.sendRawTransaction(tx.encoded().toHexString()));
    testHelper.buildNewBlock();

    final AtomicReference<Optional<TransactionReceipt>> maybeReceiptHolder =
        new AtomicReference<>(Optional.empty());
    WaitUtils.waitFor(
        60,
        () -> {
          maybeReceiptHolder.set(besuNode.execute(ethTransactions.getTransactionReceipt(txHash)));
          assertThat(maybeReceiptHolder.get()).isPresent();
        });

    final TransactionReceipt receipt = maybeReceiptHolder.get().orElseThrow();
    assertThat(receipt.getStatus()).isEqualTo("0x1");
    // Amsterdam gas model for a value transfer that creates the recipient account:
    // 18,000 EIP-2780 intrinsic (TX_BASE 12,000 + TX_VALUE_COST 4,244 + TRANSFER_LOG_COST 1,756)
    // + 3,000 cold recipient access + 183,600 EIP-8037 new-account state gas (120 bytes * 1,530
    // cost-per-state-byte) = 204,600.
    assertThat(receipt.getGasUsed().longValue()).isEqualTo(204600L);
  }

  /**
   * Tests that transactions with refunds work correctly. The receipt cumulativeGasUsed should
   * reflect post-refund gas (what users pay), while block gas accounting uses pre-refund gas.
   */
  @Test
  public void shouldProcessTransactionWithRefundSuccessfully() throws IOException {
    // Contract address for the SSTORE refund test contract (defined in dev_amsterdam.json)
    final Address sstoreRefundContract =
        Address.fromHexStringStrict("0x0000000000000000000000000000000000007778");

    // Call the contract which clears storage slot 0, generating a refund
    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.valueOf(20211))
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(1_000_000_000))
            .maxFeePerGas(Wei.fromHexString("0x02540BE400"))
            .gasLimit(100_000) // Enough gas for SSTORE and refund
            .to(sstoreRefundContract)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY) // No calldata needed, contract executes on any call
            .signAndBuild(
                secp256k1.createKeyPair(
                    secp256k1.createPrivateKey(SENDER_PRIVATE_KEY.toUnsignedBigInteger())));

    final String txHash =
        besuNode.execute(ethTransactions.sendRawTransaction(tx.encoded().toHexString()));
    testHelper.buildNewBlock();

    final AtomicReference<Optional<TransactionReceipt>> maybeReceiptHolder =
        new AtomicReference<>(Optional.empty());
    WaitUtils.waitFor(
        60,
        () -> {
          maybeReceiptHolder.set(besuNode.execute(ethTransactions.getTransactionReceipt(txHash)));
          assertThat(maybeReceiptHolder.get()).isPresent();
        });

    final TransactionReceipt receipt = maybeReceiptHolder.get().orElseThrow();
    assertThat(receipt.getStatus()).isEqualTo("0x1");

    // Receipt cumulativeGasUsed reflects post-refund gas (what users pay)
    // The SSTORE clear generates a 4800 gas refund (SSTORE_CLEARS_SCHEDULE in post-EIP-3529)
    final long gasUsed = receipt.getGasUsed().longValue();
    assertThat(gasUsed).isGreaterThan(21000L); // More than a simple transfer
    assertThat(gasUsed).isLessThan(100_000L); // Less than gas limit due to refund
  }

  /**
   * Tests that multiple transactions in a block work correctly with EIP-7778 gas accounting. Block
   * gas is tracked using pre-refund gas, but receipt cumulativeGasUsed uses post-refund gas.
   */
  @Test
  public void shouldProcessMultipleTransactionsInBlock() throws IOException {
    // First transaction
    final Transaction tx1 =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.valueOf(20211))
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(1_000_000_000))
            .maxFeePerGas(Wei.fromHexString("0x02540BE400"))
            .gasLimit(300_000)
            .to(Address.fromHexStringStrict(recipient.getAddress()))
            .value(Wei.of(1000))
            .payload(Bytes.EMPTY)
            .signAndBuild(
                secp256k1.createKeyPair(
                    secp256k1.createPrivateKey(SENDER_PRIVATE_KEY.toUnsignedBigInteger())));

    // Second transaction
    final Transaction tx2 =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.valueOf(20211))
            .nonce(1)
            .maxPriorityFeePerGas(Wei.of(1_000_000_000))
            .maxFeePerGas(Wei.fromHexString("0x02540BE400"))
            .gasLimit(300_000)
            .to(Address.fromHexStringStrict(recipient.getAddress()))
            .value(Wei.of(2000))
            .payload(Bytes.EMPTY)
            .signAndBuild(
                secp256k1.createKeyPair(
                    secp256k1.createPrivateKey(SENDER_PRIVATE_KEY.toUnsignedBigInteger())));

    final String txHash1 =
        besuNode.execute(ethTransactions.sendRawTransaction(tx1.encoded().toHexString()));
    final String txHash2 =
        besuNode.execute(ethTransactions.sendRawTransaction(tx2.encoded().toHexString()));
    testHelper.buildNewBlock();

    // Wait for both receipts
    final AtomicReference<Optional<TransactionReceipt>> receipt1Holder =
        new AtomicReference<>(Optional.empty());
    final AtomicReference<Optional<TransactionReceipt>> receipt2Holder =
        new AtomicReference<>(Optional.empty());

    WaitUtils.waitFor(
        60,
        () -> {
          receipt1Holder.set(besuNode.execute(ethTransactions.getTransactionReceipt(txHash1)));
          receipt2Holder.set(besuNode.execute(ethTransactions.getTransactionReceipt(txHash2)));
          assertThat(receipt1Holder.get()).isPresent();
          assertThat(receipt2Holder.get()).isPresent();
        });

    final TransactionReceipt receipt1 = receipt1Holder.get().orElseThrow();
    final TransactionReceipt receipt2 = receipt2Holder.get().orElseThrow();

    assertThat(receipt1.getStatus()).isEqualTo("0x1");
    assertThat(receipt2.getStatus()).isEqualTo("0x1");

    // Verify cumulative gas accounting is correct under the Amsterdam gas model.
    // tx1 creates the recipient account: 204,600 (see shouldProcessSimpleTransferSuccessfully).
    // tx2 transfers to the now-existing account: 18,000 EIP-2780 intrinsic + 3,000 cold access =
    // 21,000 (no new-account state gas), so cumulative = 204,600 + 21,000 = 225,600.
    assertThat(receipt1.getCumulativeGasUsed().longValue()).isEqualTo(204600L);
    assertThat(receipt2.getCumulativeGasUsed().longValue()).isEqualTo(225600L);
  }
}
