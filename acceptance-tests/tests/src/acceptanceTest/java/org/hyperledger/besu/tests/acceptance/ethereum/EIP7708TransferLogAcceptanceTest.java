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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.web3j.protocol.core.methods.response.Log;
import org.web3j.protocol.core.methods.response.TransactionReceipt;

/**
 * Acceptance tests for EIP-7708: ETH transfers emit a log.
 *
 * <p>EIP-7708 specifies that all nonzero ETH value transfers emit a LOG3-equivalent log with:
 *
 * <ul>
 *   <li>address: SYSTEM_ADDRESS (0xfffffffffffffffffffffffffffffffffffffffe)
 *   <li>topics[0]: Transfer event signature (keccak256('Transfer(address,address,uint256)'))
 *   <li>topics[1]: from address (zero-padded to 32 bytes)
 *   <li>topics[2]: to address (zero-padded to 32 bytes)
 *   <li>data: amount in Wei (big-endian uint256)
 * </ul>
 *
 * <p>Additionally, SELFDESTRUCT operations emit different logs depending on the beneficiary:
 *
 * <ul>
 *   <li>SELFDESTRUCT to different address: Transfer log (LOG3) with 3 topics
 *   <li>SELFDESTRUCT to self: Burn log (LOG2) with 2 topics
 * </ul>
 */
public class EIP7708TransferLogAcceptanceTest extends AcceptanceTestBase {
  private static final String GENESIS_FILE = "/dev/dev_amsterdam.json";
  private static final SECP256K1 secp256k1 = new SECP256K1();

  private static final String EIP7708_SYSTEM_ADDRESS = "0xfffffffffffffffffffffffffffffffffffffffe";
  private static final String TRANSFER_TOPIC =
      "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
  private static final String BURN_TOPIC =
      "0xcc16f5dbb4873280815c1ee09dbd06736cffcc184412cf7a71a0fdb75d397ca5";

  private final Account sender =
      accounts.createAccount(
          Address.fromHexStringStrict("a05b21E5186Ce93d2a226722b85D6e550Ac7D6E3"));
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

  @Test
  public void shouldEmitTransferLogForSimpleEthTransfer() throws IOException {
    final Wei transferAmount = Wei.of(1_000_000_000_000_000L); // 0.001 ETH

    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.valueOf(20211))
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(1_000_000_000))
            .maxFeePerGas(Wei.fromHexString("0x02540BE400"))
            .gasLimit(21_000)
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

    final List<Log> logs = receipt.getLogs();
    assertThat(logs).hasSize(1);

    final Log transferLog = logs.getFirst();

    // Verify the log is from the EIP-7708 system address
    assertThat(transferLog.getAddress()).isEqualToIgnoringCase(EIP7708_SYSTEM_ADDRESS);

    // Verify the Transfer event signature topic
    assertThat(transferLog.getTopics()).hasSize(3);
    assertThat(transferLog.getTopics().getFirst()).isEqualToIgnoringCase(TRANSFER_TOPIC);

    // Verify sender address (zero-padded to 32 bytes)
    final Address senderAddress = Address.fromHexStringStrict(sender.getAddress());
    final String expectedSenderTopic = Bytes32.leftPad(senderAddress.getBytes()).toHexString();
    assertThat(transferLog.getTopics().get(1)).isEqualToIgnoringCase(expectedSenderTopic);

    // Verify recipient address (zero-padded to 32 bytes)
    final Address recipientAddress = Address.fromHexStringStrict(recipient.getAddress());
    final String expectedRecipientTopic =
        Bytes32.leftPad(recipientAddress.getBytes()).toHexString();
    assertThat(transferLog.getTopics().get(2)).isEqualToIgnoringCase(expectedRecipientTopic);

    // Verify the data contains the transfer amount (big-endian uint256)
    final String expectedData = Bytes32.leftPad(transferAmount).toHexString();
    assertThat(transferLog.getData()).isEqualToIgnoringCase(expectedData);
  }

  @Test
  public void shouldNotEmitTransferLogForZeroValueTransfer() throws IOException {
    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.valueOf(20211))
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(1_000_000_000))
            .maxFeePerGas(Wei.fromHexString("0x02540BE400"))
            .gasLimit(21_000)
            .to(Address.fromHexStringStrict(recipient.getAddress()))
            .value(Wei.ZERO)
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

    // Zero-value transfers should NOT emit an EIP-7708 log
    assertThat(receipt.getLogs()).isEmpty();
  }

  /**
   * Test that internal CALL with value emits multiple EIP-7708 transfer logs.
   *
   * <p>This test sends ETH to a contract (0x9999) that forwards all received ETH to an address
   * provided in calldata. Two transfer logs should be emitted: one for the initial transfer to the
   * contract, and one for the internal CALL transfer from the contract to the final recipient.
   */
  @Test
  public void shouldEmitMultipleTransferLogsForContractCallWithValue() throws IOException {
    final Wei transferAmount = Wei.of(1_000_000_000_000_000L); // 0.001 ETH
    final Address forwarderContract =
        Address.fromHexStringStrict("0x0000000000000000000000000000000000009999");

    // Calldata is the recipient address left-padded to 32 bytes
    final Address recipientAddress = Address.fromHexStringStrict(recipient.getAddress());
    final Bytes callData = Bytes32.leftPad(recipientAddress.getBytes());

    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.valueOf(20211))
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(1_000_000_000))
            .maxFeePerGas(Wei.fromHexString("0x02540BE400"))
            .gasLimit(300_000)
            .to(forwarderContract)
            .value(transferAmount)
            .payload(callData)
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

    final List<Log> logs = receipt.getLogs();
    assertThat(logs).hasSize(2);

    // Log 1: sender -> forwarder contract
    final Log log1 = logs.getFirst();
    assertThat(log1.getAddress()).isEqualToIgnoringCase(EIP7708_SYSTEM_ADDRESS);
    assertThat(log1.getTopics()).hasSize(3);
    assertThat(log1.getTopics().get(0)).isEqualToIgnoringCase(TRANSFER_TOPIC);
    final Address senderAddress = Address.fromHexStringStrict(sender.getAddress());
    assertThat(log1.getTopics().get(1))
        .isEqualToIgnoringCase(Bytes32.leftPad(senderAddress.getBytes()).toHexString());
    assertThat(log1.getTopics().get(2))
        .isEqualToIgnoringCase(Bytes32.leftPad(forwarderContract.getBytes()).toHexString());
    assertThat(log1.getData()).isEqualToIgnoringCase(Bytes32.leftPad(transferAmount).toHexString());

    // Log 2: forwarder contract -> recipient (via internal CALL)
    final Log log2 = logs.get(1);
    assertThat(log2.getAddress()).isEqualToIgnoringCase(EIP7708_SYSTEM_ADDRESS);
    assertThat(log2.getTopics()).hasSize(3);
    assertThat(log2.getTopics().get(0)).isEqualToIgnoringCase(TRANSFER_TOPIC);
    assertThat(log2.getTopics().get(1))
        .isEqualToIgnoringCase(Bytes32.leftPad(forwarderContract.getBytes()).toHexString());
    assertThat(log2.getTopics().get(2))
        .isEqualToIgnoringCase(Bytes32.leftPad(recipientAddress.getBytes()).toHexString());
    // The contract forwards its entire balance (which is the transfer amount)
    assertThat(log2.getData()).isEqualToIgnoringCase(Bytes32.leftPad(transferAmount).toHexString());
  }

  /**
   * Test that SELFDESTRUCT to a DIFFERENT address emits an EIP-7708 Transfer log (LOG3).
   *
   * <p>This test calls a contract (0x7708) that self-destructs and sends its balance to an address
   * provided in calldata. Since the beneficiary is different from the contract itself, an EIP-7708
   * Transfer log (LOG3 with 3 topics) should be emitted.
   */
  @Test
  public void shouldEmitTransferLogForSelfDestructToDifferentAddress() throws IOException {
    final Address selfDestructContract =
        Address.fromHexStringStrict("0x0000000000000000000000000000000000007708");
    // Contract has 1 ETH balance from genesis
    final Wei contractBalance = Wei.of(1_000_000_000_000_000_000L);

    // Calldata is the beneficiary address left-padded to 32 bytes
    final Address beneficiary = Address.fromHexStringStrict(recipient.getAddress());
    final Bytes callData = Bytes32.leftPad(beneficiary.getBytes());

    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.valueOf(20211))
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(1_000_000_000))
            .maxFeePerGas(Wei.fromHexString("0x02540BE400"))
            .gasLimit(300_000)
            .to(selfDestructContract)
            .value(Wei.ZERO)
            .payload(callData)
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

    final List<Log> logs = receipt.getLogs();
    assertThat(logs).hasSize(1);

    // Log: selfDestructContract -> beneficiary (via SELFDESTRUCT)
    final Log transferLog = logs.getFirst();
    assertThat(transferLog.getAddress()).isEqualToIgnoringCase(EIP7708_SYSTEM_ADDRESS);
    assertThat(transferLog.getTopics()).hasSize(3);
    assertThat(transferLog.getTopics().get(0)).isEqualToIgnoringCase(TRANSFER_TOPIC);
    assertThat(transferLog.getTopics().get(1))
        .isEqualToIgnoringCase(Bytes32.leftPad(selfDestructContract.getBytes()).toHexString());
    assertThat(transferLog.getTopics().get(2))
        .isEqualToIgnoringCase(Bytes32.leftPad(beneficiary.getBytes()).toHexString());
    assertThat(transferLog.getData())
        .isEqualToIgnoringCase(Bytes32.leftPad(contractBalance).toHexString());
  }

  /**
   * Test that SELFDESTRUCT to SELF for a pre-existing contract emits NO log under EIP-6780.
   *
   * <p>With EIP-6780 (post-Cancun), SELFDESTRUCT only destroys contracts created in the same
   * transaction. For pre-existing contracts (like 0x7709 from genesis), SELFDESTRUCT to self is
   * effectively a no-op - the contract is NOT destroyed and retains its balance.
   *
   * <p>Per EIP-7708, since no actual destruction or value transfer occurs, no log should be
   * emitted. This is different from pre-Cancun behavior where the contract would be destroyed and a
   * Burn log would be emitted.
   */
  @Test
  public void shouldNotEmitLogForPreExistingContractSelfDestructToSelf() throws IOException {
    final Address selfDestructToSelfContract =
        Address.fromHexStringStrict("0x0000000000000000000000000000000000007709");

    // No calldata needed - contract selfdestructs to itself
    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.valueOf(20211))
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(1_000_000_000))
            .maxFeePerGas(Wei.fromHexString("0x02540BE400"))
            .gasLimit(100_000)
            .to(selfDestructToSelfContract)
            .value(Wei.ZERO)
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

    // Under EIP-6780, pre-existing contracts selfdestructing to self is a no-op.
    // No destruction happens, no value moves, so no EIP-7708 log should be emitted.
    assertThat(receipt.getLogs())
        .as("Pre-existing contract SELFDESTRUCT to self should NOT emit any logs under EIP-6780")
        .isEmpty();
  }

  /**
   * Test that pre-existing contracts selfdestructing to self emit NO logs under EIP-6780.
   *
   * <p>This test calls a destroyer contract (0x7700) that triggers selfdestructs on three
   * pre-existing contracts (0x7701, 0x7702, 0x7703). Each contract selfdestructs to itself.
   *
   * <p>Under EIP-6780 (post-Cancun), SELFDESTRUCT only destroys contracts created in the same
   * transaction. Since these contracts are pre-existing (from genesis), SELFDESTRUCT to self is a
   * no-op - no destruction occurs and no value moves.
   *
   * <p>Per EIP-7708, since no actual destruction or value transfer occurs, no logs should be
   * emitted.
   */
  @Test
  public void shouldNotEmitLogsForPreExistingContractsSelfDestructingToSelf() throws IOException {
    final Address destroyerContract =
        Address.fromHexStringStrict("0x0000000000000000000000000000000000007700");

    // Call destroyer contract to trigger all three selfdestructs
    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.valueOf(20211))
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(1_000_000_000))
            .maxFeePerGas(Wei.fromHexString("0x02540BE400"))
            .gasLimit(500_000)
            .to(destroyerContract)
            .value(Wei.ZERO)
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

    // Under EIP-6780, pre-existing contracts selfdestructing to self is a no-op.
    // No destruction happens, no value moves, so no EIP-7708 logs should be emitted.
    assertThat(receipt.getLogs())
        .as("Pre-existing contracts SELFDESTRUCT to self should NOT emit any logs under EIP-6780")
        .isEmpty();
  }

  /**
   * Test that reverted transactions do NOT emit EIP-7708 transfer logs.
   *
   * <p>This test sends ETH to a contract (0x666) that immediately reverts. Even though value was
   * sent, no EIP-7708 transfer log should be emitted because the transaction reverted.
   */
  @Test
  public void shouldNotEmitTransferLogForRevertedTransaction() throws IOException {
    final Wei transferAmount = Wei.of(1_000_000_000_000_000L); // 0.001 ETH
    final Address revertContract =
        Address.fromHexStringStrict("0x0000000000000000000000000000000000000666");

    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.valueOf(20211))
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(1_000_000_000))
            .maxFeePerGas(Wei.fromHexString("0x02540BE400"))
            .gasLimit(100_000)
            .to(revertContract)
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

    // Transaction should have reverted (status 0x0)
    assertThat(receipt.getStatus()).isEqualTo("0x0");

    // Reverted transactions should NOT emit any EIP-7708 logs
    assertThat(receipt.getLogs()).isEmpty();
  }

  /**
   * Test that SELFDESTRUCT to SELF for a same-tx-created contract DOES emit a Burn log.
   *
   * <p>This test calls a factory contract (0x7710) that:
   *
   * <ol>
   *   <li>CREATEs a new child contract with code "ADDRESS SELFDESTRUCT" (30FF)
   *   <li>Sends its entire balance (1 ETH) to the child
   *   <li>CALLs the child, triggering SELFDESTRUCT to itself
   * </ol>
   *
   * <p>Under EIP-6780, contracts created in the same transaction CAN be destroyed by SELFDESTRUCT.
   * Per EIP-7708, when a same-tx-created contract selfdestructs to itself, a Burn log (LOG2) should
   * be emitted with the destroyed balance.
   */
  @Test
  public void shouldEmitSelfdestructLogForSameTxCreatedContractSelfDestructToSelf()
      throws IOException {
    final Address factoryContract =
        Address.fromHexStringStrict("0x0000000000000000000000000000000000007710");
    // Factory has 1 ETH balance which it sends to the created contract
    final Wei contractBalance = Wei.of(1_000_000_000_000_000_000L);

    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .chainId(BigInteger.valueOf(20211))
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(1_000_000_000))
            .maxFeePerGas(Wei.fromHexString("0x02540BE400"))
            .gasLimit(200_000)
            .to(factoryContract)
            .value(Wei.ZERO)
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

    final List<Log> logs = receipt.getLogs();

    // Should have at least one Burn log (LOG2 with 2 topics)
    final List<Log> selfdestructLogs =
        logs.stream()
            .filter(log -> log.getTopics().size() == 2)
            .filter(log -> log.getTopics().get(0).equalsIgnoreCase(BURN_TOPIC))
            .toList();

    assertThat(selfdestructLogs)
        .as("Same-tx-created contract SELFDESTRUCT to self SHOULD emit a Burn log")
        .isNotEmpty();

    // Verify the Burn log has the correct balance
    final Log selfdestructLog = selfdestructLogs.getFirst();
    assertThat(selfdestructLog.getAddress()).isEqualToIgnoringCase(EIP7708_SYSTEM_ADDRESS);
    assertThat(selfdestructLog.getData())
        .isEqualToIgnoringCase(Bytes32.leftPad(contractBalance).toHexString());

    // There should also be a Transfer log for the CREATE value transfer (factory -> child)
    final boolean hasTransferLog =
        logs.stream()
            .anyMatch(
                log ->
                    log.getTopics().size() == 3
                        && log.getTopics().get(0).equalsIgnoreCase(TRANSFER_TOPIC));
    assertThat(hasTransferLog)
        .as("CREATE with value should emit a Transfer log for the value transfer")
        .isTrue();
  }
}
