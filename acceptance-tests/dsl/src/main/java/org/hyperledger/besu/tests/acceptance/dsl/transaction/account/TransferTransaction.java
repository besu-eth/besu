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
package org.hyperledger.besu.tests.acceptance.dsl.transaction.account;

import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.blockchain.Amount;
import org.hyperledger.besu.tests.acceptance.dsl.transaction.NodeRequests;
import org.hyperledger.besu.tests.acceptance.dsl.transaction.SignUtil;
import org.hyperledger.besu.tests.acceptance.dsl.transaction.Transaction;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import org.web3j.crypto.RawTransaction;
import org.web3j.protocol.core.methods.response.EthSendTransaction;
import org.web3j.utils.Convert;
import org.web3j.utils.Convert.Unit;
import org.web3j.utils.Numeric;

public class TransferTransaction implements Transaction<Hash> {

  /** Price for each GAS units in this transaction (wei). */
  private static final BigInteger MINIMUM_GAS_PRICE = BigInteger.valueOf(1000);

  /** Number of GAS units that the transaction will cost. */
  private static final BigInteger INTRINSIC_GAS = BigInteger.valueOf(21000);

  private final Account sender;
  private final Account recipient;
  private final BigDecimal transferAmount;
  private final Unit transferUnit;
  private final BigInteger gasPrice;
  private final BigInteger nonce;
  private final Optional<BigInteger> chainId;
  private final SignatureAlgorithm signatureAlgorithm;
  private final TransactionType transactionType;
  private final Optional<BigInteger> gasLimit;
  private byte[] signedTxData = null;

  public TransferTransaction(
      final Account sender,
      final Account recipient,
      final Amount transferAmount,
      final Amount gasPrice,
      final BigInteger nonce,
      final Optional<BigInteger> chainId,
      final SignatureAlgorithm signatureAlgorithm,
      final TransactionType transactionType,
      final Optional<BigInteger> gasLimit) {
    this.sender = sender;
    this.recipient = recipient;
    this.transferAmount = transferAmount.getValue();
    this.transferUnit = transferAmount.getUnit();
    this.gasPrice = gasPrice == null ? MINIMUM_GAS_PRICE : convertGasPriceToWei(gasPrice);
    this.nonce = nonce;
    this.chainId = chainId;
    this.signatureAlgorithm = signatureAlgorithm;
    this.transactionType = transactionType;
    this.gasLimit = gasLimit;
  }

  @Override
  public Hash execute(final NodeRequests node) {
    final String signedTransactionData = signedTransactionData();
    return sendRawTransaction(node, signedTransactionData);
  }

  public Amount executionCost() {
    return Amount.wei(INTRINSIC_GAS.multiply(gasPrice));
  }

  public String signedTransactionData() {
    return Numeric.toHexString(createSignedTransactionData());
  }

  public String transactionHash() {
    final byte[] signedTx = createSignedTransactionData();
    final byte[] txHash = org.web3j.crypto.Hash.sha3(signedTx);
    return Numeric.toHexString(txHash);
  }

  private byte[] createSignedTransactionData() {
    if (signedTxData == null) {
      signedTxData =
          SignUtil.signTransaction(createRawTransaction(), sender, signatureAlgorithm, chainId);
    }
    return signedTxData;
  }

  private Hash sendRawTransaction(final NodeRequests node, final String signedTransactionData) {
    try {
      final EthSendTransaction transaction =
          node.eth().ethSendRawTransaction(signedTransactionData).send();
      if (transaction.getResult() == null && transaction.getError() != null) {
        throw new RuntimeException(
            "Error sending transaction: " + transaction.getError().getMessage());
      }
      return Hash.fromHexString(transaction.getTransactionHash());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private BigInteger getNonce() {
    return Optional.ofNullable(nonce).orElseGet(sender::getNextNonce);
  }

  private BigInteger convertGasPriceToWei(final Amount unconverted) {
    return Convert.toWei(unconverted.getValue(), unconverted.getUnit()).toBigInteger();
  }

  private RawTransaction createRawTransaction() {
    return transactionType == TransactionType.FRONTIER
        ? createFrontierTransaction()
        : create1559Transaction(chainId.orElseThrow());
  }

  private RawTransaction createFrontierTransaction() {
    return RawTransaction.createEtherTransaction(
        getNonce(),
        gasPrice,
        gasLimit.orElse(INTRINSIC_GAS),
        recipient.getAddress(),
        Convert.toWei(transferAmount, transferUnit).toBigIntegerExact());
  }

  private RawTransaction create1559Transaction(final BigInteger chainId) {
    return RawTransaction.createEtherTransaction(
        chainId.longValueExact(),
        getNonce(),
        gasLimit.orElse(INTRINSIC_GAS),
        recipient.getAddress(),
        Convert.toWei(transferAmount, transferUnit).toBigIntegerExact(),
        BigInteger.ZERO,
        gasPrice);
  }
}
