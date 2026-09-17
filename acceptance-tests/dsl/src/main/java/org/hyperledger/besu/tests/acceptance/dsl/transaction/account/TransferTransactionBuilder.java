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

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.crypto.SECP256K1;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.blockchain.Amount;

import java.math.BigInteger;
import java.util.Optional;

public class TransferTransactionBuilder {

  private Account sender;
  private Account recipient;
  private Amount transferAmount;
  private Amount gasPrice;
  private BigInteger nonce;
  private Optional<BigInteger> chainId = Optional.empty();
  private Optional<BigInteger> gasLimit = Optional.empty();
  private SignatureAlgorithm signatureAlgorithm = new SECP256K1();

  private TransactionType transactionType;

  public TransferTransactionBuilder sender(final Account sender) {
    this.sender = sender;
    validateSender();
    return this;
  }

  public TransferTransactionBuilder recipient(final Account recipient) {
    this.recipient = recipient;
    return this;
  }

  public TransferTransactionBuilder amount(final Amount transferAmount) {
    this.transferAmount = transferAmount;
    validateTransferAmount();
    return this;
  }

  public TransferTransactionBuilder nonce(final BigInteger nonce) {
    this.nonce = nonce;
    return this;
  }

  public TransferTransactionBuilder gasPrice(final Amount gasPrice) {
    this.gasPrice = gasPrice;
    return this;
  }

  public TransferTransactionBuilder setSignatureAlgorithm(
      final SignatureAlgorithm signatureAlgorithm) {
    checkNotNull(signatureAlgorithm);
    this.signatureAlgorithm = signatureAlgorithm;
    return this;
  }

  public TransferTransactionBuilder chainId(final BigInteger chainId) {
    this.chainId = Optional.ofNullable(chainId);
    return this;
  }

  public TransferTransactionBuilder chainId(final Long chainId) {
    checkNotNull(chainId);
    return chainId(BigInteger.valueOf(chainId));
  }

  public TransferTransactionBuilder gasLimit(final long gasLimit) {
    this.gasLimit = Optional.of(BigInteger.valueOf(gasLimit));
    return this;
  }

  public TransferTransactionBuilder transactionType(final TransactionType transactionType) {
    this.transactionType = transactionType;
    return this;
  }

  public TransferTransaction build() {
    validateSender();
    validateTransferAmount();
    validateTransactionType();
    return new TransferTransaction(
        sender,
        recipient,
        transferAmount,
        gasPrice,
        nonce,
        chainId,
        signatureAlgorithm,
        transactionType,
        gasLimit);
  }

  private void validateSender() {
    if (sender == null) {
      throw new IllegalArgumentException("NULL sender is not allowed.");
    }
  }

  private void validateTransferAmount() {
    if (transferAmount == null) {
      throw new IllegalArgumentException("NULL transferAmount is not allowed.");
    }
  }

  private void validateTransactionType() {
    if (transactionType == null) {
      throw new IllegalArgumentException("NULL transactionType is not allowed.");
    }
    if (transactionType == TransactionType.EIP1559 && chainId.isEmpty()) {
      throw new IllegalStateException("Chain ID must be specified for EIP-1559 transactions");
    }
  }
}
