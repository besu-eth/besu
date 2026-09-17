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
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.account.Accounts;
import org.hyperledger.besu.tests.acceptance.dsl.blockchain.Amount;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class AccountTransactions {

  private static final Amount DEFAULT_GAS_PRICE = Amount.wei(BigInteger.valueOf(10000000000L));
  private final Accounts accounts;

  public AccountTransactions(final Accounts accounts) {
    this.accounts = accounts;
  }

  public TransferTransaction createTransfer(final Account recipient, final int amount) {
    return createTransfer(accounts.getPrimaryBenefactor(), recipient, amount);
  }

  public TransferTransaction createTransfer(
      final Account recipient, final int amount, final Amount gasPrice) {
    return createFrontierBuilder(accounts.getPrimaryBenefactor(), recipient, Amount.ether(amount))
        .gasPrice(gasPrice)
        .build();
  }

  public TransferTransaction createTransfer(
      final Account sender, final Account recipient, final Amount amount, final Amount gasPrice) {
    return createFrontierBuilder(sender, recipient, amount).gasPrice(gasPrice).build();
  }

  public TransferTransaction createTransfer(
      final Account recipient, final int amount, final SignatureAlgorithm signatureAlgorithm) {
    return createFrontierBuilder(accounts.getPrimaryBenefactor(), recipient, Amount.ether(amount))
        .setSignatureAlgorithm(signatureAlgorithm)
        .build();
  }

  public TransferTransaction createTransfer(final Account recipient, final Amount amount) {
    return createTransfer(accounts.getPrimaryBenefactor(), recipient, amount);
  }

  public TransferTransaction createTransfer(
      final Account sender, final Account recipient, final int amount) {
    return createFrontierBuilder(sender, recipient, Amount.ether(amount)).build();
  }

  public TransferTransaction createTransfer(
      final Account sender, final Account recipient, final Amount amount) {
    return createFrontierBuilder(sender, recipient, amount).build();
  }

  public TransferTransaction createTransfer(
      final Account sender, final Account recipient, final int amount, final BigInteger nonce) {
    return createFrontierBuilder(sender, recipient, Amount.ether(amount)).nonce(nonce).build();
  }

  public TransferTransaction createTransferWithGasLimit(
      final Account sender, final int amount, final long gasLimit) {
    return createFrontierBuilder(accounts.getPrimaryBenefactor(), sender, Amount.ether(amount))
        .gasLimit(gasLimit)
        .build();
  }

  public TransferTransaction createTransferWithGasLimit(
      final Account sender, final Account recipient, final int amount, final long gasLimit) {
    return createFrontierBuilder(sender, recipient, Amount.ether(amount))
        .gasLimit(gasLimit)
        .build();
  }

  public TransferTransaction createTransfer(
      final Account sender, final Account recipient, final Amount amount, final BigInteger nonce) {
    return createFrontierBuilder(sender, recipient, amount).nonce(nonce).build();
  }

  public TransferTransaction create1559Transfer(
      final Account recipient, final int amount, final long chainId) {
    return create1559Builder(
            accounts.getPrimaryBenefactor(), recipient, Amount.ether(amount), chainId)
        .build();
  }

  public TransferTransaction create1559Transfer(
      final Account recipient, final int amount, final long chainId, final Amount gasPrice) {
    return create1559Builder(
            accounts.getPrimaryBenefactor(), recipient, Amount.ether(amount), chainId)
        .gasPrice(gasPrice)
        .build();
  }

  public TransferTransactionSet createIncrementalTransfers(
      final Account sender, final Account recipient, final int etherAmount) {
    return createIncrementalTransfers(sender, recipient, etherAmount, DEFAULT_GAS_PRICE);
  }

  public TransferTransactionSet createIncrementalTransfers(
      final Account sender, final Account recipient, final int etherAmount, final Amount gasPrice) {
    final List<TransferTransaction> transfers = new ArrayList<>();
    final TransferTransactionBuilder transferOneEther =
        createFrontierBuilder(sender, recipient, Amount.ether(1)).gasPrice(gasPrice);

    for (int i = 1; i <= etherAmount; i++) {
      transfers.add(transferOneEther.build());
    }

    return new TransferTransactionSet(transfers);
  }

  public TransferTransactionSet create1559IncrementalTransfers(
      final Account sender, final Account recipient, final int etherAmount, final long chainId) {
    return create1559IncrementalTransfers(
        sender, recipient, etherAmount, chainId, DEFAULT_GAS_PRICE);
  }

  public TransferTransactionSet create1559IncrementalTransfers(
      final Account sender,
      final Account recipient,
      final int etherAmount,
      final long chainId,
      final Amount gasPrice) {
    final List<TransferTransaction> transfers = new ArrayList<>();
    final TransferTransactionBuilder transferOneEther =
        create1559Builder(sender, recipient, Amount.ether(1), chainId).gasPrice(gasPrice);

    for (int i = 1; i <= etherAmount; i++) {
      transfers.add(transferOneEther.build());
    }

    return new TransferTransactionSet(transfers);
  }

  private TransferTransactionBuilder createFrontierBuilder(
      final Account sender, final Account recipient, final Amount amount) {
    return new TransferTransactionBuilder()
        .sender(sender)
        .recipient(recipient)
        .amount(amount)
        .gasPrice(DEFAULT_GAS_PRICE)
        .transactionType(TransactionType.FRONTIER);
  }

  private TransferTransactionBuilder create1559Builder(
      final Account sender, final Account recipient, final Amount amount, final long chainId) {
    return new TransferTransactionBuilder()
        .sender(sender)
        .recipient(recipient)
        .amount(amount)
        .gasPrice(DEFAULT_GAS_PRICE)
        .chainId(chainId)
        .transactionType(TransactionType.EIP1559);
  }
}
