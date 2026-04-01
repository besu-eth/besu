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
package org.hyperledger.besu.tests.acceptance.plugins;

import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.SELECTED;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.ProcessableBlockHeader;
import org.hyperledger.besu.plugin.data.TransactionProcessingResult;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;
import org.hyperledger.besu.plugin.services.TransactionSelectionService;
import org.hyperledger.besu.plugin.services.txselection.BlockTransactionSelectionService;
import org.hyperledger.besu.plugin.services.txselection.PluginTransactionSelector;
import org.hyperledger.besu.plugin.services.txselection.PluginTransactionSelectorFactory;
import org.hyperledger.besu.plugin.services.txselection.SelectorsStateManager;
import org.hyperledger.besu.plugin.services.txselection.TransactionEvaluationContext;
import org.hyperledger.besu.tests.acceptance.dsl.account.Accounts;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTestTransactionSelectorPlugin implements BesuPlugin {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractTestTransactionSelectorPlugin.class);
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  private static final AtomicLong NONCE = new AtomicLong(0);
  private static final KeyPair SENDER_KEYS =
      SIGNATURE_ALGORITHM.createKeyPair(
          SIGNATURE_ALGORITHM.createPrivateKey(
              Bytes32.fromHexString(Accounts.GENESIS_ACCOUNT_THREE_PRIVATE_KEY)));

  private ServiceManager serviceManager;
  private File callbackDir;
  private final int pluginNum;
  private final int preMultiple;
  private final int postMultiple;

  public AbstractTestTransactionSelectorPlugin(
      final int pluginNum, final int preMultiple, final int postMultiple) {
    this.pluginNum = pluginNum;
    this.preMultiple = preMultiple;
    this.postMultiple = postMultiple;
  }

  @Override
  public void register(final ServiceManager serviceManager) {
    this.serviceManager = serviceManager;
    serviceManager
        .getService(PicoCLIOptions.class)
        .orElseThrow()
        .addPicoCLIOptions("tx-selector" + pluginNum, this);

    callbackDir = new File(System.getProperty("besu.plugins.dir", "plugins"));
  }

  protected abstract boolean isEnabled();

  @Override
  public void beforeExternalServices() {
    if (isEnabled()) {
      serviceManager
          .getService(TransactionSelectionService.class)
          .orElseThrow()
          .registerPluginTransactionSelectorFactory(
              new PluginTransactionSelectorFactory() {
                @Override
                public void selectPendingTransactions(
                    final BlockTransactionSelectionService blockTransactionSelectionService,
                    final ProcessableBlockHeader pendingBlockHeader,
                    final List<? extends org.hyperledger.besu.datatypes.PendingTransaction>
                        candidatePendingTransactions) {
                  writeCandidatePendingTransactions(
                      pendingBlockHeader, candidatePendingTransactions);
                  if (pendingBlockHeader.getNumber() == preMultiple) {
                    throw new RuntimeException(
                        "Unhandled exception: block number is multiple of " + preMultiple);
                  }

                  final var pluginBurnTx =
                      new TransactionTestFixture()
                          .chainId(Optional.of(BigInteger.valueOf(4)))
                          .nonce(NONCE.getAndIncrement())
                          .gasLimit(21_000)
                          .gasPrice(Wei.of(1_000_000_000L))
                          .to(Optional.of(Address.ZERO))
                          .createTransaction(SENDER_KEYS);

                  final var pluginPendingTx = new PendingTransaction.Local(pluginBurnTx);

                  final var result =
                      blockTransactionSelectionService.evaluatePendingTransaction(pluginPendingTx);
                  if (result.selected()) {
                    blockTransactionSelectionService.commit();
                  } else {
                    blockTransactionSelectionService.rollback();
                  }
                }

                @Override
                public PluginTransactionSelector create(
                    final ProcessableBlockHeader pendingBlockHeader,
                    final SelectorsStateManager selectorsStateManager) {
                  return new PluginTransactionSelector() {

                    @Override
                    public TransactionSelectionResult evaluateTransactionPreProcessing(
                        final TransactionEvaluationContext evaluationContext) {
                      final var value =
                          evaluationContext
                              .getPendingTransaction()
                              .getTransaction()
                              .getValue()
                              .getAsBigInteger()
                              .longValueExact();
                      LOG.info("Transaction value is {}", value);
                      if (value % preMultiple == 0) {
                        return TransactionSelectionResult.invalid(
                            "value multiple of " + preMultiple);
                      }
                      return SELECTED;
                    }

                    @Override
                    public TransactionSelectionResult evaluateTransactionPostProcessing(
                        final TransactionEvaluationContext evaluationContext,
                        final TransactionProcessingResult processingResult) {
                      if (evaluationContext
                                  .getPendingTransaction()
                                  .getTransaction()
                                  .getValue()
                                  .getAsBigInteger()
                                  .longValue()
                              % postMultiple
                          == 0) {
                        return TransactionSelectionResult.invalid(
                            "value multiple of " + postMultiple);
                      }
                      return SELECTED;
                    }
                  };
                }
              });
    }
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  private void writeCandidatePendingTransactions(
      final ProcessableBlockHeader pendingBlockHeader,
      final List<? extends org.hyperledger.besu.datatypes.PendingTransaction>
          candidatePendingTransactions) {

    final File callbackFile =
        new File(
            callbackDir,
            "seenCandidatePendingTransactions-%d-%d.txt"
                .formatted(pluginNum, pendingBlockHeader.getNumber()));

    try {
      if (!callbackFile.getParentFile().exists()) {
        callbackFile.getParentFile().mkdirs();
        callbackFile.getParentFile().deleteOnExit();
      }

      final var content =
          candidatePendingTransactions.stream()
              .map(org.hyperledger.besu.datatypes.PendingTransaction::getTransaction)
              .map(
                  tx ->
                      String.join(
                          ",",
                          tx.getSender().getBytes().toHexString(),
                          Long.toString(tx.getNonce())))
              .collect(Collectors.joining(System.lineSeparator()));

      Files.write(callbackFile.toPath(), content.getBytes(StandardCharsets.UTF_8));
    } catch (final IOException ioe) {
      throw new RuntimeException(ioe);
    } finally {
      callbackFile.deleteOnExit();
    }
  }
}
