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
package org.hyperledger.besu.ethereum.api.jsonrpc.bonsai;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.AbstractJsonRpcHttpBySpecTest;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;

public class TestingBuildBlockJsonRpcHttpBySpecTest extends AbstractJsonRpcHttpBySpecTest {

  private static final BigInteger CHAIN_ID = BigInteger.valueOf(3503995874084926L);
  private static final String PRIVATE_KEY =
      "8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63";
  private static final Address RECIPIENT =
      Address.fromHexString("0x627306090abaB3A6e1400e9345bC60c78a8BEf57");

  @Override
  protected void doSetup() throws Exception {
    blockchainSetupUtil = getBlockchainSetupUtil(DataStorageFormat.BONSAI);
    blockchainSetupUtil.importAllBlocks();
    startService();
  }

  @Override
  protected TransactionPool createTransactionPoolMock() {
    final TransactionPool transactionPoolMock = super.createTransactionPoolMock();

    final Transaction pendingTx = createTestTransaction();
    final PendingTransaction pending =
        PendingTransaction.newPendingTransaction(pendingTx, false, false, (byte) 0);

    doAnswer(
            invocation -> {
              final PendingTransactions.PendingTransactionsSelector selector =
                  invocation.getArgument(0);
              selector.evaluatePendingTransactions(List.of(pending));
              return null;
            })
        .when(transactionPoolMock)
        .selectTransactions(any());

    return transactionPoolMock;
  }

  private Transaction createTestTransaction() {
    final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();
    final SECPPrivateKey privateKey =
        signatureAlgorithm.createPrivateKey(Bytes32.fromHexString(PRIVATE_KEY));
    final KeyPair keyPair = signatureAlgorithm.createKeyPair(privateKey);

    return new TransactionTestFixture()
        .type(TransactionType.EIP1559)
        .chainId(Optional.of(CHAIN_ID))
        .nonce(0)
        .maxFeePerGas(Optional.of(Wei.of(16)))
        .maxPriorityFeePerGas(Optional.of(Wei.ZERO))
        .gasLimit(21000)
        .to(Optional.of(RECIPIENT))
        .value(Wei.of(1000))
        .createTransaction(keyPair);
  }

  @Override
  protected BlockchainSetupUtil getBlockchainSetupUtil(final DataStorageFormat storageFormat) {
    return createBlockchainSetupUtil(
        "testing_buildBlockV1/chain-data/genesis.json",
        "testing_buildBlockV1/chain-data/blocks.bin",
        storageFormat);
  }

  public static Object[][] specs() {
    return findSpecFiles(new String[] {"testing_buildBlockV1"});
  }
}
