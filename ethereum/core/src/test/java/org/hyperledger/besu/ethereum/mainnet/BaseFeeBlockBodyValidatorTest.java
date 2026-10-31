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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BaseFeeBlockBodyValidatorTest {

  private static final KeyPair keyPair = SignatureAlgorithmFactory.getInstance().generateKeyPair();

  @Mock ProtocolSpec protocolSpec;
  @Mock ProtocolSchedule protocolSchedule;
  @Mock Block block;
  @Mock BlockHeader blockHeader;

  BaseFeeBlockBodyValidator blockBodyValidator;

  @BeforeEach
  public void setup() {
    when(protocolSpec.getFeeMarket()).thenReturn(FeeMarket.london(0L));

    when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);

    when(block.getHeader()).thenReturn(blockHeader);

    blockBodyValidator = new BaseFeeBlockBodyValidator(protocolSchedule);
  }

  @Test
  public void BlockBodyValidatorSucceed() {
    when(blockHeader.getBaseFee()).thenReturn(Optional.of(Wei.of(10L)));
    when(block.getBody())
        .thenReturn(
            new BlockBody(
                List.of(
                    // eip1559 transaction
                    new TransactionTestFixture()
                        .maxFeePerGas(Optional.of(Wei.of(10L)))
                        .maxPriorityFeePerGas(Optional.of(Wei.of(1L)))
                        .type(TransactionType.EIP1559)
                        .createTransaction(keyPair),
                    // frontier transaction
                    new TransactionTestFixture().gasPrice(Wei.of(10L)).createTransaction(keyPair)),
                Collections.emptyList()));

    assertThat(blockBodyValidator.validateTransactionGasPrice(block)).isTrue();
  }

  @Test
  public void BlockBodyValidatorFail_GasPrice() {
    when(blockHeader.getBaseFee()).thenReturn(Optional.of(Wei.of(10L)));
    when(block.getBody())
        .thenReturn(
            new BlockBody(
                List.of(
                    // underpriced frontier transaction
                    new TransactionTestFixture().gasPrice(Wei.of(9L)).createTransaction(keyPair)),
                Collections.emptyList()));

    assertThat(blockBodyValidator.validateTransactionGasPrice(block)).isFalse();
  }

  @Test
  public void BlockBodyValidatorFail_MaxFeePerGas() {
    when(blockHeader.getBaseFee()).thenReturn(Optional.of(Wei.of(10L)));
    when(block.getBody())
        .thenReturn(
            new BlockBody(
                List.of(
                    // underpriced eip1559 transaction
                    new TransactionTestFixture()
                        .maxFeePerGas(Optional.of(Wei.of(1L)))
                        .maxPriorityFeePerGas(Optional.of(Wei.of(10L)))
                        .type(TransactionType.EIP1559)
                        .createTransaction(keyPair)),
                Collections.emptyList()));

    assertThat(blockBodyValidator.validateTransactionGasPrice(block)).isFalse();
  }

  @Test
  public void BlockBodyValidatorFail_OneUnderpricedTransactionInALargeBlock() {
    when(blockHeader.getBaseFee()).thenReturn(Optional.of(Wei.of(10L)));
    final Transaction priced =
        new TransactionTestFixture().gasPrice(Wei.of(10L)).createTransaction(keyPair);
    final List<Transaction> transactions = new ArrayList<>(Collections.nCopies(1000, priced));
    // checked in parallel: the underpriced transaction is one of many
    transactions.set(
        700, new TransactionTestFixture().gasPrice(Wei.of(9L)).createTransaction(keyPair));
    when(block.getBody()).thenReturn(new BlockBody(transactions, Collections.emptyList()));

    assertThat(blockBodyValidator.validateTransactionGasPrice(block)).isFalse();

    transactions.set(700, priced);
    assertThat(blockBodyValidator.validateTransactionGasPrice(block)).isTrue();
  }
}
