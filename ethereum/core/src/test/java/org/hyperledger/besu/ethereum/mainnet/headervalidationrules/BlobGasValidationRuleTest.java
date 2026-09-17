/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.mainnet.headervalidationrules;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.mainnet.CancunTargetingGasLimitCalculator;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.gascalculator.PragueGasCalculator;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the {@link BlobGasValidationRule} class. */
public class BlobGasValidationRuleTest {
  private static final int MAX_BLOBS_PER_BLOCK = 6;
  private static final int TARGET_BLOBS_PER_BLOCK = 3;
  private CancunGasCalculator cancunGasCalculator;
  private BlobGasValidationRule cancunBlobGasValidationRule;
  private CancunTargetingGasLimitCalculator cancunTargetingGasLimitCalculator;

  private PragueGasCalculator pragueGasCalculator;
  private BlobGasValidationRule pragueBlobGasValidationRule;
  private CancunTargetingGasLimitCalculator pragueGasLimitCalculator;

  @BeforeEach
  public void setUp() {
    cancunGasCalculator = new CancunGasCalculator();
    cancunTargetingGasLimitCalculator =
        new CancunTargetingGasLimitCalculator(
            0L,
            FeeMarket.cancunDefault(0L, Optional.empty()),
            cancunGasCalculator,
            MAX_BLOBS_PER_BLOCK,
            TARGET_BLOBS_PER_BLOCK);
    cancunBlobGasValidationRule =
        new BlobGasValidationRule(cancunGasCalculator, cancunTargetingGasLimitCalculator);

    pragueGasCalculator = new PragueGasCalculator();
    pragueGasLimitCalculator =
        new CancunTargetingGasLimitCalculator(
            0L,
            FeeMarket.cancunDefault(0L, Optional.empty()),
            pragueGasCalculator,
            MAX_BLOBS_PER_BLOCK,
            TARGET_BLOBS_PER_BLOCK);
    pragueBlobGasValidationRule =
        new BlobGasValidationRule(pragueGasCalculator, pragueGasLimitCalculator);
  }

  /**
   * Cancun EIP-4844 - Tests that the header blob gas matches the calculated blob gas and passes
   * validation.
   */
  @Test
  public void validateHeader_BlobGasMatchesCalculated_SuccessValidation() {
    long target = cancunTargetingGasLimitCalculator.getTargetBlobGasPerBlock();

    // Create parent header
    final BlockHeaderTestFixture parentBuilder = new BlockHeaderTestFixture();
    parentBuilder.excessBlobGas(BlobGas.of(1L));
    parentBuilder.blobGasUsed(target);
    final BlockHeader parentHeader = parentBuilder.buildHeader();

    // Create block header with matching excessBlobGas
    final BlockHeaderTestFixture headerBuilder = new BlockHeaderTestFixture();
    headerBuilder.excessBlobGas(BlobGas.of(1L));
    headerBuilder.blobGasUsed(0L);
    final BlockHeader header = headerBuilder.buildHeader();

    assertThat(cancunBlobGasValidationRule.validate(header, parentHeader)).isTrue();
  }

  /**
   * Cancun EIP-4844 - Tests that the header blob gas is different from the calculated blob gas and
   * fails validation.
   */
  @Test
  public void validateHeader_BlobGasDifferentFromCalculated_FailsValidation() {
    long target = cancunTargetingGasLimitCalculator.getTargetBlobGasPerBlock();

    // Create parent header
    final BlockHeaderTestFixture parentBuilder = new BlockHeaderTestFixture();
    parentBuilder.excessBlobGas(BlobGas.of(1L));
    parentBuilder.blobGasUsed(target);
    final BlockHeader parentHeader = parentBuilder.buildHeader();

    // Create block header with different excessBlobGas
    final BlockHeaderTestFixture headerBuilder = new BlockHeaderTestFixture();
    headerBuilder.excessBlobGas(BlobGas.of(0L));
    headerBuilder.blobGasUsed(0L);
    final BlockHeader header = headerBuilder.buildHeader();

    assertThat(cancunBlobGasValidationRule.validate(header, parentHeader)).isFalse();
  }

  /**
   * Prague EIP-7840 - Tests that the header blob gas matches the calculated blob gas and passes
   * validation.
   */
  @Test
  public void validateHeader_BlobGasMatchesCalculated_SuccessValidation_Prague() {
    long target = pragueGasLimitCalculator.getTargetBlobGasPerBlock();

    // Create parent header
    final BlockHeaderTestFixture parentBuilder = new BlockHeaderTestFixture();
    parentBuilder.excessBlobGas(BlobGas.of(1L));
    parentBuilder.blobGasUsed(target);
    final BlockHeader parentHeader = parentBuilder.buildHeader();

    // Create block header with matching excessBlobGas
    final BlockHeaderTestFixture headerBuilder = new BlockHeaderTestFixture();
    headerBuilder.excessBlobGas(BlobGas.of(1L));
    headerBuilder.blobGasUsed(0L);
    final BlockHeader header = headerBuilder.buildHeader();

    assertThat(pragueBlobGasValidationRule.validate(header, parentHeader)).isTrue();
  }

  /**
   * Prague EIP-7840 - Tests that the header blob gas is different from the calculated blob gas and
   * fails validation.
   */
  @Test
  public void validateHeader_BlobGasDifferentFromCalculated_FailsValidation_Prague() {
    long target = pragueGasLimitCalculator.getTargetBlobGasPerBlock();

    // Create parent header
    final BlockHeaderTestFixture parentBuilder = new BlockHeaderTestFixture();
    parentBuilder.excessBlobGas(BlobGas.of(1L));
    parentBuilder.blobGasUsed(target);
    final BlockHeader parentHeader = parentBuilder.buildHeader();

    // Create block header with different excessBlobGas
    final BlockHeaderTestFixture headerBuilder = new BlockHeaderTestFixture();
    headerBuilder.excessBlobGas(BlobGas.of(0L));
    headerBuilder.blobGasUsed(0L);
    final BlockHeader header = headerBuilder.buildHeader();

    assertThat(pragueBlobGasValidationRule.validate(header, parentHeader)).isFalse();
  }

  @Test
  public void validateHeader_BlobGasUsedExceedsLimit_FailsValidation() {
    long blobGasPerBlob = cancunGasCalculator.getBlobGasPerBlob();
    long overLimitBlobGasUsed = blobGasPerBlob * (MAX_BLOBS_PER_BLOCK + 1);

    final BlockHeaderTestFixture parentBuilder = new BlockHeaderTestFixture();
    parentBuilder.excessBlobGas(BlobGas.of(0L));
    parentBuilder.blobGasUsed(0L);
    final BlockHeader parentHeader = parentBuilder.buildHeader();

    final BlockHeaderTestFixture headerBuilder = new BlockHeaderTestFixture();
    headerBuilder.excessBlobGas(BlobGas.of(0L));
    headerBuilder.blobGasUsed(overLimitBlobGasUsed);
    final BlockHeader header = headerBuilder.buildHeader();

    assertThat(cancunBlobGasValidationRule.validate(header, parentHeader)).isFalse();
  }

  @Test
  public void validateHeader_MissingExcessBlobGas_FailsValidation() {
    long target = cancunTargetingGasLimitCalculator.getTargetBlobGasPerBlock();

    final BlockHeaderTestFixture parentBuilder = new BlockHeaderTestFixture();
    parentBuilder.excessBlobGas(BlobGas.of(1L));
    parentBuilder.blobGasUsed(target);
    final BlockHeader parentHeader = parentBuilder.buildHeader();

    final BlockHeaderTestFixture headerBuilder = new BlockHeaderTestFixture();
    headerBuilder.blobGasUsed(0L);
    final BlockHeader headerMissingExcessBlobGas = headerBuilder.buildHeader();

    assertThat(cancunBlobGasValidationRule.validate(headerMissingExcessBlobGas, parentHeader))
        .isFalse();
  }

  @Test
  public void validateHeader_MissingBlobGasUsed_FailsValidation() {
    long target = cancunTargetingGasLimitCalculator.getTargetBlobGasPerBlock();

    final BlockHeaderTestFixture parentBuilder = new BlockHeaderTestFixture();
    parentBuilder.excessBlobGas(BlobGas.of(1L));
    parentBuilder.blobGasUsed(target);
    final BlockHeader parentHeader = parentBuilder.buildHeader();

    final BlockHeaderTestFixture headerBuilder = new BlockHeaderTestFixture();
    headerBuilder.excessBlobGas(BlobGas.of(1L));
    final BlockHeader headerMissingBlobGasUsed = headerBuilder.buildHeader();

    assertThat(cancunBlobGasValidationRule.validate(headerMissingBlobGasUsed, parentHeader))
        .isFalse();
  }
}
