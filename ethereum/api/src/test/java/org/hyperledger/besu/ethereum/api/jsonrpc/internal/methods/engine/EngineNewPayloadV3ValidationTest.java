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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PRAGUE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;
import java.util.Optional;

import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EngineNewPayloadV3ValidationTest {

  @Mock private BlockHeader mockHeader;
  @Mock private BlockHeader mockParentHeader;
  @Mock private ProtocolSpec mockProtocolSpec;
  @Mock private GasCalculator mockGasCalculator;

  private EngineNewPayloadV3 method;

  @BeforeEach
  void setUp() {
    final Vertx vertx = Vertx.vertx();
    final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
    final ProtocolContext protocolContext = mock(ProtocolContext.class);
    final MergeMiningCoordinator mergeCoordinator = mock(MergeMiningCoordinator.class);
    final EthPeers ethPeers = mock(EthPeers.class);
    final EngineCallListener engineCallListener = mock(EngineCallListener.class);
    method =
        new EngineNewPayloadV3(
            vertx,
            protocolSchedule,
            protocolContext,
            mergeCoordinator,
            ethPeers,
            engineCallListener,
            new NoOpMetricsSystem(),
            CANCUN,
            PRAGUE);
  }

  @Test
  void validateExcessBlobGas_valid_returnsEmpty() {
    final BlobGas expected = BlobGas.of(1000);
    when(mockHeader.getExcessBlobGas()).thenReturn(Optional.of(BlobGas.of(1000)));

    try (MockedStatic<ExcessBlobGasCalculator> mocked = mockStatic(ExcessBlobGasCalculator.class)) {
      mocked
          .when(
              () ->
                  ExcessBlobGasCalculator.calculateExcessBlobGasForParent(
                      mockProtocolSpec, mockParentHeader))
          .thenReturn(expected);

      Optional<BlobGas> result =
          invokeValidateExcessBlobGas(mockHeader, mockParentHeader, mockProtocolSpec);
      assertThat(result).isEmpty();
    }
  }

  @Test
  void validateExcessBlobGas_invalid_returnsCalculated() {
    final BlobGas calculated = BlobGas.of(1000);
    when(mockHeader.getExcessBlobGas()).thenReturn(Optional.of(BlobGas.of(800)));

    try (MockedStatic<ExcessBlobGasCalculator> mocked = mockStatic(ExcessBlobGasCalculator.class)) {
      mocked
          .when(
              () ->
                  ExcessBlobGasCalculator.calculateExcessBlobGasForParent(
                      mockProtocolSpec, mockParentHeader))
          .thenReturn(calculated);

      Optional<BlobGas> result =
          invokeValidateExcessBlobGas(mockHeader, mockParentHeader, mockProtocolSpec);
      assertThat(result).contains(calculated);
    }
  }

  @Test
  void validateBlobGasUsed_validSingleBlob_returnsEmpty() {
    when(mockProtocolSpec.getGasCalculator()).thenReturn(mockGasCalculator);
    when(mockGasCalculator.blobGasCost(1)).thenReturn(131072L);
    when(mockHeader.getBlobGasUsed()).thenReturn(Optional.of(131072L));

    List<VersionedHash> versionedHashes = List.of(createValidVersionedHash());

    Optional<Long> result =
        invokeValidateBlobGasUsed(mockHeader, versionedHashes, mockProtocolSpec);
    assertThat(result).isEmpty();
  }

  @Test
  void validateBlobGasUsed_invalid_returnsCalculated() {
    when(mockProtocolSpec.getGasCalculator()).thenReturn(mockGasCalculator);
    when(mockGasCalculator.blobGasCost(1)).thenReturn(131072L);
    when(mockHeader.getBlobGasUsed()).thenReturn(Optional.of(100000L));

    List<VersionedHash> versionedHashes = List.of(createValidVersionedHash());

    Optional<Long> result =
        invokeValidateBlobGasUsed(mockHeader, versionedHashes, mockProtocolSpec);
    assertThat(result).contains(131072L);
  }

  @Test
  void validateBlobGasUsed_invalidMultipleBlobs_returnsCalculated() {
    when(mockProtocolSpec.getGasCalculator()).thenReturn(mockGasCalculator);
    when(mockGasCalculator.blobGasCost(3)).thenReturn(131072L * 3);
    when(mockHeader.getBlobGasUsed()).thenReturn(Optional.of(200000L));

    List<VersionedHash> versionedHashes =
        List.of(createValidVersionedHash(), createValidVersionedHash(), createValidVersionedHash());

    Optional<Long> result =
        invokeValidateBlobGasUsed(mockHeader, versionedHashes, mockProtocolSpec);
    assertThat(result).contains(131072L * 3L);
  }

  @Test
  void validateBlobs_reportsEnhancedMessage_forExcessBlobGasMismatch() {
    when(mockHeader.getExcessBlobGas()).thenReturn(Optional.of(BlobGas.of(800)));

    var mockGasLimitCalculator = mock(org.hyperledger.besu.ethereum.GasLimitCalculator.class);
    when(mockProtocolSpec.getGasLimitCalculator()).thenReturn(mockGasLimitCalculator);
    when(mockGasLimitCalculator.transactionBlobGasLimitCap()).thenReturn(1000000L);
    when(mockGasLimitCalculator.currentBlobGasLimit()).thenReturn(1000000L);

    try (MockedStatic<ExcessBlobGasCalculator> mocked = mockStatic(ExcessBlobGasCalculator.class)) {
      mocked
          .when(
              () ->
                  ExcessBlobGasCalculator.calculateExcessBlobGasForParent(
                      mockProtocolSpec, mockParentHeader))
          .thenReturn(BlobGas.of(1000));

      var result =
          method.validateBlobs(
              List.of(),
              mockHeader,
              Optional.of(mockParentHeader),
              Optional.empty(),
              mockProtocolSpec);

      assertThat(result.isValid()).isFalse();
      assertThat(result.getErrorMessage()).contains("Expected", "1000");
      assertThat(result.getErrorMessage()).contains("got", "800");
    }
  }

  @Test
  void validateBlobs_reportsEnhancedMessage_forBlobGasUsedMismatch() {
    when(mockProtocolSpec.getGasCalculator()).thenReturn(mockGasCalculator);
    when(mockGasCalculator.blobGasCost(2L)).thenReturn(262144L);
    when(mockGasCalculator.blobGasCost(1L)).thenReturn(131072L);

    var mockGasLimitCalculator = mock(org.hyperledger.besu.ethereum.GasLimitCalculator.class);
    when(mockProtocolSpec.getGasLimitCalculator()).thenReturn(mockGasLimitCalculator);
    when(mockGasLimitCalculator.transactionBlobGasLimitCap()).thenReturn(1000000L);
    when(mockGasLimitCalculator.currentBlobGasLimit()).thenReturn(1000000L);

    when(mockHeader.getBlobGasUsed()).thenReturn(Optional.of(100000L));

    List<VersionedHash> versionedHashes =
        List.of(createValidVersionedHash(1), createValidVersionedHash(2));

    Transaction blobTx1 = mock(Transaction.class);
    Transaction blobTx2 = mock(Transaction.class);
    when(blobTx1.getVersionedHashes()).thenReturn(Optional.of(List.of(versionedHashes.get(0))));
    when(blobTx2.getVersionedHashes()).thenReturn(Optional.of(List.of(versionedHashes.get(1))));
    List<Transaction> blobTransactions = List.of(blobTx1, blobTx2);

    var result =
        method.validateBlobs(
            blobTransactions,
            mockHeader,
            Optional.of(mockParentHeader),
            Optional.of(versionedHashes),
            mockProtocolSpec);

    assertThat(result.isValid()).isFalse();
    assertThat(result.getErrorMessage()).contains("Expected", "262144");
    assertThat(result.getErrorMessage()).contains("got", "100000");
  }

  @Test
  void validateBlobs_invalidWhenVersionedHashesMismatch() {
    when(mockProtocolSpec.getGasCalculator()).thenReturn(mockGasCalculator);
    when(mockGasCalculator.blobGasCost(1L)).thenReturn(131072L);

    var mockGasLimitCalculator = mock(org.hyperledger.besu.ethereum.GasLimitCalculator.class);
    when(mockProtocolSpec.getGasLimitCalculator()).thenReturn(mockGasLimitCalculator);
    when(mockGasLimitCalculator.transactionBlobGasLimitCap()).thenReturn(1000000L);

    Transaction blobTx = mock(Transaction.class);
    List<VersionedHash> txHashes = List.of(createValidVersionedHash(1));
    when(blobTx.getVersionedHashes()).thenReturn(Optional.of(txHashes));

    List<Transaction> blobTransactions = List.of(blobTx);
    List<VersionedHash> provided = List.of(createValidVersionedHash(2));

    var result =
        method.validateBlobs(
            blobTransactions,
            mockHeader,
            Optional.of(mockParentHeader),
            Optional.of(provided),
            mockProtocolSpec);

    assertThat(result.isValid()).isFalse();
    assertThat(result.getErrorMessage()).isNotNull();
  }

  private Optional<BlobGas> invokeValidateExcessBlobGas(
      final BlockHeader header, final BlockHeader parent, final ProtocolSpec spec) {
    return method.validateExcessBlobGas(header, parent, spec);
  }

  private Optional<Long> invokeValidateBlobGasUsed(
      final BlockHeader header, final List<VersionedHash> hashes, final ProtocolSpec spec) {
    return method.validateBlobGasUsed(header, hashes, spec);
  }

  private VersionedHash createValidVersionedHash() {
    byte[] validHash = new byte[32];
    validHash[0] = 0x01;
    for (int i = 1; i < 32; i++) {
      validHash[i] = (byte) (i % 256);
    }
    return new VersionedHash(Bytes32.wrap(validHash));
  }

  private VersionedHash createValidVersionedHash(final int seed) {
    byte[] validHash = new byte[32];
    validHash[0] = 0x01;
    for (int i = 1; i < 32; i++) {
      validHash[i] = (byte) ((i + seed) % 256);
    }
    return new VersionedHash(Bytes32.wrap(validHash));
  }
}
