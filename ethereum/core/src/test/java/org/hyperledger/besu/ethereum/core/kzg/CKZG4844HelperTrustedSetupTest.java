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
package org.hyperledger.besu.ethereum.core.kzg;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mockStatic;

import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.util.TrustedSetupClassLoaderExtension;
import org.hyperledger.besu.evm.precompile.KZGPointEvalPrecompiledContract;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

/**
 * Every KZG operation waits for the trusted setup first, which matters when it loads on a
 * background thread.
 */
class CKZG4844HelperTrustedSetupTest extends TrustedSetupClassLoaderExtension {

  private static final BlobsWithCommitments BLOBS =
      CKZG4844Helper.convertToVersion1(new BlobTestFixture().createBlobsWithCommitments(1));
  private static final BlobProofBundle BUNDLE = BLOBS.getBlobProofBundles().getFirst();
  private static final Blob BLOB = BUNDLE.getBlob().orElseThrow();

  static Stream<Arguments> kzgOperations() {
    return Stream.of(
        Arguments.of(
            "computeBlobKzgProof",
            (Runnable) () -> CKZG4844Helper.computeBlobKzgProof(BLOB, BUNDLE.getKzgCommitment())),
        Arguments.of(
            "computeBlobKzgProofs", (Runnable) () -> CKZG4844Helper.computeBlobKzgProofs(BLOB)),
        Arguments.of("computeCells", (Runnable) () -> CKZG4844Helper.computeCells(BLOB)),
        Arguments.of("recoverBlobs", (Runnable) () -> CKZG4844Helper.recoverBlobs(BLOBS)),
        Arguments.of("verify4844Kzg", (Runnable) () -> CKZG4844Helper.verify4844Kzg(BLOBS)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("kzgOperations")
  void callsAwaitTrustedSetupBeforeUsingIt(final String name, final Runnable kzgOperation) {
    try (MockedStatic<KZGPointEvalPrecompiledContract> precompile =
        mockStatic(KZGPointEvalPrecompiledContract.class)) {
      precompile
          .when(KZGPointEvalPrecompiledContract::awaitTrustedSetup)
          .thenThrow(new IllegalStateException("KZG trusted setup failed to load"));

      assertThatThrownBy(kzgOperation::run)
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("KZG trusted setup failed to load");
    }
  }
}
