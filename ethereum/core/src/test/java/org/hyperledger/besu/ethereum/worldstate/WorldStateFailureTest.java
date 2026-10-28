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
package org.hyperledger.besu.ethereum.worldstate;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.MissingCodeException;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;

class WorldStateFailureTest {
  @Test
  void missingCodeRemainsRepairableDespiteExtendingStorageException() {
    assertThat(WorldStateFailure.isMissingState(new MissingCodeException(Address.ZERO, Hash.ZERO)))
        .isTrue();
    assertThat(
            WorldStateFailure.isMissingState(
                new CompletionException(new MerkleTrieException("missing"))))
        .isTrue();
  }

  @Test
  void ioAndCancellationNeverBecomeRepairableThroughWrapping() {
    for (final Throwable failure :
        new Throwable[] {
          new StorageException("io"),
          new IOException("io"),
          new CancellationException(),
          new InterruptedException()
        }) {
      final var missing = new MerkleTrieException("missing");
      missing.initCause(failure);
      assertThat(WorldStateFailure.isMissingState(new CompletionException(missing))).isFalse();
      assertThat(WorldStateFailure.isMissingState(new CompletionException(failure))).isFalse();
    }
    assertThat(WorldStateFailure.isMissingState(new IllegalArgumentException("invalid block")))
        .isFalse();
  }
}
