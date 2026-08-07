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
package org.hyperledger.besu.ethereum.storage.keyvalue;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class KeyValueSegmentIdentifierTest {

  @Test
  void trieNodeHistoryArchiveIsScopedToXBonsaiArchiveFormatOnly() {
    final KeyValueSegmentIdentifier segment = KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
    assertThat(segment.includeInDatabaseFormat(DataStorageFormat.X_BONSAI_ARCHIVE)).isTrue();
    assertThat(segment.includeInDatabaseFormat(DataStorageFormat.BONSAI)).isFalse();
    assertThat(segment.includeInDatabaseFormat(DataStorageFormat.FOREST)).isFalse();
  }

  @Test
  void trieNodeHistoryArchiveUsesItsNameAsItsSegmentId() {
    // Matches the string-id convention of the other archive segments (ACCOUNT_INFO_STATE_ARCHIVE,
    // ACCOUNT_STORAGE_ARCHIVE) rather than the single-byte ids used by the older segments.
    final KeyValueSegmentIdentifier segment = KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
    assertThat(segment.getId())
        .isEqualTo("TRIE_BRANCH_STORAGE_ARCHIVE".getBytes(StandardCharsets.UTF_8));
    assertThat(segment.getName()).isEqualTo("TRIE_BRANCH_STORAGE_ARCHIVE");
  }
}
