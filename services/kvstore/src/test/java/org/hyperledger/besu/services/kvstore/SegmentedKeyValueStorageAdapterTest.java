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
package org.hyperledger.besu.services.kvstore;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class SegmentedKeyValueStorageAdapterTest {

  private final RecordingStorage segmented = new RecordingStorage();
  private final SegmentedKeyValueStorageAdapter adapter =
      new SegmentedKeyValueStorageAdapter(Segment.FOO, segmented);

  @Test
  public void writesBatchTransactionsToItsSegment() {
    final KeyValueStorageTransaction transaction = adapter.startBatchTransaction();
    transaction.put(new byte[] {1}, new byte[] {2});
    transaction.commit();

    assertThat(segmented.calls).first().isEqualTo("batch");
    assertThat(segmented.get(Segment.FOO, new byte[] {1}).map(Bytes::wrap)).contains(Bytes.of(2));
  }

  // records the transactions the adapter starts
  private static final class RecordingStorage extends SegmentedInMemoryKeyValueStorage {
    private final List<String> calls = new ArrayList<>();

    @Override
    public SegmentedKeyValueStorageTransaction startBatchTransaction() {
      calls.add("batch");
      return super.startBatchTransaction();
    }
  }

  private enum Segment implements SegmentIdentifier {
    FOO;

    @Override
    public String getName() {
      return name();
    }

    @Override
    public byte[] getId() {
      return name().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean containsStaticData() {
      return false;
    }

    @Override
    public boolean isEligibleToHighSpecFlag() {
      return false;
    }
  }
}
