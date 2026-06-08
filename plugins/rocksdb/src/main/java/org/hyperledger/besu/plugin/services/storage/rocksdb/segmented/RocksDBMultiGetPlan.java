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
package org.hyperledger.besu.plugin.services.storage.rocksdb.segmented;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

final class RocksDBMultiGetPlan {

  private final List<byte[]> keys;
  private final int[] resultIndexes;

  private RocksDBMultiGetPlan(final List<byte[]> keys, final int[] resultIndexes) {
    this.keys = keys;
    this.resultIndexes = resultIndexes;
  }

  static RocksDBMultiGetPlan preserveInputOrder(final List<byte[]> keys) {
    final int[] resultIndexes = new int[keys.size()];
    for (int i = 0; i < keys.size(); i++) {
      resultIndexes[i] = i;
    }
    return new RocksDBMultiGetPlan(keys, resultIndexes);
  }

  List<byte[]> keys() {
    return keys;
  }

  List<Optional<byte[]>> restoreInputOrder(final List<byte[]> values) {
    if (values.size() != resultIndexes.length) {
      throw new IllegalStateException(
          "RocksDB multiget returned "
              + values.size()
              + " values for "
              + resultIndexes.length
              + " keys");
    }

    final List<Optional<byte[]>> result =
        new ArrayList<>(Collections.nCopies(resultIndexes.length, Optional.empty()));
    for (int i = 0; i < values.size(); i++) {
      result.set(resultIndexes[i], Optional.ofNullable(values.get(i)));
    }
    return result;
  }
}
