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
package org.hyperledger.besu.ethereum.processing;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class TransactionProcessingResultTest {
  private static final List<Log> LOGS =
      List.of(
          new Log(
              Address.fromHexString("0x1234"),
              Bytes.of(1, 2, 3),
              List.of(LogTopic.of(Bytes32.fromHexStringLenient("0x5678")))),
          new Log(Address.fromHexString("0x9abc"), Bytes.EMPTY, List.of()));

  @Test
  void logsBloomIsTheBloomOfTheLogs() {
    assertThat(successful(LOGS).getLogsBloom())
        .isEqualTo(LogsBloomFilter.builder().insertLogs(LOGS).build());
  }

  @Test
  void logsBloomOfNoLogsIsEmpty() {
    assertThat(successful(List.of()).getLogsBloom()).isEqualTo(LogsBloomFilter.empty());
  }

  @Test
  void logsBloomIsComputedOnce() {
    final TransactionProcessingResult result = successful(LOGS);

    assertThat(result.getLogsBloom()).isSameAs(result.getLogsBloom());
  }

  private static TransactionProcessingResult successful(final List<Log> logs) {
    return TransactionProcessingResult.successful(
        logs, 0, 0, Bytes.EMPTY, Optional.empty(), ValidationResult.valid());
  }
}
