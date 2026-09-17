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
package org.hyperledger.besu.ethereum.chain.pluginadapter;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.plugin.data.BlockHeader;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

public class RlpConverterServiceImplTest {

  @Test
  public void testBuildRlpFromHeader() {
    // Arrange
    RlpConverterServiceImpl rlpConverterServiceImpl =
        new RlpConverterServiceImpl(ProtocolScheduleFixture.TESTING_NETWORK);
    // header with cancun fields
    BlockHeader header =
        new BlockHeaderTestFixture()
            .timestamp(1710338135 + 1)
            .baseFeePerGas(Wei.of(1000))
            .requestsHash(Hash.ZERO)
            .balHash(Hash.ZERO)
            .parentBeaconBlockRoot(Optional.of(Bytes32.ZERO))
            .withdrawalsRoot(Hash.ZERO)
            .blobGasUsed(500L)
            .excessBlobGas(BlobGas.of(500L))
            .buildHeader();

    Bytes rlpBytes = rlpConverterServiceImpl.buildRlpFromHeader(header);
    BlockHeader deserialized = rlpConverterServiceImpl.buildHeaderFromRlp(rlpBytes);
    // Assert
    assertThat(header).isEqualTo(deserialized);
    assertThat(header.getBlobGasUsed()).isEqualTo(deserialized.getBlobGasUsed());
    assertThat(header.getExcessBlobGas()).isEqualTo(deserialized.getExcessBlobGas());
    assertThat(header.getBalHash()).isEqualTo(deserialized.getBalHash());
  }
}
