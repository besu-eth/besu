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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.RequestType;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link BodyValidation}. */
public final class BodyValidationTest {

  @Test
  public void calculateTransactionsRoot() throws IOException {
    for (final int block : Arrays.asList(300006, 4400002)) {
      final BlockHeader header = ValidationTestUtils.readHeader(block);
      final BlockBody body = ValidationTestUtils.readBody(block);
      final Hash transactionRoot = BodyValidation.transactionsRoot(body.getTransactions());
      Assertions.assertThat(transactionRoot).isEqualTo(header.getTransactionsRoot());
    }
  }

  @Test
  public void calculateOmmersHash() throws IOException {
    for (final int block : Arrays.asList(300006, 4400002)) {
      final BlockHeader header = ValidationTestUtils.readHeader(block);
      final BlockBody body = ValidationTestUtils.readBody(block);
      final Hash ommersHash = BodyValidation.ommersHash(body.getOmmers());
      Assertions.assertThat(header.getOmmersHash()).isEqualTo(ommersHash);
    }
  }

  @Test
  public void calculateWithdrawalsRoot() throws IOException {
    for (final int block : Arrays.asList(4156, 12691)) {
      final BlockHeader header = ValidationTestUtils.readHeader(block);
      final BlockBody body = ValidationTestUtils.readBody(block);
      final Hash withdrawalsRoot = BodyValidation.withdrawalsRoot(body.getWithdrawals().get());
      Assertions.assertThat(header.getWithdrawalsRoot()).hasValue(withdrawalsRoot);
    }
  }

  @Test
  public void calculateRequestsHash() {
    List<Request> requests =
        List.of(
            new Request(
                RequestType.DEPOSIT,
                Bytes.fromHexString(
                    "0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100405973070000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000020000000000000000")),
            new Request(
                RequestType.WITHDRAWAL,
                Bytes.fromHexString(
                    "0x6389e7f33ce3b1e94e4325ef02829cd12297ef710000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010000000000000000")),
            new Request(
                RequestType.CONSOLIDATION,
                Bytes.fromHexString(
                    "0x8a0a19589531694250d570040a0c4b74576919b8000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001")));

    Hash requestHash = BodyValidation.requestsHash(requests);
    Assertions.assertThat(requestHash.getBytes())
        .isEqualTo(
            Bytes32.fromHexString(
                "0x0e53a6857da18cf29c6ae28be10a333fc0eaafbd3f425f09e5e81f29e4d3d766"));
  }

  @Test
  public void balHashOfADecodedBlockAccessListIsTheHashOfItsEncoding() {
    final BlockAccessList generated = new BlockDataGenerator().blockAccessList(3);
    final BlockAccessList decoded = BlockAccessList.fromBytes(generated.rawRlp().orElseThrow());
    final BlockAccessList notDecoded = new BlockAccessList(generated.accountChanges());

    Assertions.assertThat(BodyValidation.balHash(decoded))
        .isEqualTo(BodyValidation.balHash(notDecoded));
  }
}
