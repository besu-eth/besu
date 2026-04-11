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
package org.hyperledger.besu.chainexport;

import org.hyperledger.besu.config.NetworkDefinition;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.BlockBodyEncoder;
import org.hyperledger.besu.ethereum.core.encoding.BlockHeaderEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.util.era1.Era1Type;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class Era1BlockExporterTest {

  private @Mock Blockchain blockchain;
  private @Mock Era1FileWriterFactory era1FileWriterFactory;
  private @Mock Era1FileWriter era1FileWriter;
  private @Mock Era1AccumulatorFactory era1AccumulatorFactory;
  private @Mock Era1Accumulator era1Accumulator;
  private @Mock Era1BlockIndexConverter era1BlockIndexConverter;
  private @Mock BlockHeaderEncoder blockHeaderEncoder;
  private @Mock BlockBodyEncoder blockBodyEncoder;
  private @Mock TransactionReceiptEncoder transactionReceiptEncoder;

  private Era1BlockExporter era1BlockExporter;

  @BeforeEach
  public void beforeTest() {
    era1BlockExporter =
        new Era1BlockExporter(
            blockchain,
            NetworkDefinition.MAINNET,
            era1FileWriterFactory,
            era1AccumulatorFactory,
            era1BlockIndexConverter,
            blockHeaderEncoder,
            blockBodyEncoder,
            transactionReceiptEncoder);
  }

  @Test
  public void testExport() throws IOException {
    Mockito.when(era1AccumulatorFactory.getEra1Accumulator()).thenReturn(era1Accumulator);
    List<Runnable> blockVerifications = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final int blockNumber = i;
      Block block = Mockito.mock(Block.class);
      Hash blockHash = Hash.wrap(Bytes32.random());
      Mockito.when(blockchain.getBlockByNumber(blockNumber)).thenReturn(Optional.of(block));

      BlockHeader blockHeader = Mockito.mock(BlockHeader.class);
      Mockito.when(block.getHeader()).thenReturn(blockHeader);

      BlockBody blockBody = Mockito.mock(BlockBody.class);
      Mockito.when(block.getBody()).thenReturn(blockBody);

      TransactionReceipt transactionReceipt = Mockito.mock(TransactionReceipt.class);
      Mockito.when(block.getHash()).thenReturn(blockHash);
      Mockito.when(blockchain.getTxReceipts(blockHash))
          .thenReturn(Optional.of(List.of(transactionReceipt)));

      Difficulty difficulty = Difficulty.of(UInt256.fromBytes(Bytes32.random()));
      Mockito.when(blockchain.getTotalDifficultyByHash(blockHash))
          .thenReturn(Optional.of(difficulty));

      final Bytes encodedValue = Bytes.ofUnsignedLong(blockNumber);
      Mockito.when(blockHeaderEncoder.encode(blockHeader)).thenReturn(encodedValue);
      Mockito.when(blockBodyEncoder.encode(blockBody)).thenReturn(encodedValue);
      Mockito.when(
              transactionReceiptEncoder.encode(
                  List.of(transactionReceipt), TransactionReceiptEncodingConfiguration.DEFAULT))
          .thenReturn(encodedValue);

      blockVerifications.add(
          () -> {
            Mockito.verify(blockchain).getBlockByNumber(blockNumber);
            Mockito.verify(block).getHeader();
            Mockito.verify(block).getBody();
            Mockito.verify(block, Mockito.times(3)).getHash();
            Mockito.verify(blockchain).getTxReceipts(blockHash);
            Mockito.verify(blockchain).getTotalDifficultyByHash(blockHash);
            Mockito.verify(era1Accumulator)
                .addBlock(Bytes32.wrap(blockHash.getBytes()), difficulty.toUInt256());

            try {
              Mockito.verify(blockHeaderEncoder).encode(blockHeader);
              Mockito.verify(era1FileWriter)
                  .writeSection(Era1Type.COMPRESSED_EXECUTION_BLOCK_HEADER, encodedValue.toArray());

              Mockito.verify(blockBodyEncoder).encode(blockBody);
              Mockito.verify(era1FileWriter)
                  .writeSection(Era1Type.COMPRESSED_EXECUTION_BLOCK_BODY, encodedValue.toArray());

              Mockito.verify(transactionReceiptEncoder)
                  .encode(
                      List.of(transactionReceipt), TransactionReceiptEncodingConfiguration.DEFAULT);
              Mockito.verify(era1FileWriter)
                  .writeSection(
                      Era1Type.COMPRESSED_EXECUTION_BLOCK_RECEIPTS, encodedValue.toArray());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
    }

    Hash accumulatorHash = Hash.wrap(Bytes32.random());
    Mockito.when(era1Accumulator.accumulate()).thenReturn(Bytes32.wrap(accumulatorHash.getBytes()));

    String expectedFilename =
        "mainnet-00000-" + accumulatorHash.getBytes().toFastHex(false).substring(0, 8) + ".era1";
    Mockito.when(era1FileWriterFactory.era1FileWriter(Mockito.any(File.class)))
        .thenReturn(era1FileWriter);

    AtomicLong position = new AtomicLong(1);
    Mockito.when(era1FileWriter.getPosition())
        .thenAnswer((invocationOnMock) -> position.getAndIncrement());

    byte[] blockIndex = new byte[] {1, 2, 3, 4};
    Mockito.when(era1BlockIndexConverter.convert(Mockito.any(), Mockito.any(), Mockito.anyLong()))
        .thenReturn(blockIndex);

    File tempFile = File.createTempFile("era1", "test");
    era1BlockExporter.export(0, 0, tempFile);

    Mockito.verify(era1Accumulator).accumulate();
    ArgumentCaptor<File> fileArgumentCaptor = ArgumentCaptor.forClass(File.class);
    Mockito.verify(era1FileWriterFactory).era1FileWriter(fileArgumentCaptor.capture());
    Mockito.verify(era1FileWriter).writeSection(Era1Type.VERSION, new byte[] {});
    blockVerifications.forEach(Runnable::run);
    Mockito.verify(era1FileWriter)
        .writeSection(Era1Type.ACCUMULATOR, accumulatorHash.getBytes().toArray());
    Mockito.verify(era1FileWriter).writeSection(Era1Type.BLOCK_INDEX, blockIndex);

    File file = fileArgumentCaptor.getValue();
    Assertions.assertEquals(expectedFilename, file.getName());
  }
}
