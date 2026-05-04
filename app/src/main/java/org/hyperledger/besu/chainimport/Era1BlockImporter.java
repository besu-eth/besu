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
package org.hyperledger.besu.chainimport;

import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptDecoder;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult;
import org.hyperledger.besu.ethereum.mainnet.BodyValidationMode;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.util.era1.Era1BlockIndex;
import org.hyperledger.besu.util.era1.Era1ExecutionBlockBody;
import org.hyperledger.besu.util.era1.Era1ExecutionBlockHeader;
import org.hyperledger.besu.util.era1.Era1ExecutionBlockReceipts;
import org.hyperledger.besu.util.era1.Era1Reader;
import org.hyperledger.besu.util.era1.Era1ReaderListener;
import org.hyperledger.besu.util.io.InputStreamFactory;
import org.hyperledger.besu.util.snappy.SnappyFactory;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool for importing era1-encoded block data, headers, and transaction receipts from era1 files.
 */
public class Era1BlockImporter implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Era1BlockImporter.class);

  private static final int ERA1_BLOCK_COUNT_MAX = 8192;
  private static final int IMPORT_COUNT_FOR_LOG_UPDATE = 1000;

  /** Default Constructor. */
  public Era1BlockImporter() {}

  /**
   * Imports the blocks, headers, and transaction receipts from the file found at the supplied path
   *
   * @param controller The BesuController
   * @param path The path
   * @throws IOException IOException
   * @throws ExecutionException ExecutionException
   * @throws InterruptedException InterruptedException
   * @throws TimeoutException TimeoutException
   */
  public void importBlocks(final BesuController controller, final Path path)
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    final ProtocolSchedule protocolSchedule = controller.getProtocolSchedule();
    final BlockHeaderFunctions blockHeaderFunctions =
        ScheduleBasedBlockHeaderFunctions.create(protocolSchedule);
    final ProtocolContext context = controller.getProtocolContext();

    Era1Reader reader = new Era1Reader(new SnappyFactory(), new InputStreamFactory());

    final List<Future<BlockHeader>> headersFutures = new ArrayList<>(ERA1_BLOCK_COUNT_MAX);
    final List<Future<BlockBody>> bodiesFutures = new ArrayList<>(ERA1_BLOCK_COUNT_MAX);
    final List<Future<List<TransactionReceipt>>> receiptsFutures =
        new ArrayList<>(ERA1_BLOCK_COUNT_MAX);
    try (FileInputStream fis = new FileInputStream(path.toFile())) {
      reader.read(
          fis,
          new Era1ReaderListener() {

            @Override
            public void handleExecutionBlockHeader(
                final Era1ExecutionBlockHeader executionBlockHeader) {
              headersFutures.add(
                  CompletableFuture.supplyAsync(
                      () ->
                          BlockHeader.readFrom(
                              new BytesValueRLPInput(
                                  Bytes.wrap(executionBlockHeader.header()), false),
                              blockHeaderFunctions)));
            }

            @Override
            public void handleExecutionBlockBody(final Era1ExecutionBlockBody executionBlockBody) {
              bodiesFutures.add(
                  CompletableFuture.supplyAsync(
                      () ->
                          BlockBody.readWrappedBodyFrom(
                              new BytesValueRLPInput(Bytes.wrap(executionBlockBody.block()), false),
                              blockHeaderFunctions,
                              true)));
            }

            @Override
            public void handleExecutionBlockReceipts(
                final Era1ExecutionBlockReceipts executionBlockReceipts) {
              receiptsFutures.add(
                  CompletableFuture.supplyAsync(
                      () -> {
                        RLPInput input =
                            new BytesValueRLPInput(
                                Bytes.wrap(executionBlockReceipts.receipts()), false);
                        final List<TransactionReceipt> receiptsForBlock = new ArrayList<>();
                        input.readList(
                            (in) ->
                                receiptsForBlock.add(TransactionReceiptDecoder.readFrom(in, false)));
                        return receiptsForBlock;
                      }));
            }

            @Override
            public void handleBlockIndex(final Era1BlockIndex blockIndex) {
              // not really necessary, do nothing
            }
          });
    }

    LOG.info("Read {} blocks, now importing", headersFutures.size());

    Block block = null;
    for (int i = 0; i < headersFutures.size(); i++) {
      BlockHeader blockHeader = headersFutures.get(i).get(10, TimeUnit.SECONDS);
      BlockImporter blockImporter =
          protocolSchedule.getByBlockHeader(blockHeader).getBlockImporter();
      block = new Block(blockHeader, bodiesFutures.get(i).get(10, TimeUnit.SECONDS));

      BlockImportResult importResult =
          blockImporter.importBlockForSyncing(
              context,
              block,
              receiptsFutures.get(i).get(10, TimeUnit.SECONDS),
              HeaderValidationMode.NONE,
              HeaderValidationMode.NONE,
              BodyValidationMode.NONE,
              false);
      if (importResult.getStatus() != BlockImportResult.BlockImportStatus.IMPORTED) {
        LOG.warn(
            "Failed to import block {} due to {}",
            blockHeader.getNumber(),
            importResult.getStatus());
      } else if (i % IMPORT_COUNT_FOR_LOG_UPDATE == 0) {
        LOG.info("{}/{} blocks imported", i, headersFutures.size());
      }
    }
    LOG.info("Done importing {} blocks", headersFutures.size());
  }

  @Override
  public void close() throws IOException {}
}
