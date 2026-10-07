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
package org.hyperledger.besu.chainexport;

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.RLP;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Rlp block exporter. */
public class RlpBlockExporter {
  private static final Logger LOG = LoggerFactory.getLogger(RlpBlockExporter.class);
  private final Blockchain blockchain;

  /**
   * Instantiates a new Rlp block exporter.
   *
   * @param blockchain the blockchain
   */
  public RlpBlockExporter(final Blockchain blockchain) {
    this.blockchain = blockchain;
  }

  /**
   * Export blocks that are stored in Besu's block storage.
   *
   * @param outputFile the path at which to save the exported block data
   * @param maybeBalsOutputFile optional path for a sidecar file holding each block's Block Access
   *     List; if empty no sidecar is written
   * @param maybeStartBlock the starting index of the block list to export (inclusive)
   * @param maybeEndBlock the ending index of the block list to export (exclusive), if not specified
   *     a single block will be exported
   * @throws IOException if an I/O error occurs while writing data to disk
   */
  public void exportBlocks(
      final File outputFile,
      final Optional<File> maybeBalsOutputFile,
      final Optional<Long> maybeStartBlock,
      final Optional<Long> maybeEndBlock)
      throws IOException {

    // Get range to export
    final long startBlock = maybeStartBlock.orElse(BlockHeader.GENESIS_BLOCK_NUMBER);
    final long endBlock = maybeEndBlock.orElse(blockchain.getChainHeadBlockNumber() + 1L);
    checkArgument(startBlock >= 0 && endBlock >= 0, "Start and end blocks must be greater than 0.");
    checkArgument(startBlock < endBlock, "Start block must be less than end block");

    // Append to file if a range is specified
    final boolean append = maybeStartBlock.isPresent();

    LOG.info(
        "Exporting blocks [{},{}) to file {} (appending: {}, bals: {})",
        startBlock,
        endBlock,
        outputFile.toString(),
        Boolean.toString(append),
        maybeBalsOutputFile.map(File::toString).orElse("none"));

    try (final FileOutputStream outputStream = new FileOutputStream(outputFile, append);
        final DataOutputStream balsStream = openBalsStream(maybeBalsOutputFile, append)) {
      long blockNumber = 0L;
      for (long i = startBlock; i < endBlock; i++) {
        final Optional<Block> maybeBlock = blockchain.getBlockByNumber(i);
        if (maybeBlock.isEmpty()) {
          LOG.warn("Unable to export blocks [{} - {}).  Blocks not found.", i, endBlock);
          break;
        }

        final Block block = maybeBlock.get();
        blockNumber = block.getHeader().getNumber();
        if (blockNumber % 100 == 0) {
          LOG.info("Export at block {}", blockNumber);
        }

        exportBlock(outputStream, block);
        if (balsStream != null) {
          exportBal(balsStream, block);
        }
      }
      LOG.info("Export complete at block {}", blockNumber);
    }
  }

  private DataOutputStream openBalsStream(
      final Optional<File> maybeBalsOutputFile, final boolean append) throws IOException {
    if (maybeBalsOutputFile.isEmpty()) {
      return null;
    }
    return new DataOutputStream(new FileOutputStream(maybeBalsOutputFile.get(), append));
  }

  private void exportBal(final DataOutputStream balsStream, final Block block) throws IOException {
    final Bytes balRlp =
        blockchain
            .getBlockAccessList(block.getHash())
            .flatMap(BlockAccessList::rawRlp)
            .orElse(Bytes.EMPTY);
    if (balRlp.isEmpty()) {
      LOG.warn(
          "Block {} has no Block Access List — writing empty BAL frame"
              + " (block may pre-date Amsterdam activation or BAL data is missing)",
          block.getHash());
    }
    final byte[] bytes = balRlp.toArrayUnsafe();
    balsStream.writeInt(bytes.length);
    balsStream.write(bytes);
  }

  /**
   * Export block.
   *
   * @param outputStream The FileOutputStream where the block will be exported
   * @param block The block to export
   * @throws IOException In case of an error while exporting.
   */
  protected void exportBlock(final FileOutputStream outputStream, final Block block)
      throws IOException {
    final Bytes rlp = RLP.encode(block::writeTo);
    outputStream.write(rlp.toArrayUnsafe());
  }
}
