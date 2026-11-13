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
package org.hyperledger.besu.ethereum.eth.sync.fullsync.ereprepipeline;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.util.ere.EreReader;
import org.hyperledger.besu.util.snappy.SnappyFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stages remote archives and decodes blocks individually for ordered full validation and import.
 */
public class EreFileReader implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(EreFileReader.class);
  private static final int MAX_COMPONENT_SIZE = 64 * 1024 * 1024;
  private static final long MAX_DOWNLOAD_SIZE = 16L * 1024 * 1024 * 1024;
  private final BlockHeaderFunctions headerFunctions;
  private final @Nullable Path temporaryDirectory;
  private final Set<Path> temporaryFiles = new HashSet<>();
  private volatile boolean closed;

  public EreFileReader(final BlockHeaderFunctions headerFunctions) {
    this(headerFunctions, null);
  }

  EreFileReader(
      final BlockHeaderFunctions headerFunctions, final @Nullable Path temporaryDirectory) {
    this.headerFunctions = headerFunctions;
    this.temporaryDirectory = temporaryDirectory;
  }

  public Archive prepare(final URI uri) throws IOException {
    EreFileSource.metadata(uri);
    synchronized (this) {
      if (closed) {
        throw new IOException("Ere reader is closed");
      }
    }
    if ("file".equals(uri.getScheme())) {
      return new Archive(uri, Path.of(uri), false);
    }
    if (!"http".equals(uri.getScheme()) && !"https".equals(uri.getScheme())) {
      throw new IOException("Unsupported Ere URI scheme");
    }
    final Path temporary =
        temporaryDirectory == null
            ? Files.createTempFile("besu-ere-", ".tmp")
            : Files.createTempFile(temporaryDirectory, "besu-ere-", ".tmp");
    try {
      download(uri, temporary);
      synchronized (this) {
        if (closed) {
          throw new IOException("Ere reader closed while downloading");
        }
        temporaryFiles.add(temporary);
      }
      return new Archive(uri, temporary, true);
    } catch (IOException | RuntimeException e) {
      try {
        Files.deleteIfExists(temporary);
      } catch (IOException cleanup) {
        e.addSuppressed(cleanup);
      }
      throw e;
    }
  }

  public void read(
      final Archive archive,
      final BlockHeader currentHead,
      final Hash genesisHash,
      final Consumer<Block> consumer)
      throws IOException {
    final var metadata = EreFileSource.metadata(archive.uri());
    final long firstNumber = metadata.epoch() * EreFileSource.BLOCKS_PER_ARCHIVE;
    final BlockHeader[] previous = new BlockHeader[1];
    try (archive;
        var channel = Files.newByteChannel(archive.path())) {
      new EreReader(new SnappyFactory())
          .read(
              channel,
              MAX_COMPONENT_SIZE,
              (number, header, body) -> {
                if (Thread.currentThread().isInterrupted()) {
                  throw new IOException("Ere import interrupted");
                }
                final var headerInput = new BytesValueRLPInput(Bytes.wrap(header), false);
                final BlockHeader decodedHeader =
                    BlockHeader.readFrom(headerInput, headerFunctions);
                final var bodyInput = new BytesValueRLPInput(Bytes.wrap(body), false);
                final BlockBody decodedBody =
                    BlockBody.readWrappedBodyFrom(bodyInput, headerFunctions, true);
                if (!headerInput.isDone()
                    || !bodyInput.isDone()
                    || decodedHeader.getNumber() != number) {
                  throw new IOException("Ere block index or RLP data does not match its header");
                }
                final BlockHeader parent = previous[0];
                if (parent == null
                    ? number != firstNumber
                    : !decodedHeader.getParentHash().equals(parent.getHash())) {
                  throw new IOException(
                      "Ere archive has an incorrect start or discontinuous chain");
                }
                if (number == 0 && !decodedHeader.getHash().equals(genesisHash)) {
                  throw new IOException("Ere genesis does not match the configured network");
                }
                if (number == currentHead.getNumber()
                    && !decodedHeader.getHash().equals(currentHead.getHash())) {
                  throw new IOException("Ere resume block does not match the existing chain head");
                }
                previous[0] = decodedHeader;
                if (number > currentHead.getNumber()) {
                  consumer.accept(new Block(decodedHeader, decodedBody));
                }
              });
      final BlockHeader last = previous[0];
      if (last == null
          || !last.getHash().toHexString().substring(2).startsWith(metadata.lastHashPrefix())) {
        throw new IOException("Ere filename does not match its last block hash");
      }
    } finally {
      synchronized (this) {
        temporaryFiles.remove(archive.path());
      }
    }
  }

  private void download(final URI uri, final Path target) throws IOException {
    final HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
    connection.setConnectTimeout(30000);
    connection.setReadTimeout(60000);
    connection.setInstanceFollowRedirects(false);
    try {
      if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
        throw new IOException("Ere archive returned HTTP " + connection.getResponseCode());
      }
      final long expected = connection.getContentLengthLong();
      if (expected > MAX_DOWNLOAD_SIZE) {
        throw new IOException("Ere archive exceeds download size limit");
      }
      long total = 0;
      try (var input = connection.getInputStream();
          var output = Files.newOutputStream(target)) {
        final byte[] buffer = new byte[64 * 1024];
        int count;
        while ((count = input.read(buffer)) != -1) {
          total += count;
          if (total > MAX_DOWNLOAD_SIZE) {
            throw new IOException("Ere archive exceeds download size limit");
          }
          if (closed || Thread.currentThread().isInterrupted()) {
            throw new IOException("Ere download cancelled");
          }
          output.write(buffer, 0, count);
        }
      }
      if (expected >= 0 && total != expected) {
        throw new IOException("Truncated Ere download");
      }
    } finally {
      connection.disconnect();
    }
  }

  @Override
  public synchronized void close() {
    closed = true;
    for (final Path file : temporaryFiles) {
      try {
        Files.deleteIfExists(file);
      } catch (IOException e) {
        LOG.warn("Unable to remove temporary Ere archive {}", file, e);
      }
    }
    temporaryFiles.clear();
  }

  public record Archive(URI uri, Path path, boolean temporary) implements AutoCloseable {
    @Override
    public void close() throws IOException {
      if (temporary) {
        Files.deleteIfExists(path);
      }
    }
  }
}
