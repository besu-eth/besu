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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIOException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockOptions;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EreFileReaderTest {
  @TempDir Path directory;
  private List<Block> blocks;
  private Path file;
  private EreFileReader reader;

  @BeforeEach
  void setUp() throws IOException {
    blocks = new BlockDataGenerator(11244).blockSequence(3);
    file = EreTestFixtures.write(directory, blocks);
    reader = new EreFileReader(new MainnetBlockHeaderFunctions(), directory);
  }

  @Test
  void decodesBlocksAndPreservesCallerOwnedFile() throws IOException {
    final List<Block> delivered = new ArrayList<>();
    reader.read(
        reader.prepare(file.toUri()),
        blocks.getFirst().getHeader(),
        blocks.getFirst().getHash(),
        delivered::add);
    assertThat(delivered).containsExactlyElementsOf(blocks.subList(1, 3));
    assertThat(file).exists();
  }

  @Test
  void resumesAfterExistingHeadWithoutReimportingEarlierBlocks() throws IOException {
    final List<Block> delivered = new ArrayList<>();
    reader.read(
        reader.prepare(file.toUri()),
        blocks.get(1).getHeader(),
        blocks.getFirst().getHash(),
        delivered::add);
    assertThat(delivered).containsExactly(blocks.get(2));
  }

  @Test
  void rejectsWrongGenesisBeforeImport() throws IOException {
    final List<Block> delivered = new ArrayList<>();
    assertThatIOException()
        .isThrownBy(
            () ->
                reader.read(
                    reader.prepare(file.toUri()),
                    blocks.getFirst().getHeader(),
                    Hash.ZERO,
                    delivered::add))
        .withMessageContaining("genesis");
    assertThat(delivered).isEmpty();
  }

  @Test
  void rejectsResumeHashMismatch() throws IOException {
    final Block other = new BlockDataGenerator(3).block(BlockOptions.create().setBlockNumber(1));
    assertThatIOException()
        .isThrownBy(
            () ->
                reader.read(
                    reader.prepare(file.toUri()),
                    other.getHeader(),
                    blocks.getFirst().getHash(),
                    ignored -> {}))
        .withMessageContaining("resume block");
  }

  @Test
  void rejectsIndexNumberMismatch() throws IOException {
    final byte[] data = Files.readAllBytes(file);
    ByteBuffer.wrap(data)
        .order(ByteOrder.LITTLE_ENDIAN)
        .putLong(data.length - 24 - blocks.size() * 16, 1);
    Files.write(file, data);
    assertThatIOException()
        .isThrownBy(
            () ->
                reader.read(
                    reader.prepare(file.toUri()),
                    blocks.getFirst().getHeader(),
                    blocks.getFirst().getHash(),
                    ignored -> {}))
        .withMessageContaining("index");
  }

  @Test
  void rejectsDiscontinuousHeaders() throws IOException {
    final Block other =
        new BlockDataGenerator(3)
            .block(BlockOptions.create().setBlockNumber(1).setParentHash(Hash.ZERO));
    final Path broken = EreTestFixtures.write(directory, List.of(blocks.getFirst(), other));
    assertThatIOException()
        .isThrownBy(
            () ->
                reader.read(
                    reader.prepare(broken.toUri()),
                    blocks.getFirst().getHeader(),
                    blocks.getFirst().getHash(),
                    ignored -> {}))
        .withMessageContaining("discontinuous");
  }

  @Test
  void rejectsIncorrectFilenameHash() throws IOException {
    final Path wrongName = directory.resolve("testnet-00000-00000000.ere");
    Files.move(file, wrongName);
    assertThatIOException()
        .isThrownBy(
            () ->
                reader.read(
                    reader.prepare(wrongName.toUri()),
                    blocks.getFirst().getHeader(),
                    blocks.getFirst().getHash(),
                    ignored -> {}))
        .withMessageContaining("filename");
  }

  @Test
  void cleansUpDownloadedArchiveAfterSuccessfulImport() throws IOException {
    final var server = server(200, Files.readAllBytes(file));
    try {
      final var archive = reader.prepare(remote(server));
      assertThat(archive.path()).exists().isNotEqualTo(file);
      final List<Block> delivered = new ArrayList<>();
      reader.read(
          archive, blocks.getFirst().getHeader(), blocks.getFirst().getHash(), delivered::add);
      assertThat(delivered).hasSize(2);
      assertThat(archive.path()).doesNotExist();
      assertThat(file).exists();
    } finally {
      server.stop(0);
    }
  }

  @Test
  void cleansUpDownloadedArchiveOnDecodeFailure() throws IOException {
    final byte[] data = Files.readAllBytes(file);
    data[30] ^= 1;
    final var server = server(200, data);
    try {
      final var archive = reader.prepare(remote(server));
      assertThatIOException()
          .isThrownBy(
              () ->
                  reader.read(
                      archive,
                      blocks.getFirst().getHeader(),
                      blocks.getFirst().getHash(),
                      ignored -> {}));
      assertThat(archive.path()).doesNotExist();
    } finally {
      server.stop(0);
    }
  }

  @Test
  void rejectsFailedDownloadAndRemovesTemporaryFile() throws IOException {
    final var server = server(503, new byte[] {1});
    try {
      assertThatIOException()
          .isThrownBy(() -> reader.prepare(remote(server)))
          .withMessageContaining("HTTP 503");
      assertNoTemporaryFiles();
    } finally {
      server.stop(0);
    }
  }

  @Test
  void rejectsUriCredentialsBeforeCreatingATemporaryArchive() throws IOException {
    final var server = server(200, Files.readAllBytes(file));
    try {
      final URI uri =
          URI.create(
              remote(server).toString().replace("http://", "http://operator:archive-secret@"));
      assertThatThrownBy(() -> reader.prepare(uri))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Ere URI must not contain user information, a query, or a fragment");
      assertNoTemporaryFiles();
    } finally {
      reader.close();
      server.stop(0);
    }
  }

  @Test
  void downloadFailureDoesNotExposePrivateUriPaths() throws IOException {
    final var server = server(503, new byte[] {1});
    try {
      final URI uri = remote(server).resolve("/archive-secret/" + file.getFileName());
      assertThatIOException()
          .isThrownBy(() -> reader.prepare(uri))
          .withMessageNotContaining("archive-secret")
          .withMessageContaining("HTTP 503");
      assertNoTemporaryFiles();
    } finally {
      reader.close();
      server.stop(0);
    }
  }

  @Test
  void closingReaderRemovesQueuedDownloadsAndPreventsFurtherPreparation() throws IOException {
    final var server = server(200, Files.readAllBytes(file));
    try {
      final var archive = reader.prepare(remote(server));
      reader.close();
      assertThat(archive.path()).doesNotExist();
      assertThatIOException()
          .isThrownBy(() -> reader.prepare(file.toUri()))
          .withMessageContaining("closed");
    } finally {
      server.stop(0);
    }
  }

  @Test
  void removesPartialFileAfterATruncatedDownload() throws IOException {
    final byte[] data = Files.readAllBytes(file);
    final var server =
        HttpServer.create(
            new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), 0), 0);
    server.createContext(
        "/",
        exchange -> {
          exchange.sendResponseHeaders(200, data.length + 10);
          try {
            exchange.getResponseBody().write(data);
          } finally {
            exchange.close();
          }
        });
    server.start();
    try {
      assertThatIOException().isThrownBy(() -> reader.prepare(remote(server)));
      assertNoTemporaryFiles();
    } finally {
      server.stop(0);
    }
  }

  @Test
  void rejectsOversizedDownloadBeforeReadingItsBody() throws IOException {
    final var server =
        HttpServer.create(
            new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), 0), 0);
    server.createContext(
        "/",
        exchange -> {
          exchange.sendResponseHeaders(200, 17L * 1024 * 1024 * 1024);
          exchange.getResponseBody().write(0);
          exchange.getResponseBody().flush();
          exchange.close();
        });
    server.start();
    try {
      assertThatIOException()
          .isThrownBy(() -> reader.prepare(remote(server)))
          .withMessageContaining("size limit");
      assertNoTemporaryFiles();
    } finally {
      server.stop(0);
    }
  }

  @Test
  void cleansUpWhenClosedDuringAnActiveDownload() throws Exception {
    final byte[] data = Files.readAllBytes(file);
    final var started = new CountDownLatch(1);
    final var finish = new CountDownLatch(1);
    final var server =
        HttpServer.create(
            new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), 0), 0);
    server.createContext(
        "/",
        exchange -> {
          try {
            exchange.sendResponseHeaders(200, data.length);
            started.countDown();
            if (finish.await(10, TimeUnit.SECONDS)) {
              exchange.getResponseBody().write(data);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            exchange.close();
          }
        });
    server.start();
    try {
      final var pending =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return reader.prepare(remote(server));
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
      reader.close();
      finish.countDown();
      assertThatThrownBy(() -> pending.get(10, TimeUnit.SECONDS))
          .hasRootCauseInstanceOf(IOException.class);
      assertNoTemporaryFiles();
    } finally {
      finish.countDown();
      server.stop(0);
    }
  }

  @Test
  void cleansUpAnInterruptedImport() throws IOException {
    final var server = server(200, Files.readAllBytes(file));
    try {
      final var archive = reader.prepare(remote(server));
      Thread.currentThread().interrupt();
      try {
        assertThatIOException()
            .isThrownBy(
                () ->
                    reader.read(
                        archive,
                        blocks.getFirst().getHeader(),
                        blocks.getFirst().getHash(),
                        ignored -> {}));
      } finally {
        Thread.interrupted();
      }
      assertNoTemporaryFiles();
    } finally {
      server.stop(0);
    }
  }

  @Test
  void closingReaderStopsAnActiveDownloadBeforeTheResponseCompletes() throws Exception {
    final var started = new CountDownLatch(1);
    final var sendByte = new CountDownLatch(1);
    final var finish = new CountDownLatch(1);
    final var server =
        HttpServer.create(
            new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), 0), 0);
    server.createContext(
        "/",
        exchange -> {
          try {
            exchange.sendResponseHeaders(200, 0);
            started.countDown();
            if (sendByte.await(10, TimeUnit.SECONDS)) {
              exchange.getResponseBody().write(0);
              exchange.getResponseBody().flush();
              finish.await(10, TimeUnit.SECONDS);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            exchange.close();
          }
        });
    server.start();
    try {
      final var pending =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return reader.prepare(remote(server));
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
      reader.close();
      sendByte.countDown();
      assertThatThrownBy(() -> pending.get(5, TimeUnit.SECONDS))
          .hasRootCauseInstanceOf(IOException.class);
      assertNoTemporaryFiles();
    } finally {
      sendByte.countDown();
      finish.countDown();
      reader.close();
      server.stop(0);
    }
  }

  private void assertNoTemporaryFiles() throws IOException {
    try (var files = Files.list(directory)) {
      assertThat(
              files.filter(path -> path.getFileName().toString().startsWith("besu-ere-")).toList())
          .isEmpty();
    }
  }

  private URI remote(final HttpServer server) {
    return URI.create(
        "http://127.0.0.1:" + server.getAddress().getPort() + "/" + file.getFileName());
  }

  private static HttpServer server(final int status, final byte[] bytes) throws IOException {
    final var server =
        HttpServer.create(
            new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), 0), 0);
    server.createContext(
        "/",
        exchange -> {
          exchange.sendResponseHeaders(status, bytes.length);
          try (var output = exchange.getResponseBody()) {
            output.write(bytes);
          }
        });
    server.start();
    return server;
  }
}
