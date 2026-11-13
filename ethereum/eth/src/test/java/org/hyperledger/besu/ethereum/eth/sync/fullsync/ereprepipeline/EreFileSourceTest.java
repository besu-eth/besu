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
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class EreFileSourceTest {
  @TempDir Path directory;

  @Test
  void ordersFilesAndAcceptsProfiles() throws IOException {
    final Path second = Files.createFile(directory.resolve("mainnet-00001-01234567-noproofs.ere"));
    final Path first =
        Files.createFile(directory.resolve("mainnet-00000-89abcdef-noproofs-noreceipts.ere"));
    Files.createFile(directory.resolve("mainnet-00000-01234567.erae"));
    Files.createFile(directory.resolve("mainnet-00000-01234567Xere"));
    final var source = new EreFileSource(directory.toUri(), 0);
    assertThat(source.next()).isEqualTo(first.toUri());
    assertThat(source.next()).isEqualTo(second.toUri());
    assertThat(source.hasNext()).isFalse();
    assertThatThrownBy(source::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void resumesInCurrentArchiveOrAtNextArchiveBoundary() throws IOException {
    final Path first = Files.createFile(directory.resolve("custom-chain-00000-01234567.ere"));
    final Path second = Files.createFile(directory.resolve("custom-chain-00001-89abcdef.ere"));
    assertThat(new EreFileSource(directory.toUri(), 42).next()).isEqualTo(first.toUri());
    assertThat(new EreFileSource(directory.toUri(), 8191).next()).isEqualTo(second.toUri());
    assertThat(new EreFileSource(directory.toUri(), 16383).hasNext()).isFalse();
  }

  @Test
  void failsOnMissingIntermediateEpoch() throws IOException {
    Files.createFile(directory.resolve("mainnet-00000-01234567.ere"));
    Files.createFile(directory.resolve("mainnet-00002-89abcdef.ere"));
    final var source = new EreFileSource(directory.toUri(), 0);
    source.next();
    assertThatIllegalStateException().isThrownBy(source::hasNext).withMessageContaining("epoch 1");
  }

  @Test
  void rejectsAmbiguousEpochs() throws IOException {
    Files.createFile(directory.resolve("mainnet-00000-01234567.ere"));
    Files.createFile(directory.resolve("mainnet-00000-89abcdef-noreceipts.ere"));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new EreFileSource(directory.toUri(), 0).hasNext())
        .withMessageContaining("Multiple Ere archives");
  }

  @Test
  void rejectsLegacyOnlyDirectory() throws IOException {
    Files.createFile(directory.resolve("mainnet-00000-01234567.erae"));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new EreFileSource(directory.toUri(), 0).hasNext())
        .withMessageContaining("No current-format");
  }

  @Test
  void acceptsUriEncodedLocalPathsWithoutAScheme() throws IOException {
    final Path spaced = Files.createDirectory(directory.resolve("history with spaces"));
    final Path file = Files.createFile(spaced.resolve("mainnet-00000-01234567.ere"));
    assertThat(new EreFileSource(URI.create(spaced.toUri().getRawPath()), 0).next())
        .isEqualTo(file.toUri());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "https://operator:archive-secret@example.invalid/",
        "https://operator:archive-secret@archive_host.invalid/",
        "https://example.invalid/?token=archive-secret",
        "https://example.invalid/#archive-secret"
      })
  void rejectsSensitiveUriPartsWithoutEchoingTheirValues(final String value) {
    final URI uri = URI.create(value);
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new EreFileSource(uri, 0))
        .withMessage("Ere URI must not contain user information, a query, or a fragment");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> EreFileSource.metadata(uri))
        .withMessage("Ere URI must not contain user information, a query, or a fragment");
  }

  @Test
  void invalidFilenameDoesNotExposeUriCredentials() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                EreFileSource.metadata(
                    URI.create("https://operator:archive-secret@example.invalid/invalid")))
        .withMessageNotContaining("archive-secret");
  }

  @Test
  void rejectsOpaqueUriWithoutEchoingItsValue() {
    final URI uri = URI.create("https:operator:archive-secret@example.invalid");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new EreFileSource(uri, 0))
        .withMessage("Ere URI must be hierarchical");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> EreFileSource.metadata(uri))
        .withMessage("Ere URI must be hierarchical");
  }

  @Test
  void discoversHttpFilesWithoutFollowingExternalLinks() throws IOException {
    final String html =
        "<a href='mainnet-00000-01234567-noreceipts.ere'>file</a>"
            + "<a href=\"https://other.example/mainnet-00001-89abcdef.ere\">external</a>"
            + "<a href='../mainnet-00001-89abcdef.ere'>parent</a>";
    final var server = server(200, html);
    try {
      final URI root = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/");
      final var source = new EreFileSource(root, 0);
      assertThat(source.next()).isEqualTo(root.resolve("mainnet-00000-01234567-noreceipts.ere"));
      assertThat(source.hasNext()).isFalse();
    } finally {
      server.stop(0);
    }
  }

  @Test
  void reportsHttpFailureInsteadOfFallingBackSilently() throws IOException {
    final var server = server(503, "unavailable");
    try {
      final URI root = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/");
      assertThatThrownBy(() -> new EreFileSource(root, 0).hasNext())
          .isInstanceOf(UncheckedIOException.class)
          .hasRootCauseMessage("Ere directory returned HTTP 503");
    } finally {
      server.stop(0);
    }
  }

  @Test
  void directoryFailureDoesNotExposePrivateUriPaths() throws IOException {
    final var server = server(503, "unavailable");
    try {
      final URI root =
          URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/archive-secret/");
      assertThatThrownBy(() -> new EreFileSource(root, 0).hasNext())
          .isInstanceOf(UncheckedIOException.class)
          .hasMessageNotContaining("archive-secret")
          .hasRootCauseMessage("Ere directory returned HTTP 503");
    } finally {
      server.stop(0);
    }
  }

  private static HttpServer server(final int status, final String body) throws IOException {
    final var server =
        HttpServer.create(
            new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), 0), 0);
    server.createContext(
        "/",
        exchange -> {
          final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(status, bytes.length);
          try (var output = exchange.getResponseBody()) {
            output.write(bytes);
          }
        });
    server.start();
    return server;
  }
}
