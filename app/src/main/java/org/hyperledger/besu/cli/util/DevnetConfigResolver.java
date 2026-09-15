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
package org.hyperledger.besu.cli.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.hyperledger.besu.datatypes.Hash;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves the genesis file and bootnodes of an ethpandaops devnet, e.g. {@code
 * glamsterdam-devnet-11}, and caches them locally.
 *
 * <p>A devnet name {@code <prefix>-<dir>} maps to {@code
 * ethpandaops/<prefix>-devnets/network-configs/<dir>/metadata}.
 */
public final class DevnetConfigResolver {

  private static final Logger LOG = LoggerFactory.getLogger(DevnetConfigResolver.class);

  /** The default location the devnet repositories are fetched from. */
  public static final String DEFAULT_BASE_URL = "https://raw.githubusercontent.com/ethpandaops/";

  static final String GENESIS_FILE = "genesis.json";
  static final String ENODES_FILE = "enodes.txt";
  static final String ENRS_FILE = "el_enrs.txt";
  static final String GENESIS_HASH_FILE = "deposit_contract_block_hash.txt";

  // Repo prefixes contain no dash, so the first dash separates repo from devnet directory. The
  // restricted character set also keeps the name safe to use as a path segment.
  private static final Pattern NAME_PATTERN =
      Pattern.compile("^([a-z0-9]+)-([a-z0-9][a-z0-9-]*(?:\\.[a-z0-9-]+)*)$");
  private static final Duration TIMEOUT = Duration.ofSeconds(30);

  private DevnetConfigResolver() {}

  /**
   * The resolved configuration of a devnet.
   *
   * @param genesisFile the cached genesis file
   * @param bootnodes the enode and ENR bootnodes
   * @param expectedGenesisHash the genesis block hash published with the devnet, if any
   */
  public record DevnetConfig(
      Path genesisFile, List<String> bootnodes, Optional<Hash> expectedGenesisHash) {}

  /**
   * Resolves the configuration of the given devnet.
   *
   * <p>The genesis file and genesis hash are only downloaded if they are not cached yet, so the
   * chain a data directory was initialised with never changes underneath it. Bootnodes are
   * refreshed on every call, falling back to the cached copy if the download fails.
   *
   * @param name the devnet name, e.g. {@code glamsterdam-devnet-11}
   * @param cacheDir the directory the devnet files are cached in
   * @param baseUrl the base URL the devnet repositories are fetched from
   * @return the resolved devnet configuration
   * @throws DevnetConfigResolutionException if the devnet cannot be resolved
   */
  public static DevnetConfig resolve(final String name, final Path cacheDir, final String baseUrl) {
    final URI metadataUri = metadataUri(name, baseUrl);
    try (HttpClient httpClient =
        HttpClient.newBuilder()
            .connectTimeout(TIMEOUT)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build()) {
      Files.createDirectories(cacheDir);

      final Path genesisFile = cacheDir.resolve(GENESIS_FILE);
      if (!Files.exists(genesisFile)) {
        final URI genesisUri = metadataUri.resolve(GENESIS_FILE);
        final byte[] genesis =
            fetch(httpClient, genesisUri)
                .orElseThrow(
                    () ->
                        new DevnetConfigResolutionException(
                            "Unknown devnet '" + name + "', no genesis found at " + genesisUri));
        // Fetched together with the genesis, so the hash always belongs to the cached genesis.
        final Optional<byte[]> genesisHash =
            fetch(httpClient, metadataUri.resolve(GENESIS_HASH_FILE));
        if (genesisHash.isPresent()) {
          writeAtomically(cacheDir.resolve(GENESIS_HASH_FILE), genesisHash.get());
        }
        writeAtomically(genesisFile, genesis);
        LOG.info("Downloaded genesis of devnet {} from {}", name, genesisUri);
      }

      final List<String> bootnodes = new ArrayList<>();
      bootnodes.addAll(refreshAndRead(httpClient, metadataUri, cacheDir, ENODES_FILE));
      bootnodes.addAll(refreshAndRead(httpClient, metadataUri, cacheDir, ENRS_FILE));
      if (bootnodes.isEmpty()) {
        LOG.warn("No bootnodes found for devnet {} at {}", name, metadataUri);
      }

      return new DevnetConfig(genesisFile, bootnodes, readGenesisHash(cacheDir));
    } catch (final IOException e) {
      throw new DevnetConfigResolutionException(
          "Failed to resolve devnet '" + name + "' from " + metadataUri + ": " + describe(e), e);
    }
  }

  /**
   * Returns the URI of the metadata directory of the given devnet.
   *
   * @param name the devnet name
   * @param baseUrl the base URL the devnet repositories are fetched from
   * @return the metadata directory URI, ending with a slash
   * @throws DevnetConfigResolutionException if the name is not a valid devnet name
   */
  static URI metadataUri(final String name, final String baseUrl) {
    final Matcher matcher = NAME_PATTERN.matcher(name);
    if (!matcher.matches()) {
      throw new DevnetConfigResolutionException(
          "Invalid devnet name '"
              + name
              + "', expected <prefix>-<devnet> (e.g. glamsterdam-devnet-11)");
    }
    final String base = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
    return URI.create(
        base
            + matcher.group(1)
            + "-devnets/master/network-configs/"
            + matcher.group(2)
            + "/metadata/");
  }

  private static List<String> refreshAndRead(
      final HttpClient httpClient, final URI metadataUri, final Path cacheDir, final String file)
      throws IOException {
    final Path cached = cacheDir.resolve(file);
    final URI uri = metadataUri.resolve(file);
    try {
      final Optional<byte[]> content = fetch(httpClient, uri);
      if (content.isPresent()) {
        writeAtomically(cached, content.get());
      } else {
        Files.deleteIfExists(cached);
      }
    } catch (final IOException e) {
      if (!Files.exists(cached)) {
        throw e;
      }
      LOG.warn("Failed to refresh {}, using cached copy: {}", uri, describe(e));
    }
    return Files.exists(cached) ? readLines(cached) : List.of();
  }

  private static Optional<Hash> readGenesisHash(final Path cacheDir) throws IOException {
    final Path hashFile = cacheDir.resolve(GENESIS_HASH_FILE);
    if (!Files.exists(hashFile)) {
      return Optional.empty();
    }
    final String hash = Files.readString(hashFile, UTF_8).trim();
    try {
      return Optional.of(Hash.fromHexString(hash));
    } catch (final IllegalArgumentException e) {
      LOG.warn("Ignoring invalid genesis hash '{}' in {}", hash, hashFile);
      return Optional.empty();
    }
  }

  private static Optional<byte[]> fetch(final HttpClient httpClient, final URI uri)
      throws IOException {
    final HttpRequest request = HttpRequest.newBuilder(uri).timeout(TIMEOUT).GET().build();
    final HttpResponse<byte[]> response;
    try {
      response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while fetching " + uri, e);
    }
    if (response.statusCode() == 404) {
      return Optional.empty();
    }
    if (response.statusCode() != 200) {
      throw new IOException("HTTP " + response.statusCode() + " fetching " + uri);
    }
    return Optional.of(response.body());
  }

  // A partially written file would otherwise be picked up as a valid cache entry on restart.
  private static void writeAtomically(final Path target, final byte[] content) throws IOException {
    final Path tmp = Files.createTempFile(target.getParent(), target.getFileName().toString(), "");
    try {
      Files.write(tmp, content);
      Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    } finally {
      Files.deleteIfExists(tmp);
    }
  }

  private static List<String> readLines(final Path path) throws IOException {
    try (var lines = Files.lines(path, UTF_8)) {
      return lines
          .map(String::trim)
          .filter(l -> !l.isEmpty())
          .filter(l -> !l.startsWith("#"))
          .toList();
    }
  }

  // Connection failures often carry no message, which would otherwise be logged as "null".
  private static String describe(final IOException e) {
    return e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
  }

  /** Exception thrown when a devnet configuration cannot be resolved. */
  public static final class DevnetConfigResolutionException extends RuntimeException {
    /**
     * Constructs a new DevnetConfigResolutionException with the specified detail message.
     *
     * @param message The detail message.
     */
    public DevnetConfigResolutionException(final String message) {
      super(message);
    }

    /**
     * Constructs a new DevnetConfigResolutionException with the specified detail message and cause.
     *
     * @param message The detail message.
     * @param cause The cause of the exception.
     */
    public DevnetConfigResolutionException(final String message, final Throwable cause) {
      super(message, cause);
    }
  }
}
