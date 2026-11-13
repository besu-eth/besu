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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.regex.Pattern;

/** Discovers current-format Ere archives in a local or HTTP(S) directory, in epoch order. */
public class EreFileSource implements Iterator<URI> {
  static final int BLOCKS_PER_ARCHIVE = 8192;
  private static final int MAX_LISTING_SIZE = 8 * 1024 * 1024;
  private static final Pattern FILE_NAME =
      Pattern.compile(
          "[a-z][a-z0-9-]*-(?<epoch>[0-9]{5})-(?<hash>[0-9a-f]{8})(?:-noproofs)?(?:-noreceipts)?\\.ere");
  private static final Pattern LINK =
      Pattern.compile("<a\\b[^<>]*\\bhref\\s*=\\s*[\"']([^\"'<>]+)[\"']", Pattern.CASE_INSENSITIVE);
  private final URI directory;
  private final TreeMap<Long, URI> archives = new TreeMap<>();
  private long nextEpoch;
  private boolean loaded;

  public EreFileSource(final URI directory, final long currentHeadBlockNumber) {
    validateUri(directory);
    if (currentHeadBlockNumber < 0 || currentHeadBlockNumber == Long.MAX_VALUE) {
      throw new IllegalArgumentException("Invalid current chain head");
    }
    this.directory =
        directory.getScheme() == null
            ? Path.of(directory.getPath()).toAbsolutePath().toUri()
            : directory;
    nextEpoch = (currentHeadBlockNumber + 1) / BLOCKS_PER_ARCHIVE;
  }

  @Override
  public boolean hasNext() {
    if (!loaded) {
      load();
      loaded = true;
    }
    if (!archives.containsKey(nextEpoch) && archives.higherKey(nextEpoch) != null) {
      throw new IllegalStateException("Missing Ere archive for epoch " + nextEpoch);
    }
    return archives.containsKey(nextEpoch);
  }

  @Override
  public URI next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more Ere archives");
    }
    return archives.get(nextEpoch++);
  }

  static FileMetadata metadata(final URI uri) {
    validateUri(uri);
    final String path = uri.getPath();
    final var matcher = FILE_NAME.matcher(path.substring(path.lastIndexOf('/') + 1));
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Not a current-format Ere filename");
    }
    return new FileMetadata(Long.parseLong(matcher.group("epoch")), matcher.group("hash"));
  }

  private void load() {
    try {
      switch (directory.getScheme()) {
        case "file" -> {
          try (var files = Files.list(Path.of(directory))) {
            files.filter(Files::isRegularFile).forEach(path -> add(path.toUri()));
          }
        }
        case "http", "https" -> loadHttpDirectory();
        default -> throw new IllegalArgumentException("Unsupported Ere URI scheme");
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to list Ere directory", e);
    }
    if (archives.isEmpty()) {
      throw new IllegalArgumentException("No current-format .ere archives found");
    }
  }

  private void loadHttpDirectory() throws IOException {
    if (!directory.getPath().endsWith("/")) {
      throw new IllegalArgumentException("Ere HTTP directory URI must end with /");
    }
    final HttpURLConnection connection = (HttpURLConnection) directory.toURL().openConnection();
    connection.setConnectTimeout(30000);
    connection.setReadTimeout(60000);
    connection.setInstanceFollowRedirects(false);
    try {
      if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
        throw new IOException("Ere directory returned HTTP " + connection.getResponseCode());
      }
      try (var input = connection.getInputStream()) {
        final byte[] data = input.readNBytes(MAX_LISTING_SIZE + 1);
        if (data.length > MAX_LISTING_SIZE) {
          throw new IOException("Ere directory listing exceeds size limit");
        }
        final var links = LINK.matcher(new String(data, StandardCharsets.UTF_8));
        while (links.find()) {
          final String link = links.group(1);
          // A directory listing may name files, but cannot redirect downloads to other origins
          // or paths outside the selected directory.
          if (FILE_NAME.matcher(link).matches()) {
            add(directory.resolve(link));
          }
        }
      }
    } finally {
      connection.disconnect();
    }
  }

  private void add(final URI uri) {
    final String path = uri.getPath();
    if (!FILE_NAME.matcher(path.substring(path.lastIndexOf('/') + 1)).matches()) {
      return;
    }
    final long epoch = metadata(uri).epoch();
    final URI previous = archives.putIfAbsent(epoch, uri);
    if (previous != null && !previous.equals(uri)) {
      throw new IllegalArgumentException("Multiple Ere archives found for epoch " + epoch);
    }
  }

  private static void validateUri(final URI uri) {
    if (uri.isOpaque()) {
      throw new IllegalArgumentException("Ere URI must be hierarchical");
    }
    // A malformed host can leave user information inside an unparsed authority.
    final String authority = uri.getRawAuthority();
    if ((authority != null && authority.contains("@"))
        || uri.getRawQuery() != null
        || uri.getRawFragment() != null) {
      throw new IllegalArgumentException(
          "Ere URI must not contain user information, a query, or a fragment");
    }
  }

  record FileMetadata(long epoch, String lastHashPrefix) {}
}
