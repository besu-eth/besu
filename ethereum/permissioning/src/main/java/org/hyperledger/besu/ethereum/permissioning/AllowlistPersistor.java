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
package org.hyperledger.besu.ethereum.permissioning;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.toml.Toml;
import org.apache.tuweni.toml.TomlParseError;
import org.apache.tuweni.toml.TomlParseResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AllowlistPersistor {
  private static final Logger LOG = LoggerFactory.getLogger(AllowlistPersistor.class);

  // The node and account allowlists can share a file, each written by its own controller through
  // its own persistor. Every persistor takes this lock to rewrite its file, so that one cannot
  // write back a stale copy of the allowlist the other has just changed. There are at most two
  // files and changes are rare, so writes to separate files can wait for each other.
  private static final Object FILE_LOCK = new Object();

  private final File configurationFile;
  private final FileMover fileMover;

  /** Moves a file over another one. */
  @FunctionalInterface
  interface FileMover {
    void move(Path source, Path target) throws IOException;
  }

  public enum ALLOWLIST_TYPE {
    ACCOUNTS("accounts-allowlist"),
    NODES("nodes-allowlist");

    private final String tomlKey;

    ALLOWLIST_TYPE(final String tomlKey) {
      this.tomlKey = tomlKey;
    }

    public String getTomlKey() {
      return tomlKey;
    }
  }

  public AllowlistPersistor(final String configurationFile) {
    this(
        configurationFile,
        (source, target) -> Files.move(source, target, StandardCopyOption.ATOMIC_MOVE));
  }

  @VisibleForTesting
  AllowlistPersistor(final String configurationFile, final FileMover fileMover) {
    this.configurationFile = new File(configurationFile);
    this.fileMover = fileMover;
  }

  /**
   * Checks that the allowlist of the given type in the file has the same entries as the given one,
   * in any order. A file with more entries does not match either: they would be lost when the file
   * is next written from the given list.
   *
   * @param allowlistType the allowlist to compare
   * @param checkLists the entries the allowlist in the file must have
   * @param configurationFilePath the file to read
   * @return true if the allowlists match
   * @throws IOException if the file cannot be read or is not valid TOML
   * @throws AllowlistFileSyncException if the allowlists do not match
   */
  public static boolean verifyConfigFileMatchesState(
      final ALLOWLIST_TYPE allowlistType,
      final Collection<String> checkLists,
      final Path configurationFilePath)
      throws IOException, AllowlistFileSyncException {

    final Map<ALLOWLIST_TYPE, Collection<String>> configItems =
        existingConfigItems(configurationFilePath);
    final Collection<String> existingValues =
        configItems.get(allowlistType) != null
            ? configItems.get(allowlistType)
            : Collections.emptyList();

    if (!new HashSet<>(existingValues).equals(new HashSet<>(checkLists))) {
      LOG.atDebug()
          .setMessage("\n LISTS DO NOT MATCH {} configFile::{}")
          .addArgument(existingValues)
          .addArgument(configurationFilePath)
          .log();
      LOG.atDebug()
          .setMessage("\nLISTS DO NOT MATCH in-memory :: {}")
          .addArgument(checkLists)
          .log();
      throw new AllowlistFileSyncException();
    }
    return true;
  }

  public boolean verifyConfigFileMatchesState(
      final ALLOWLIST_TYPE allowlistType, final Collection<String> checkLists)
      throws IOException, AllowlistFileSyncException {
    // the file can be rewritten in place, see replaceFile, and must not be read half written
    synchronized (FILE_LOCK) {
      return verifyConfigFileMatchesState(allowlistType, checkLists, configurationFile.toPath());
    }
  }

  /**
   * Replaces the allowlist of the given type in the file, keeping the other allowlist as it is in
   * the file. The file is replaced as a whole, so that it is never left partly written.
   *
   * @param allowlistType the allowlist to replace
   * @param updatedAllowlistValues its new entries
   * @throws IOException if the file cannot be read or written, or is not valid TOML
   */
  public void updateConfig(
      final ALLOWLIST_TYPE allowlistType, final Collection<String> updatedAllowlistValues)
      throws IOException {
    synchronized (FILE_LOCK) {
      // the other allowlist is read under the lock, so that its latest version is kept
      final Map<ALLOWLIST_TYPE, Collection<String>> existingItems =
          existingConfigItems(configurationFile.toPath());
      final List<String> lines = new ArrayList<>();
      for (final ALLOWLIST_TYPE otherType : ALLOWLIST_TYPE.values()) {
        if (otherType != allowlistType && existingItems.containsKey(otherType)) {
          lines.add(valueListToTomlArray(otherType, existingItems.get(otherType)));
        }
      }
      lines.add(valueListToTomlArray(allowlistType, updatedAllowlistValues));
      replaceFile(String.join(System.lineSeparator(), lines) + System.lineSeparator());
    }
  }

  /**
   * Writes the content to a temporary file next to the configuration file, flushes it to disk, and
   * moves it over the configuration file, so that a crash leaves either the old or the new file.
   *
   * <p>Where the file cannot be replaced, it is rewritten in place, as it always was before: when
   * no temporary file can be created next to it, or when it cannot be moved over, as a single file
   * bind-mounted into a container, or on a file system without atomic moves.
   */
  private void replaceFile(final String content) throws IOException {
    // the file a symbolic link points to is replaced, not the link
    final Path target = configurationFile.toPath().toRealPath();
    // replacing goes through the directory, so it would succeed on a file made read-only
    if (!Files.isWritable(target)) {
      throw new AccessDeniedException(target.toString(), null, "the file is not writable");
    }
    final Path directory =
        Objects.requireNonNull(target.getParent(), "a real path to a file has a parent");
    final Path temporaryFile;
    try {
      temporaryFile = Files.createTempFile(directory, "." + target.getFileName() + ".", ".tmp");
    } catch (final IOException e) {
      rewriteInPlace(target, content, e);
      return;
    }
    try {
      // written before its permissions are set, which may not let Besu write it: Besu may be
      // allowed to write the configuration file by its group only
      write(temporaryFile, content, StandardOpenOption.WRITE);
      copyPermissions(target, temporaryFile);
      try {
        fileMover.move(temporaryFile, target);
      } catch (final FileSystemException e) {
        rewriteInPlace(target, content, e);
        return;
      }
    } finally {
      Files.deleteIfExists(temporaryFile);
    }
    syncDirectory(directory);
  }

  /**
   * Rewrites the file in place. If that fails too, the error that prevented replacing it is thrown,
   * with this one attached.
   */
  private static void rewriteInPlace(
      final Path target, final String content, final IOException cannotReplace) throws IOException {
    LOG.warn(
        "Cannot replace {} atomically, rewriting it in place: {}",
        target,
        cannotReplace.toString());
    try {
      write(target, content, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    } catch (final IOException e) {
      cannotReplace.addSuppressed(e);
      throw cannotReplace;
    }
  }

  private static void write(final Path file, final String content, final OpenOption... options)
      throws IOException {
    try (FileChannel channel = FileChannel.open(file, options)) {
      final ByteBuffer buffer = ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8));
      while (buffer.hasRemaining()) {
        channel.write(buffer);
      }
      channel.force(true);
    }
  }

  /**
   * Gives the new file the permissions of the one it replaces. It belongs to the user Besu runs as,
   * which may not own the replaced file, so the owner can always read and write it.
   */
  private static void copyPermissions(final Path from, final Path to) throws IOException {
    if (Files.getFileStore(to).supportsFileAttributeView(PosixFileAttributeView.class)) {
      final Set<PosixFilePermission> permissions =
          new HashSet<>(Files.getPosixFilePermissions(from));
      permissions.add(PosixFilePermission.OWNER_READ);
      permissions.add(PosixFilePermission.OWNER_WRITE);
      Files.setPosixFilePermissions(to, permissions);
    }
  }

  /** Flushes the directory entry of the replaced file to disk, where the platform allows it. */
  private static void syncDirectory(final Path directory) {
    try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    } catch (final IOException e) {
      LOG.trace("Could not flush directory {} to disk", directory, e);
    }
  }

  private static Map<ALLOWLIST_TYPE, Collection<String>> existingConfigItems(
      final Path configurationFilePath) throws IOException {
    TomlParseResult parsedToml = Toml.parse(configurationFilePath);
    // the parser keeps what it can of an invalid file, and writing that back would lose the rest,
    // such as the entries after a typo in a line edited by hand
    if (parsedToml.hasErrors()) {
      throw new IOException(
          "Invalid TOML in "
              + configurationFilePath
              + ": "
              + parsedToml.errors().stream()
                  .map(TomlParseError::toString)
                  .collect(Collectors.joining("; ")));
    }

    return Arrays.stream(ALLOWLIST_TYPE.values())
        .filter(k -> parsedToml.contains(k.getTomlKey()))
        .map(
            allowlist_type ->
                new AbstractMap.SimpleImmutableEntry<>(
                    allowlist_type, parsedToml.getArrayOrEmpty(allowlist_type.getTomlKey())))
        .collect(
            Collectors.toMap(
                AbstractMap.SimpleImmutableEntry::getKey,
                o ->
                    o.getValue().toList().parallelStream()
                        .map(Object::toString)
                        .collect(Collectors.toList())));
  }

  @VisibleForTesting
  public static void addNewConfigItem(
      final ALLOWLIST_TYPE allowlistType,
      final Collection<String> allowlistValues,
      final Path configFilePath)
      throws IOException {
    String newConfigItem = valueListToTomlArray(allowlistType, allowlistValues);

    Files.writeString(
        configFilePath,
        newConfigItem,
        StandardCharsets.UTF_8,
        StandardOpenOption.WRITE,
        StandardOpenOption.APPEND);
  }

  private static String valueListToTomlArray(
      final ALLOWLIST_TYPE allowlistType, final Collection<String> allowlistValues) {
    return String.format(
        "%s=[%s]",
        allowlistType.getTomlKey(),
        allowlistValues.parallelStream()
            .map(uri -> String.format("\"%s\"", uri))
            .collect(Collectors.joining(",")));
  }
}
