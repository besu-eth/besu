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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.hyperledger.besu.ethereum.permissioning.AllowlistPersistor.verifyConfigFileMatchesState;

import org.hyperledger.besu.ethereum.permissioning.AllowlistPersistor.ALLOWLIST_TYPE;

import java.io.File;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.tuweni.toml.Toml;
import org.apache.tuweni.toml.TomlParseResult;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

public class AllowlistPersistorTest {

  private AllowlistPersistor allowlistPersistor;
  @TempDir private File tempFile;
  @TempDir private Path tempDirectory;
  private final String accountsAllowlist =
      String.format("%s=[%s]", ALLOWLIST_TYPE.ACCOUNTS.getTomlKey(), "\"account1\",\"account2\"");
  private final String nodesAllowlist =
      String.format("%s=[%s]", ALLOWLIST_TYPE.NODES.getTomlKey(), "\"node1\",\"node2\"");

  @BeforeEach
  public void setUp() throws IOException {
    List<String> lines = Lists.newArrayList(nodesAllowlist, accountsAllowlist);
    tempFile = File.createTempFile("test", "test");
    tempFile.deleteOnExit();
    Files.write(tempFile.toPath(), lines, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    allowlistPersistor = new AllowlistPersistor(tempFile.getAbsolutePath());
  }

  @Test
  public void otherAllowlistShouldBeKept() throws IOException {
    allowlistPersistor.updateConfig(ALLOWLIST_TYPE.NODES, Set.of("node5"));

    assertThat(countLines()).isEqualTo(2);
    assertThat(hasKeyAndExactLineContent(ALLOWLIST_TYPE.ACCOUNTS, accountsAllowlist)).isTrue();
    assertThat(hasKeyAndExactLineContent(ALLOWLIST_TYPE.NODES, "nodes-allowlist=[\"node5\"]"))
        .isTrue();
  }

  @Test
  public void allowlistShouldBeAddedWhenTheFileHasNone() throws IOException {
    Files.writeString(tempFile.toPath(), accountsAllowlist);

    allowlistPersistor.updateConfig(ALLOWLIST_TYPE.NODES, Set.of("node5"));

    assertThat(countLines()).isEqualTo(2);
    assertThat(hasKeyAndExactLineContent(ALLOWLIST_TYPE.ACCOUNTS, accountsAllowlist)).isTrue();
    assertThat(hasKeyAndContainsValue(ALLOWLIST_TYPE.NODES, "node5")).isTrue();
  }

  @Test
  public void lineShouldBeReplaced() throws IOException {
    ALLOWLIST_TYPE key = ALLOWLIST_TYPE.NODES;
    String newValue = "node5";

    assertThat(countLines()).isEqualTo(2);
    assertThat(hasKeyAndExactLineContent(key, nodesAllowlist)).isTrue();

    allowlistPersistor.updateConfig(key, Collections.singleton(newValue));

    assertThat(countLines()).isEqualTo(2);
    assertThat(hasKeyAndContainsValue(key, newValue)).isTrue();
    assertThat(hasKeyAndExactLineContent(key, nodesAllowlist)).isFalse();
  }

  @Test
  public void outputIsValidTOML() throws IOException {
    ALLOWLIST_TYPE key = ALLOWLIST_TYPE.ACCOUNTS;
    List<String> newValue = Lists.newArrayList("account5", "account6", "account4");
    String expectedValue =
        String.format("%s=[%s]", "accounts-allowlist", "\"account5\",\"account6\",\"account4\"");

    allowlistPersistor.updateConfig(key, newValue);

    assertThat(hasKey(key)).isTrue();
    assertThat(hasKeyAndExactLineContent(key, expectedValue)).isTrue();
  }

  @Test
  public void compareContentsWhenEqual_shouldMatch() throws Exception {
    final ALLOWLIST_TYPE key = ALLOWLIST_TYPE.ACCOUNTS;
    final List<String> newValue = Lists.newArrayList("account5", "account6", "account4");

    allowlistPersistor.updateConfig(key, newValue);

    assertThat(verifyConfigFileMatchesState(key, newValue, tempFile.toPath())).isTrue();
  }

  @Test
  public void compareContentsWhenDifferent_shouldThrow() throws Exception {
    final ALLOWLIST_TYPE key = ALLOWLIST_TYPE.ACCOUNTS;
    final List<String> newValue = Lists.newArrayList("account5", "account6", "account4");

    allowlistPersistor.updateConfig(key, newValue);

    assertThatThrownBy(
            () ->
                verifyConfigFileMatchesState(
                    key, Lists.newArrayList("not-the-same"), tempFile.toPath()))
        .isInstanceOf(AllowlistFileSyncException.class);
  }

  @Test
  public void compareContentsWhenFileHasMoreEntries_shouldThrow() {
    // e.g. an entry added to the file by hand, which writing the file from the list would drop
    assertThatThrownBy(
            () ->
                verifyConfigFileMatchesState(
                    ALLOWLIST_TYPE.NODES, List.of("node1"), tempFile.toPath()))
        .isInstanceOf(AllowlistFileSyncException.class);
  }

  @Test
  public void compareContentsWhenSameEntriesInAnotherOrder_shouldMatch() throws Exception {
    assertThat(
            verifyConfigFileMatchesState(
                ALLOWLIST_TYPE.NODES, List.of("node2", "node1"), tempFile.toPath()))
        .isTrue();
  }

  @Test
  public void fileWithTomlErrorsShouldNotBeRewritten() throws IOException {
    // a missing comma in a line edited by hand: the parser keeps only part of that allowlist
    final String invalid = nodesAllowlist + "\naccounts-allowlist=[\"account1\" \"account2\"]\n";
    Files.writeString(tempFile.toPath(), invalid);

    assertThatThrownBy(
            () -> allowlistPersistor.updateConfig(ALLOWLIST_TYPE.NODES, List.of("node3")))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Invalid TOML");
    assertThatThrownBy(
            () ->
                allowlistPersistor.verifyConfigFileMatchesState(
                    ALLOWLIST_TYPE.NODES, List.of("node1", "node2")))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Invalid TOML");
    assertThat(Files.readString(tempFile.toPath())).isEqualTo(invalid);
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  public void fileShouldNeverBeSeenPartlyWritten() throws Exception {
    // what a reader can see while the file is rewritten is what a crash can leave behind
    final AtomicBoolean writing = new AtomicBoolean(true);
    final AtomicInteger incompleteReads = new AtomicInteger();
    final Thread reader =
        new Thread(
            () -> {
              while (writing.get()) {
                try {
                  final TomlParseResult toml = Toml.parse(tempFile.toPath());
                  if (toml.hasErrors()
                      || !toml.contains(ALLOWLIST_TYPE.NODES.getTomlKey())
                      || !toml.contains(ALLOWLIST_TYPE.ACCOUNTS.getTomlKey())) {
                    incompleteReads.incrementAndGet();
                  }
                } catch (final IOException e) {
                  incompleteReads.incrementAndGet();
                }
              }
            });
    reader.start();
    try {
      for (int i = 0; i < 100; i++) {
        allowlistPersistor.updateConfig(ALLOWLIST_TYPE.NODES, List.of("node" + i));
      }
    } finally {
      writing.set(false);
      reader.join();
    }

    assertThat(incompleteReads.get()).isZero();
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  public void persistorsOfTheSameFileShouldNotUndoEachOthersChanges() throws Exception {
    // the node and account controllers each have a persistor, possibly over one file
    final AllowlistPersistor otherPersistor = new AllowlistPersistor(tempFile.getAbsolutePath());
    final int updates = 100;
    final CyclicBarrier start = new CyclicBarrier(2);
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final Future<?> nodes =
          executor.submit(
              () -> {
                start.await();
                for (int i = 1; i <= updates; i++) {
                  allowlistPersistor.updateConfig(ALLOWLIST_TYPE.NODES, List.of("node" + i));
                }
                return null;
              });
      final Future<?> accounts =
          executor.submit(
              () -> {
                start.await();
                for (int i = 1; i <= updates; i++) {
                  otherPersistor.updateConfig(ALLOWLIST_TYPE.ACCOUNTS, List.of("account" + i));
                }
                return null;
              });
      nodes.get();
      accounts.get();
    } finally {
      executor.shutdownNow();
    }

    // each allowlist is the last one written to it
    assertThat(
            verifyConfigFileMatchesState(
                ALLOWLIST_TYPE.NODES, List.of("node" + updates), tempFile.toPath()))
        .isTrue();
    assertThat(
            verifyConfigFileMatchesState(
                ALLOWLIST_TYPE.ACCOUNTS, List.of("account" + updates), tempFile.toPath()))
        .isTrue();
  }

  @Test
  public void fileShouldBeReplacedWithoutLeavingTemporaryFiles() throws IOException {
    final Path file =
        Files.writeString(tempDirectory.resolve("permissions_config.toml"), nodesAllowlist);

    new AllowlistPersistor(file.toString()).updateConfig(ALLOWLIST_TYPE.NODES, List.of("node5"));

    try (Stream<Path> files = Files.list(tempDirectory)) {
      assertThat(files).containsExactly(file);
    }
  }

  @Test
  public void fileASymbolicLinkPointsToShouldBeReplaced() throws IOException {
    final Path file = Files.writeString(tempDirectory.resolve("target.toml"), nodesAllowlist);
    final Path link = tempDirectory.resolve("permissions_config.toml");
    try {
      Files.createSymbolicLink(link, file.getFileName());
    } catch (final UnsupportedOperationException | IOException e) {
      Assumptions.abort("symbolic links are not supported here");
    }

    new AllowlistPersistor(link.toString()).updateConfig(ALLOWLIST_TYPE.NODES, List.of("node5"));

    assertThat(Files.isSymbolicLink(link)).isTrue();
    assertThat(Files.readString(file)).contains("node5");
  }

  @Test
  public void permissionsOfTheFileShouldBeKept() throws IOException {
    Assumptions.assumeTrue(
        Files.getFileStore(tempFile.toPath())
            .supportsFileAttributeView(PosixFileAttributeView.class));
    final Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rw-r-----");
    Files.setPosixFilePermissions(tempFile.toPath(), permissions);

    allowlistPersistor.updateConfig(ALLOWLIST_TYPE.NODES, List.of("node5"));

    assertThat(Files.getPosixFilePermissions(tempFile.toPath())).isEqualTo(permissions);
  }

  @Test
  public void fileInADirectoryThatCannotBeWrittenShouldStillBeUpdated() throws IOException {
    final Path file =
        Files.writeString(tempDirectory.resolve("permissions_config.toml"), nodesAllowlist);
    Assumptions.assumeTrue(
        Files.getFileStore(file).supportsFileAttributeView(PosixFileAttributeView.class));
    final Set<PosixFilePermission> directoryPermissions =
        Files.getPosixFilePermissions(tempDirectory);
    Files.setPosixFilePermissions(tempDirectory, PosixFilePermissions.fromString("r-x------"));
    try {
      // a user that may write any directory, such as root, cannot run this test
      Assumptions.assumeFalse(Files.isWritable(tempDirectory));

      new AllowlistPersistor(file.toString()).updateConfig(ALLOWLIST_TYPE.NODES, List.of("node5"));

      assertThat(Files.readString(file)).contains("node5");
    } finally {
      Files.setPosixFilePermissions(tempDirectory, directoryPermissions);
    }
  }

  @Test
  public void fileMadeReadOnlyShouldNotBeReplaced() throws IOException {
    Assumptions.assumeTrue(
        Files.getFileStore(tempFile.toPath())
            .supportsFileAttributeView(PosixFileAttributeView.class));
    final Path file =
        Files.writeString(tempDirectory.resolve("permissions_config.toml"), nodesAllowlist);
    Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("r--r--r--"));
    // a user that may write any file, such as root, cannot run this test
    Assumptions.assumeFalse(Files.isWritable(file));

    assertThatThrownBy(
            () ->
                new AllowlistPersistor(file.toString())
                    .updateConfig(ALLOWLIST_TYPE.NODES, List.of("node5")))
        .isInstanceOf(AccessDeniedException.class);

    assertThat(Files.readString(file)).isEqualTo(nodesAllowlist);
    try (Stream<Path> files = Files.list(tempDirectory)) {
      assertThat(files).containsExactly(file);
    }
  }

  @Test
  public void fileThatCannotBeMovedOverShouldBeRewrittenInPlace() throws IOException {
    // a single file bind-mounted into a container is a mount point, which cannot be renamed over
    final Path file =
        Files.writeString(tempDirectory.resolve("permissions_config.toml"), nodesAllowlist);
    final AllowlistPersistor persistor =
        new AllowlistPersistor(
            file.toString(),
            (source, target) -> {
              throw new FileSystemException(
                  source.toString(), target.toString(), "Device or resource busy");
            });

    persistor.updateConfig(ALLOWLIST_TYPE.NODES, List.of("node5"));

    assertThat(Files.readString(file)).contains("node5").doesNotContain("node1");
    try (Stream<Path> files = Files.list(tempDirectory)) {
      assertThat(files).containsExactly(file);
    }
  }

  @Test
  public void errorReplacingTheFileShouldBeThrownWhenItCannotBeRewrittenInPlaceEither()
      throws IOException {
    Assumptions.assumeTrue(
        Files.getFileStore(tempFile.toPath())
            .supportsFileAttributeView(PosixFileAttributeView.class));
    final Path file =
        Files.writeString(tempDirectory.resolve("permissions_config.toml"), nodesAllowlist);
    final FileSystemException cannotMove =
        new FileSystemException(file.toString(), null, "Input/output error");
    final AllowlistPersistor persistor =
        new AllowlistPersistor(
            file.toString(),
            (source, target) -> {
              Files.setPosixFilePermissions(target, PosixFilePermissions.fromString("r--------"));
              throw cannotMove;
            });
    try {
      final Throwable thrown =
          catchThrowable(() -> persistor.updateConfig(ALLOWLIST_TYPE.NODES, List.of("node5")));
      // a user that may write any file, such as root, rewrites it in place
      Assumptions.assumeTrue(thrown != null);

      assertThat(thrown).isSameAs(cannotMove);
      assertThat(thrown.getSuppressed()).hasSize(1);
      assertThat(Files.readString(file)).isEqualTo(nodesAllowlist);
    } finally {
      Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));
    }
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  public void fileShouldNotBeVerifiedWhileAnotherPersistorRewritesIt() throws Exception {
    // a rewrite in place leaves the file half written for a moment
    final CountDownLatch moving = new CountDownLatch(1);
    final CountDownLatch finishMove = new CountDownLatch(1);
    final AllowlistPersistor writer =
        new AllowlistPersistor(
            tempFile.getAbsolutePath(),
            (source, target) -> {
              moving.countDown();
              Uninterruptibles.awaitUninterruptibly(finishMove);
              Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
            });
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final Future<?> write =
          executor.submit(
              () -> {
                writer.updateConfig(ALLOWLIST_TYPE.NODES, List.of("node5"));
                return null;
              });
      moving.await();
      final Future<Boolean> verify =
          executor.submit(
              () ->
                  allowlistPersistor.verifyConfigFileMatchesState(
                      ALLOWLIST_TYPE.ACCOUNTS, List.of("account1", "account2")));

      assertThatThrownBy(() -> verify.get(500, TimeUnit.MILLISECONDS))
          .isInstanceOf(TimeoutException.class);
      finishMove.countDown();
      write.get();
      assertThat(verify.get()).isTrue();
    } finally {
      finishMove.countDown();
      executor.shutdownNow();
    }
  }

  private long countLines() throws IOException {
    try (Stream<String> lines = Files.lines(tempFile.toPath())) {
      return lines.count();
    }
  }

  private boolean hasKey(final ALLOWLIST_TYPE key) throws IOException {
    try (Stream<String> lines = Files.lines(tempFile.toPath())) {
      return lines.anyMatch(s -> s.startsWith(key.getTomlKey()));
    }
  }

  private boolean hasKeyAndContainsValue(final ALLOWLIST_TYPE key, final String value)
      throws IOException {
    try (Stream<String> lines = Files.lines(tempFile.toPath())) {
      return lines.anyMatch(s -> s.startsWith(key.getTomlKey()) && s.contains(value));
    }
  }

  private boolean hasKeyAndExactLineContent(final ALLOWLIST_TYPE key, final String exactKeyValue)
      throws IOException {
    try (Stream<String> lines = Files.lines(tempFile.toPath())) {
      return lines.anyMatch(s -> s.startsWith(key.getTomlKey()) && s.equals(exactKeyValue));
    }
  }
}
