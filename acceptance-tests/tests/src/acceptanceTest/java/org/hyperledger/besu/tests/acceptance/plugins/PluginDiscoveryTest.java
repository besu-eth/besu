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
package org.hyperledger.besu.tests.acceptance.plugins;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.junit.jupiter.api.Test;

/**
 * Plugin discovery and selection: jars load in file name order, {@code --plugins} takes a plugin id
 * or an unambiguous simple class name and is applied before any plugin is instantiated, and a
 * plugins directory that cannot be read or a jar that cannot be loaded stops the node.
 *
 * <p>{@code discoveryTestPlugins.jar} holds a second {@code TestPicoCLIPlugin}, in another package,
 * and a plugin whose constructor throws; its file name sorts before {@code testPlugins.jar}.
 */
public class PluginDiscoveryTest extends AcceptanceTestBase {
  private static final String MAIN_PLUGIN =
      "org.hyperledger.besu.tests.acceptance.plugins.TestPicoCLIPlugin";
  private static final String SAME_NAME_PLUGIN =
      "org.hyperledger.besu.tests.acceptance.plugins.discovery.TestPicoCLIPlugin";
  private static final String BROKEN_PLUGIN =
      "org.hyperledger.besu.tests.acceptance.plugins.discovery.BrokenConstructorPlugin";

  private BesuNode node;

  private BesuNode nodeWith(final List<String> jars, final List<String> requestedPlugins)
      throws IOException {
    return besu.createPluginsNode(
        "node1",
        jars,
        builder -> builder.jsonRpcEnabled().jsonRpcAdmin().requestedPlugins(requestedPlugins));
  }

  private String startAndCaptureConsole(final BesuNode node) {
    cluster.startConsoleCapture();
    cluster.runNodeStart(node);
    node.verify(net.netServicesAllActive());
    waitForFile(node.homeDirectory().resolve("plugins/pluginLifecycle.started"));
    return cluster.getConsoleContents();
  }

  private void assertStartupFails(final BesuNode node, final String... expectedConsoleText) {
    cluster.startConsoleCapture();

    // runNodeStart throws when the process exits before writing its ports file
    final Throwable thrown = catchThrowable(() -> cluster.runNodeStart(node));
    assertThat(thrown).isInstanceOf(IllegalStateException.class);

    // the exit code is set asynchronously via process.onExit(); wait for it
    await().atMost(10, SECONDS).until(() -> node.exitCode().isPresent());
    assertThat(node.exitCode().orElseThrow()).isNotZero();

    assertThat(cluster.getConsoleContents()).contains(expectedConsoleText);
  }

  private static final List<String> SUMMARY_LINE_STARTS =
      List.of(
          " - ",
          "Plugin Registration Summary:",
          "Registered Plugins:",
          "No plugins have been registered.",
          "Detected but not registered:",
          "Detected but not selected:",
          "TOTAL = ");

  /**
   * The lines of the framed startup overview, with the frame removed and the lines the frame chops
   * at 76 characters joined again, so that a plugin id can be matched whole. A segment follows a
   * full one unless it starts like a summary line of its own.
   */
  private static List<String> unframe(final String console) {
    final List<String> lines = new ArrayList<>();
    final StringBuilder current = new StringBuilder();
    boolean previousFull = false;
    for (final String raw : console.lines().toList()) {
      final String line = raw.replaceAll("\u001b\\[[0-9;]*m", "");
      if (line.length() != 100 || !line.startsWith("# ") || !line.endsWith(" #")) {
        continue;
      }
      final String segment = line.substring(2, 98).stripTrailing();
      final boolean continuation =
          previousFull && SUMMARY_LINE_STARTS.stream().noneMatch(segment::startsWith);
      if (!continuation && current.length() > 0) {
        lines.add(current.toString());
        current.setLength(0);
      }
      current.append(segment);
      previousFull = segment.length() == 76;
    }
    if (current.length() > 0) {
      lines.add(current.toString());
    }
    return lines;
  }

  /** Writes a jar holding only a service file that declares the given plugin classes. */
  private static void writeJarDeclaring(final Path jar, final String... pluginClassNames)
      throws IOException {
    try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar))) {
      out.putNextEntry(new ZipEntry("META-INF/services/org.hyperledger.besu.plugin.BesuPlugin"));
      out.write(String.join("\n", pluginClassNames).getBytes(StandardCharsets.UTF_8));
      out.closeEntry();
    }
  }

  @Test
  public void pluginsRegisterInJarFileNameOrderAndUnselectedPluginsNeverRun() throws IOException {
    // testPlugins.jar is copied into the plugins directory first, but sorts last
    node =
        nodeWith(
            List.of("testPlugins", "discoveryTestPlugins"), List.of(MAIN_PLUGIN, SAME_NAME_PLUGIN));

    final String console = startAndCaptureConsole(node);

    final int sameNameRegistered =
        console.indexOf("Registered plugin of type " + SAME_NAME_PLUGIN + ".");
    final int mainRegistered = console.indexOf("Registered plugin of type " + MAIN_PLUGIN + ".");
    assertThat(sameNameRegistered).isNotNegative().isLessThan(mainRegistered);
    // the broken plugin was left out of --plugins, so its constructor never ran
    assertThat(unframe(console))
        .containsSubsequence("Detected but not selected:", " - " + BROKEN_PLUGIN);
    assertThat(console).doesNotContain("Error instantiating plugin");
  }

  @Test
  public void simpleClassNameSelectsThePluginWhenItNamesOnlyOne() throws IOException {
    node = nodeWith(List.of("testPlugins"), List.of("TestPicoCLIPlugin"));

    final String console = startAndCaptureConsole(node);

    assertThat(console).contains("Registered plugin of type " + MAIN_PLUGIN + ".");
  }

  @Test
  public void idSelectsOneOfTwoPluginsWithTheSameSimpleClassName() throws IOException {
    node = nodeWith(List.of("testPlugins", "discoveryTestPlugins"), List.of(MAIN_PLUGIN));

    final String console = startAndCaptureConsole(node);

    assertThat(console).contains("Registered plugin of type " + MAIN_PLUGIN + ".");
    assertThat(unframe(console))
        .containsSubsequence("Detected but not selected:", " - " + SAME_NAME_PLUGIN);
  }

  @Test
  public void simpleClassNameNamingTwoPluginsStopsTheNode() throws IOException {
    node = nodeWith(List.of("testPlugins", "discoveryTestPlugins"), List.of("TestPicoCLIPlugin"));

    assertStartupFails(
        node,
        "Plugin name TestPicoCLIPlugin matches several plugins, select one by its id: "
            + SAME_NAME_PLUGIN
            + ", "
            + MAIN_PLUGIN);
  }

  @Test
  public void selectedPluginWhoseConstructorFailsStopsTheNode() throws IOException {
    node = nodeWith(List.of("discoveryTestPlugins"), List.of());

    assertStartupFails(node, "Error instantiating plugin of type " + BROKEN_PLUGIN);
  }

  @Test
  public void pluginDeclarationThatCannotBeLoadedStopsTheNode() throws IOException {
    node = nodeWith(List.of("testPlugins"), List.of());
    final Path pluginsDir = node.homeDirectory().resolve("plugins");
    writeJarDeclaring(pluginsDir.resolve("broken.jar"), "com.acme.MissingPlugin");

    assertStartupFails(
        node,
        "Cannot load a plugin declared by a jar in " + pluginsDir.toAbsolutePath(),
        "com.acme.MissingPlugin");
  }

  @Test
  public void unreadablePluginsDirectoryStopsTheNode() throws IOException {
    node = nodeWith(List.of("testPlugins"), List.of());
    final Path pluginsDir = node.homeDirectory().resolve("plugins");
    assumeTrue(
        Files.getFileStore(pluginsDir).supportsFileAttributeView(PosixFileAttributeView.class));
    Files.setPosixFilePermissions(pluginsDir, Set.of());
    try {
      assumeTrue(!Files.isReadable(pluginsDir), "this user reads the directory regardless");

      assertStartupFails(node, "Cannot read the plugins directory " + pluginsDir.toAbsolutePath());
    } finally {
      Files.setPosixFilePermissions(pluginsDir, PosixFilePermissions.fromString("rwxr-xr-x"));
    }
  }
}
