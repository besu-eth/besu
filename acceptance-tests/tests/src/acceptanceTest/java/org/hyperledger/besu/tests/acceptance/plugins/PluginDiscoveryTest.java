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
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

import org.junit.jupiter.api.Test;

/**
 * Plugin discovery and selection: jars load in file name order, {@code --plugins} takes a plugin id
 * or an unambiguous simple class name and is applied before any plugin is instantiated, a plugins
 * directory that cannot be read or a jar that cannot be opened stops the node, and a plugin whose
 * class cannot be loaded stops it only when the plugin is selected.
 *
 * <p>{@code discoveryTestPlugins.jar} holds a second {@code TestPicoCLIPlugin}, in another package,
 * and four plugins that fail: in the constructor, in the static initializer, at class loading for
 * want of a library, and for want of a constructor without arguments; its file name sorts before
 * {@code testPlugins.jar}.
 */
public class PluginDiscoveryTest extends AcceptanceTestBase {
  // ids as strings: discoveryTestPlugins.jar is deliberately not on the test class path, so that
  // the ServiceLoader lookup of RunVersionTest keeps listing the plugins of testPlugins.jar only
  private static final String MAIN_PLUGIN =
      "org.hyperledger.besu.tests.acceptance.plugins.TestPicoCLIPlugin";
  private static final String SAME_NAME_PLUGIN =
      "org.hyperledger.besu.tests.acceptance.plugins.discovery.TestPicoCLIPlugin";
  private static final String BROKEN_CONSTRUCTOR_PLUGIN =
      "org.hyperledger.besu.tests.acceptance.plugins.discovery.BrokenConstructorPlugin";
  private static final String BROKEN_STATIC_INITIALIZER_PLUGIN =
      "org.hyperledger.besu.tests.acceptance.plugins.discovery.BrokenStaticInitializerPlugin";
  private static final String MISSING_DEPENDENCY_PLUGIN =
      "org.hyperledger.besu.tests.acceptance.plugins.discovery.MissingDependencyPlugin";
  private static final String NO_DEFAULT_CONSTRUCTOR_PLUGIN =
      "org.hyperledger.besu.tests.acceptance.plugins.discovery.NoDefaultConstructorPlugin";

  private BesuNode node;

  /**
   * A node with the given plugin jars copied into its own plugins directory, which the process
   * runner passes as {@code --plugins-dir} as soon as there is at least one jar.
   */
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
          "No external plugins have been registered.",
          "Detected but not registered:",
          "Detected but not selected:",
          "TOTAL = ");

  /**
   * The lines of the framed startup overview, with the frame removed and the lines the frame chops
   * at 76 characters joined again, so that a plugin id can be matched whole. A segment follows a
   * full one unless it starts like a summary line of its own. The widths come from {@code
   * FramedLogMessage}: 100 characters per framed line, the text chopped at 76.
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

  /** Writes a jar holding only a manifest, whose class path names the given jar. */
  private static void writeJarWithClassPath(final Path jar, final String classPath)
      throws IOException {
    final Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, classPath);
    new JarOutputStream(Files.newOutputStream(jar), manifest).close();
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
    // the broken plugins were left out of --plugins, so their constructor and static
    // initializer never ran
    assertThat(unframe(console))
        .containsSubsequence(
            "Detected but not selected:",
            " - " + BROKEN_CONSTRUCTOR_PLUGIN,
            " - " + BROKEN_STATIC_INITIALIZER_PLUGIN);
    assertThat(console).doesNotContain("Error instantiating plugin");
    // the plugin whose class cannot be loaded is declared ahead of both registered plugins, and
    // did not stop discovery from reaching them
    assertThat(unframe(console))
        .contains(" - " + MISSING_DEPENDENCY_PLUGIN + " (could not be loaded)");
  }

  @Test
  public void simpleClassNameSelectsThePluginWhenItNamesOnlyOne() throws IOException {
    node = nodeWith(List.of("testPlugins"), List.of("TestPicoCLIPlugin"));

    final String console = startAndCaptureConsole(node);

    assertThat(console).contains("Registered plugin of type " + MAIN_PLUGIN + ".");
    // the other plugins of the jar were left out, so the shorthand selected, not merely matched
    assertThat(unframe(console))
        .containsSubsequence(
            "Detected but not selected:",
            " - org.hyperledger.besu.tests.acceptance.plugins.TestBesuEventsPlugin");
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
    node = nodeWith(List.of("discoveryTestPlugins"), List.of(BROKEN_CONSTRUCTOR_PLUGIN));

    assertStartupFails(
        node,
        "Error instantiating plugin of type " + BROKEN_CONSTRUCTOR_PLUGIN,
        "I was told to fail in the constructor");
  }

  @Test
  public void selectedPluginWhoseStaticInitializerFailsStopsTheNodeNamingTheCause()
      throws IOException {
    node = nodeWith(List.of("discoveryTestPlugins"), List.of(BROKEN_STATIC_INITIALIZER_PLUGIN));

    assertStartupFails(
        node,
        "Error instantiating plugin of type " + BROKEN_STATIC_INITIALIZER_PLUGIN,
        "I was told to fail in the static initializer");
  }

  @Test
  public void selectedPluginWithAMissingDependencyStopsTheNodeNamingItsJarAndTheMissingClass()
      throws IOException {
    node = nodeWith(List.of("discoveryTestPlugins"), List.of(MISSING_DEPENDENCY_PLUGIN));

    assertStartupFails(
        node,
        "Plugin of type "
            + MISSING_DEPENDENCY_PLUGIN
            + " (declared by discoveryTestPlugins.jar) needs class"
            + " org/junit/jupiter/api/function/Executable, which is not available;"
            + " a dependency may be missing from the plugin");
  }

  @Test
  public void declaredPluginWhoseClassIsMissingStopsTheNodeNamingItAndItsJar() throws IOException {
    node = nodeWith(List.of("testPlugins"), List.of());
    final Path pluginsDir = node.homeDirectory().resolve("plugins");
    writeJarDeclaring(pluginsDir.resolve("broken.jar"), "com.acme.MissingPlugin");

    assertStartupFails(
        node, "Error loading plugin of type com.acme.MissingPlugin (declared by broken.jar)");
  }

  @Test
  public void selectedPluginWithoutAConstructorWithoutArgumentsStopsTheNodeNamingItAndItsJar()
      throws IOException {
    node = nodeWith(List.of("discoveryTestPlugins"), List.of(NO_DEFAULT_CONSTRUCTOR_PLUGIN));

    assertStartupFails(
        node,
        "Error loading plugin of type "
            + NO_DEFAULT_CONSTRUCTOR_PLUGIN
            + " (declared by discoveryTestPlugins.jar)",
        "java.lang.NoSuchMethodException: " + NO_DEFAULT_CONSTRUCTOR_PLUGIN + ".<init>()");
  }

  @Test
  public void unloadablePluginThatNoJarOfThePluginsDirectoryDeclaresStopsTheNodeNamingIt()
      throws IOException {
    node = nodeWith(List.of("testPlugins"), List.of(MAIN_PLUGIN));
    final Path pluginsDir = node.homeDirectory().resolve("plugins");
    // an unloadable plugin that is not selected, so by itself it would be ignored; its jar sorts
    // first, so the service loader reports it first
    writeJarDeclaring(pluginsDir.resolve("broken.jar"), "com.acme.MissingPlugin");
    // the manifest of linking.jar puts lib/library.jar on the plugin class path, so the service
    // loader also reports the plugin of a jar that is not in the plugins directory
    Files.createDirectory(pluginsDir.resolve("lib"));
    writeJarDeclaring(pluginsDir.resolve("lib").resolve("library.jar"), "com.acme.OutsidePlugin");
    writeJarWithClassPath(pluginsDir.resolve("linking.jar"), "lib/library.jar");

    assertStartupFails(
        node,
        "Cannot load a plugin that no jar in " + pluginsDir.toAbsolutePath() + " declares",
        "Provider com.acme.OutsidePlugin not found");
  }

  @Test
  public void declaredClassThatIsNotAPluginStopsTheNodeNamingItAndItsJar() throws IOException {
    node = nodeWith(List.of("testPlugins"), List.of());
    final Path pluginsDir = node.homeDirectory().resolve("plugins");
    // a class every node has, and that is no plugin
    writeJarDeclaring(pluginsDir.resolve("notaplugin.jar"), "java.lang.String");

    assertStartupFails(
        node,
        "Error loading plugin of type java.lang.String (declared by notaplugin.jar)",
        "java.lang.String does not implement org.hyperledger.besu.plugin.BesuPlugin");
  }

  @Test
  public void jarThatCannotBeOpenedStopsTheNode() throws IOException {
    node = nodeWith(List.of("testPlugins"), List.of());
    final Path corrupt = node.homeDirectory().resolve("plugins").resolve("corrupt.jar");
    Files.writeString(corrupt, "not a zip file");

    assertStartupFails(node, "Cannot open the plugin jar " + corrupt.toAbsolutePath());
  }

  @Test
  public void hiddenFileNamedLikeAJarIsIgnored() throws IOException {
    node = nodeWith(List.of("testPlugins"), List.of(MAIN_PLUGIN));
    // the macOS tar keeps a jar's file attributes in a companion file named ._<jar name>, which
    // becomes a real file beside the jar when unpacked on Linux; it ends in .jar but is not a jar
    Files.writeString(
        node.homeDirectory().resolve("plugins").resolve("._testPlugins.jar"), "Mac OS X");

    final String console = startAndCaptureConsole(node);

    assertThat(console).contains("Registered plugin of type " + MAIN_PLUGIN + ".");
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
