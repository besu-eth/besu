/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.services;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.CoreConfiguration;
import org.hyperledger.besu.plugin.services.BlockchainService;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.StorageService;
import org.hyperledger.besu.plugin.services.p2p.P2PService;
import org.hyperledger.besu.plugin.services.p2p.PeerEventService;
import org.hyperledger.besu.plugin.services.query.BftQueryService;
import org.hyperledger.besu.plugin.services.sync.SyncEventService;
import org.hyperledger.besu.plugin.services.sync.SynchronizationService;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.security.CodeSource;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Proves that the per-phase contexts reject a wrong-phase lookup at compile time.
 *
 * <p>A test cannot contain code that does not compile, so each case is a source snippet handed to
 * the Java compiler at test time. The legal lookups are compiled the same way, to show the bounds
 * reject the wrong phase and nothing else.
 */
class PhaseContextBoundsTest {

  @TempDir Path outputDir;

  private static final String IMPORTS =
"""
      import org.hyperledger.besu.plugin.CoreConfiguration;
      import org.hyperledger.besu.plugin.RegistrationContext;
      import org.hyperledger.besu.plugin.RunningContext;
      import org.hyperledger.besu.plugin.StartContext;
      import org.hyperledger.besu.plugin.services.BlockchainService;
      import org.hyperledger.besu.plugin.services.MetricsSystem;
      import org.hyperledger.besu.plugin.services.StorageService;
      import org.hyperledger.besu.plugin.services.p2p.P2PService;
import org.hyperledger.besu.plugin.services.p2p.PeerEventService;
      import org.hyperledger.besu.plugin.services.query.BftQueryService;
import org.hyperledger.besu.plugin.services.sync.SyncEventService;
import org.hyperledger.besu.plugin.services.sync.SynchronizationService;
      """;

  /** The jars and class directories holding the plugin API types the snippets refer to. */
  private static String classpath() {
    final Set<String> entries = new LinkedHashSet<>();
    Stream.of(
            BesuPlugin.class,
            CoreConfiguration.class,
            BlockchainService.class,
            MetricsSystem.class,
            StorageService.class,
            P2PService.class,
            PeerEventService.class,
            SyncEventService.class,
            SynchronizationService.class,
            BftQueryService.class)
        .map(Class::getProtectionDomain)
        .map(java.security.ProtectionDomain::getCodeSource)
        .map(CodeSource::getLocation)
        .map(location -> new File(URI.create(location.toString())).getAbsolutePath())
        .forEach(entries::add);
    entries.add(System.getProperty("java.class.path"));
    return String.join(File.pathSeparator, entries);
  }

  /** Compiles a method body that has the three contexts in scope, returning the errors. */
  private List<String> compile(final String body) {
    final String source =
        IMPORTS
            + "class Snippet {\n"
            + "  void run(RegistrationContext registration, StartContext start,"
            + " RunningContext running) {\n"
            + body
            + "\n  }\n}\n";
    final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    assertThat(compiler).as("the tests run on a JDK").isNotNull();
    final DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
    final JavaFileObject file =
        new SimpleJavaFileObject(URI.create("string:///Snippet.java"), JavaFileObject.Kind.SOURCE) {
          @Override
          public CharSequence getCharContent(final boolean ignoreEncodingErrors) {
            return source;
          }
        };
    compiler
        .getTask(
            null,
            null,
            diagnostics,
            List.of("-proc:none", "-classpath", classpath(), "-d", outputDir.toString()),
            null,
            List.of(file))
        .call();
    return diagnostics.getDiagnostics().stream()
        .filter(diagnostic -> diagnostic.getKind() == Diagnostic.Kind.ERROR)
        .map(diagnostic -> diagnostic.getMessage(Locale.ROOT))
        .collect(Collectors.toList());
  }

  private void assertRejected(final String body) {
    final List<String> errors = compile(body);
    assertThat(errors).as("errors compiling: %s", body).isNotEmpty();
    assertThat(errors).allSatisfy(error -> assertThat(error).contains("getBesuService"));
  }

  private void assertAccepted(final String body) {
    assertThat(compile(body)).as("errors compiling: %s", body).isEmpty();
  }

  @Test
  void registrationContextRejectsAStartService() {
    assertRejected("registration.getBesuService(BlockchainService.class);");
    assertRejected("registration.getBesuService(MetricsSystem.class);");
  }

  @Test
  void registrationContextRejectsARunningService() {
    assertRejected("registration.getBesuService(P2PService.class);");
  }

  @Test
  void startContextRejectsARegistrationService() {
    assertRejected("start.getBesuService(StorageService.class);");
  }

  @Test
  void startContextRejectsARunningService() {
    assertRejected("start.getBesuService(P2PService.class);");
    assertRejected("start.getBesuService(SynchronizationService.class);");
  }

  @Test
  void eventHalvesOfSyncAndP2pAreStartServicesWhileTheirParentsAreNot() {
    // a subscription must be in place before the main loop starts the component
    assertAccepted(
        """
        SyncEventService syncEvents = start.getBesuService(SyncEventService.class);
        PeerEventService peerEvents = start.getBesuService(PeerEventService.class);
        """);
    assertRejected("registration.getBesuService(SyncEventService.class);");
    assertRejected("registration.getBesuService(PeerEventService.class);");
  }

  @Test
  void runningContextRejectsARegistrationService() {
    assertRejected("running.getBesuService(StorageService.class);");
  }

  @Test
  void registrationContextAcceptsRegistrationServicesAndConfigurationViews() {
    assertAccepted(
        """
        StorageService storage = registration.getBesuService(StorageService.class);
        CoreConfiguration configuration = registration.getBesuService(CoreConfiguration.class);
        """);
  }

  @Test
  void startContextAcceptsStartServicesAndConfigurationViews() {
    assertAccepted(
        """
        BlockchainService blockchain = start.getBesuService(BlockchainService.class);
        MetricsSystem metrics = start.getBesuService(MetricsSystem.class);
        CoreConfiguration configuration = start.getBesuService(CoreConfiguration.class);
        BftQueryService bftQueries = start.getBesuService(BftQueryService.class);
        """);
  }

  @Test
  void runningContextAcceptsRunningServicesAndEveryStartService() {
    assertAccepted(
        """
        P2PService p2p = running.getBesuService(P2PService.class);
        BlockchainService blockchain = running.getBesuService(BlockchainService.class);
        CoreConfiguration configuration = running.getBesuService(CoreConfiguration.class);
        """);
  }
}
