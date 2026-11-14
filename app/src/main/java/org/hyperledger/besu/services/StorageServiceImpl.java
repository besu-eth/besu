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
package org.hyperledger.besu.services;

import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.plugin.RegistrationClosedException;
import org.hyperledger.besu.plugin.services.StorageService;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageFactory;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import javax.inject.Inject;

/** The Storage service implementation. */
public class StorageServiceImpl implements StorageService {

  /** The registrant of an entry Besu registered itself, outside any plugin's register(). */
  private static final String BESU = "Besu";

  /** A registered factory with the id of the plugin that registered it. */
  private record Registered(String registrant, KeyValueStorageFactory factory) {}

  private final List<SegmentIdentifier> segments;
  private final Map<String, Registered> factories;
  private final BooleanSupplier registrationOpen;
  private final Supplier<Optional<String>> currentRegistrant;

  /** Instantiates a new Storage service that accepts registrations at any time. */
  @Inject
  public StorageServiceImpl() {
    this(() -> true);
  }

  /**
   * Instantiates a new Storage service whose registrations all count as Besu's own.
   *
   * @param registrationOpen whether plugin registration is currently open; registrations made while
   *     it is closed are rejected
   */
  public StorageServiceImpl(final BooleanSupplier registrationOpen) {
    this(registrationOpen, Optional::empty);
  }

  /**
   * Instantiates a new Storage service.
   *
   * @param registrationOpen whether plugin registration is currently open; registrations made while
   *     it is closed are rejected
   * @param currentRegistrant the id of the plugin whose register() is running, empty when Besu
   *     itself is registering; a factory name already registered by another registrant is rejected
   */
  public StorageServiceImpl(
      final BooleanSupplier registrationOpen, final Supplier<Optional<String>> currentRegistrant) {
    this.segments = List.of(KeyValueSegmentIdentifier.values());
    this.factories = new ConcurrentHashMap<>();
    this.registrationOpen = registrationOpen;
    this.currentRegistrant = currentRegistrant;
  }

  @Override
  public void registerKeyValueStorage(final KeyValueStorageFactory factory) {
    if (!registrationOpen.getAsBoolean()) {
      throw new RegistrationClosedException("StorageService", "registerKeyValueStorage");
    }
    final String registrant = currentRegistrant.get().orElse(BESU);
    final Registered previous = factories.get(factory.getName());
    if (previous != null && !previous.registrant().equals(registrant)) {
      throw new IllegalStateException(
          "cannot register storage factory "
              + factory.getName()
              + ": "
              + owner(previous.registrant())
              + " already registered it");
    }
    factories.put(factory.getName(), new Registered(registrant, factory));
  }

  private static String owner(final String registrant) {
    return BESU.equals(registrant) ? BESU : "plugin " + registrant;
  }

  @Override
  public List<SegmentIdentifier> getAllSegmentIdentifiers() {
    return segments;
  }

  @Override
  public Optional<KeyValueStorageFactory> getByName(final String name) {
    return Optional.ofNullable(factories.get(name)).map(Registered::factory);
  }
}
