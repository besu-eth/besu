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
package org.hyperledger.besu.util;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import com.google.common.collect.Maps;

/**
 * The named entries of a keyed registration service, each with the registrant that made it: the id
 * of the plugin whose {@code register()} is running, or {@value #BESU} for an entry Besu registers
 * itself. A name belongs to the registrant that registered it first. The same registrant may
 * register it again, replacing its entry, which the Ephemery restart relies on; another registrant
 * may not: {@link #register} rejects it naming both, and {@link #replace} lets it through while
 * reporting the previous owner, for the services whose contract is to replace.
 *
 * @param <V> the type of the registered values
 */
public final class KeyedRegistrations<V> {
  /** The registrant of an entry Besu registered itself, outside any plugin's register(). */
  public static final String BESU = "Besu";

  /** A registered value with the id of the registrant that registered it. */
  private record Registered<V>(String registrant, V value) {}

  private final String kind;
  private final Supplier<Optional<String>> currentRegistrant;
  private final UnaryOperator<String> keyOf;
  private final Map<String, Registered<V>> entries = new ConcurrentHashMap<>();

  /**
   * Creates registrations keyed by the names as given.
   *
   * @param kind what the names denote, as the error names it, such as {@code storage factory}
   * @param currentRegistrant the id of the plugin whose register() is running, empty when Besu
   *     itself is registering
   */
  public KeyedRegistrations(final String kind, final Supplier<Optional<String>> currentRegistrant) {
    this(kind, currentRegistrant, UnaryOperator.identity());
  }

  /**
   * Creates registrations keyed by a normalized form of the names.
   *
   * @param kind what the names denote, as the error names it, such as {@code metric category}
   * @param currentRegistrant the id of the plugin whose register() is running, empty when Besu
   *     itself is registering
   * @param keyOf the normalization every name goes through before it is stored or looked up, such
   *     as upper-casing; errors still print the name as given
   */
  public KeyedRegistrations(
      final String kind,
      final Supplier<Optional<String>> currentRegistrant,
      final UnaryOperator<String> keyOf) {
    this.kind = kind;
    this.currentRegistrant = currentRegistrant;
    this.keyOf = keyOf;
  }

  /**
   * Registers a value under a name for the current registrant, replacing that registrant's own
   * earlier entry.
   *
   * @param name the name
   * @param value the value
   * @throws IllegalStateException if another registrant already registered that name
   */
  public void register(final String name, final V value) {
    final String registrant = registrant();
    // the check and the claim run under the map's lock for the key, so two registrants racing for
    // the same name cannot both pass the check; a throw leaves the entry as it was
    entries.compute(
        keyOf.apply(name),
        (key, previous) -> {
          if (previous != null && !previous.registrant().equals(registrant)) {
            throw new IllegalStateException(
                "cannot register "
                    + kind
                    + " "
                    + name
                    + ": "
                    + owner(previous.registrant())
                    + " already registered it");
          }
          return new Registered<>(registrant, value);
        });
  }

  /**
   * Registers a value under a name for the current registrant, replacing whatever entry the name
   * had, whoever registered it.
   *
   * @param name the name
   * @param value the value
   * @return the owner of the replaced entry, as {@link #currentOwner()} prints it, when it was
   *     another registrant's; empty when the name was free or the current registrant's own
   */
  public Optional<String> replace(final String name, final V value) {
    final String registrant = registrant();
    final Registered<V> previous =
        entries.put(keyOf.apply(name), new Registered<>(registrant, value));
    return previous != null && !previous.registrant().equals(registrant)
        ? Optional.of(owner(previous.registrant()))
        : Optional.empty();
  }

  /**
   * Registers a value under a name as Besu's own, whoever is registering, replacing any entry.
   *
   * @param name the name
   * @param value the value
   */
  public void registerAsBesu(final String name, final V value) {
    entries.put(keyOf.apply(name), new Registered<>(BESU, value));
  }

  /**
   * Removes the entry of a name, if any.
   *
   * @param name the name
   */
  public void remove(final String name) {
    entries.remove(keyOf.apply(name));
  }

  /**
   * Looks a name up.
   *
   * @param name the name
   * @return the registered value, empty when the name is free
   */
  public Optional<V> get(final String name) {
    return Optional.ofNullable(entries.get(keyOf.apply(name))).map(Registered::value);
  }

  /**
   * The registrant recorded for a name: a plugin id, or {@link #BESU} when Besu registered it.
   *
   * @param name the name, as given at registration
   * @return the registrant, or empty when nothing is registered under the name
   */
  public Optional<String> registrantOf(final String name) {
    return Optional.ofNullable(entries.get(keyOf.apply(name))).map(Registered::registrant);
  }

  /**
   * Whether a name is registered.
   *
   * @param name the name
   * @return true when an entry exists for it
   */
  public boolean contains(final String name) {
    return entries.containsKey(keyOf.apply(name));
  }

  /**
   * The registered names, in their stored form.
   *
   * @return an unmodifiable live view of the names
   */
  public Set<String> names() {
    return Collections.unmodifiableSet(entries.keySet());
  }

  /**
   * The registered values by their stored name.
   *
   * @return an unmodifiable live view of the entries
   */
  public Map<String, V> byName() {
    return Collections.unmodifiableMap(Maps.transformValues(entries, Registered::value));
  }

  /**
   * The current registrant as the errors and logs name it.
   *
   * @return {@value #BESU}, or {@code plugin } followed by the plugin's id
   */
  public String currentOwner() {
    return owner(registrant());
  }

  private String registrant() {
    return currentRegistrant.get().orElse(BESU);
  }

  private static String owner(final String registrant) {
    return BESU.equals(registrant) ? BESU : "plugin " + registrant;
  }
}
