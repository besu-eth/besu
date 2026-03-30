/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.plugin.services.storage.rocksdb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses RocksDB option strings as semicolon-separated {@code key=value} pairs passed to the native
 * option parser via {@link org.rocksdb.ColumnFamilyOptions#getColumnFamilyOptionsFromProps} and
 * {@link org.rocksdb.DBOptions#getDBOptionsFromProps}.
 *
 * <p>Additional column-family strings are parsed here; {@link
 * org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.RocksDBColumnarKeyValueStorage}
 * merges Besu defaults using {@link InsertionOrderedProperties} so JNI builds a deterministic
 * option string (standard {@link Properties} iteration order is undefined).
 *
 * <p>Options are applied through RocksDB's native option parser ({@code
 * GetColumnFamilyOptionsFromString} / block-table factory configuration), not only through
 * rocksdbjni Java setters. That means column-family and block-table settings that are valid in
 * RocksDB's C++ option map but missing or incomplete on {@link org.rocksdb.BlockBasedTableConfig}
 * (or other Java wrappers) can still be supplied via {@code
 * --Xplugin-rocksdb-additional-column-family-options}, as long as the key names match the native
 * option registry for the linked RocksDB version.
 */
public final class RocksDbNativeOptionStrings {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDbNativeOptionStrings.class);

  private RocksDbNativeOptionStrings() {}

  /**
   * {@link Properties} whose {@link #stringPropertyNames()} follows insertion order, so {@link
   * org.rocksdb.Options#getOptionStringFromProps} emits a stable option string for JNI.
   */
  public static final class InsertionOrderedProperties extends Properties {
    private final List<String> insertionOrder = new ArrayList<>();
    private final Set<String> seenKeys = new HashSet<>();

    @Override
    public synchronized Object put(final Object key, final Object value) {
      if (key instanceof final String s && seenKeys.add(s)) {
        insertionOrder.add(s);
      }
      return super.put(key, value);
    }

    @Override
    public Set<String> stringPropertyNames() {
      synchronized (this) {
        final LinkedHashSet<String> names = new LinkedHashSet<>();
        for (final String k : insertionOrder) {
          if (containsKey(k)) {
            names.add(k);
          }
        }
        return names;
      }
    }
  }

  /**
   * Parses a DB options string for {@link org.rocksdb.DBOptions#getDBOptionsFromProps}.
   *
   * @param raw semicolon-separated {@code key=value} segments; may be null or blank
   * @return properties suitable for {@code getDBOptionsFromProps}; empty if {@code raw} is null or
   *     blank
   */
  public static Properties parseDbOptionString(final String raw) {
    return parseSemicolonKeyValueString(raw);
  }

  /**
   * Parses a semicolon-separated options string ({@code a=b;c=d;}) into flat {@link Properties}
   * keys.
   *
   * @param raw semicolon-separated {@code key=value} segments; may be null or blank
   * @return parsed properties; malformed segments are skipped with a warning
   */
  public static Properties parseSemicolonKeyValueString(final String raw) {
    final Properties props = new Properties();
    if (raw == null || raw.isBlank()) {
      return props;
    }
    for (final String segment : Splitter.on(';').split(raw)) {
      final String part = segment.trim();
      if (part.isEmpty()) {
        continue;
      }
      final int eq = part.indexOf('=');
      if (eq <= 0 || eq == part.length() - 1) {
        LOG.warn("Ignoring malformed RocksDB option segment (expected key=value): {}", part);
        continue;
      }
      final String key = part.substring(0, eq).trim();
      final String value = part.substring(eq + 1).trim();
      props.setProperty(key, value);
    }
    return props;
  }
}
