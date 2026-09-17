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
package org.hyperledger.besu.collections.undo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.Table;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.jspecify.annotations.Nullable;

/**
 * A Table that supports rolling back the Table to a prior state.
 *
 * <p>To register a prior state you want to roll back to call `mark()`. Then use that value in a
 * subsequent call to `undo(mark)`. Every mutation operation across all undoable collections
 * increases the global mark, so a mark set in once collection is usable across all
 * UndoableCollection instances.
 *
 * @param <R> the type of the table row keys
 * @param <C> the type of the table column keys
 * @param <V> the type of the mapped values
 */
public class UndoTable<R, C, V> implements Table<R, C, V>, Undoable {

  record UndoEntry<R, C, V>(R row, C column, V value, long level) {
    UndoEntry(final R row, final C column, final V value) {
      this(row, column, value, Undoable.incrementMarkStatic());
    }
  }

  Table<R, C, V> delegate;
  List<UndoEntry<R, C, V>> undoLog;

  /**
   * Create an UndoTable backed by another Table instance.
   *
   * @param table The Table instance to use for backing storage
   * @param <R> The row type
   * @param <C> The column type
   * @param <V> The value type
   * @return a new undo table backed by the provided table.
   */
  public static <R, C, V> UndoTable<R, C, V> of(final Table<R, C, V> table) {
    return new UndoTable<>(table);
  }

  /**
   * Protected constructor for UndoTable
   *
   * @param delegate the table backing the undotable.
   */
  protected UndoTable(final Table<R, C, V> delegate) {
    this.delegate = delegate;
    undoLog = new ArrayList<>();
  }

  @Override
  public void undo(final long mark) {
    int pos = undoLog.size() - 1;
    while (pos >= 0 && undoLog.get(pos).level > mark) {
      final var entry = undoLog.get(pos);
      undoLog.remove(pos);
      if (entry.value() == null) {
        delegate.remove(entry.row(), entry.column());
      } else {
        delegate.put(entry.row(), entry.column(), entry.value());
      }
      pos--;
    }
  }

  @Override
  public long lastUpdate() {
    return undoLog.isEmpty() ? 0L : undoLog.get(undoLog.size() - 1).level;
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public boolean contains(final Object rowKey, final Object columnKey) {
    return delegate.contains(rowKey, columnKey);
  }

  @Override
  public boolean containsRow(final Object rowKey) {
    return delegate.containsRow(rowKey);
  }

  @Override
  public boolean containsColumn(final Object columnKey) {
    return delegate.containsColumn(columnKey);
  }

  @Override
  public boolean containsValue(final Object value) {
    return delegate.containsValue(value);
  }

  @Override
  @Nullable
  public V get(final Object rowKey, final Object columnKey) {
    return delegate.get(rowKey, columnKey);
  }

  @Override
  public void clear() {
    delegate.clear();
  }

  @Override
  @CanIgnoreReturnValue
  @Nullable
  public V put(final R rowKey, final C columnKey, final V value) {
    V oldV = delegate.put(rowKey, columnKey, value);
    undoLog.add(new UndoEntry<>(rowKey, columnKey, oldV));
    return oldV;
  }

  @Override
  public void putAll(final Table<? extends R, ? extends C, ? extends V> table) {
    for (var cell : table.cellSet()) {
      V newV = cell.getValue();
      V oldV = delegate.put(cell.getRowKey(), cell.getColumnKey(), newV);
      if (!Objects.equals(oldV, newV)) {
        undoLog.add(new UndoEntry<>(cell.getRowKey(), cell.getColumnKey(), oldV));
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  @CanIgnoreReturnValue
  @Nullable
  public V remove(final Object rowKey, final Object columnKey) {
    V oldV = delegate.remove(rowKey, columnKey);
    undoLog.add(new UndoEntry<>((R) rowKey, (C) columnKey, oldV));
    return oldV;
  }

  @Override
  public Map<C, V> row(final R rowKey) {
    return Collections.unmodifiableMap(delegate.row(rowKey));
  }

  @Override
  public Map<R, V> column(final C columnKey) {
    return Collections.unmodifiableMap(delegate.column(columnKey));
  }

  @Override
  public Set<Cell<R, C, V>> cellSet() {
    return Collections.unmodifiableSet(delegate.cellSet());
  }

  @Override
  public Set<R> rowKeySet() {
    return Collections.unmodifiableSet(delegate.rowKeySet());
  }

  @Override
  public Set<C> columnKeySet() {
    return Collections.unmodifiableSet(delegate.columnKeySet());
  }

  @Override
  public Collection<V> values() {
    return Collections.unmodifiableCollection(delegate.values());
  }

  @Override
  public Map<R, Map<C, V>> rowMap() {
    return Collections.unmodifiableMap(delegate.rowMap());
  }

  @Override
  public Map<C, Map<R, V>> columnMap() {
    return Collections.unmodifiableMap(delegate.columnMap());
  }

  @Override
  public boolean equals(final Object o) {
    return o instanceof UndoTable && delegate.equals(o);
  }

  @Override
  public int hashCode() {
    return delegate.hashCode() ^ 0xde1e647e;
  }
}
