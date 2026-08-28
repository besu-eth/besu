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
package org.hyperledger.besu.plugin.services.txselection;

import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * Manages the state of transaction selectors (including the plugin transaction selector {@link
 * PluginTransactionSelector}) during the block creation process. Some selectors have a state, for
 * example the amount of gas used by selected pending transactions so far, and changes made to the
 * state must be only committed after the evaluated pending transaction has been definitely selected
 * for inclusion, until that point it will be always possible to rollback the changes to the state
 * and return the previous committed state.
 */
@SuppressWarnings("rawtypes")
public class SelectorsStateManager {
  private final List<Map<TransactionSelector, SelectorState<?, ? extends StateDuplicator<?>>>>
      uncommitedStates = new ArrayList<>();
  private Map<TransactionSelector, SelectorState<?, ? extends StateDuplicator<?>>> committedState =
      new HashMap<>();
  private volatile boolean blockSelectionStarted = false;

  /** Create an empty selectors state manager, here to make javadoc linter happy. */
  public SelectorsStateManager() {}

  /**
   * Create, initialize and track the state for a selector.
   *
   * <p>Call to this method must be performed before the block selection is started with {@link
   * #blockSelectionStarted()}, otherwise it fails.
   *
   * @param selector the selector
   * @param initialState the initial value of the state
   * @param duplicator the state duplicator
   * @param <S> the type of the selector state
   * @param <D> the type of the state duplicator
   */
  public <S, D extends StateDuplicator<S>> void createSelectorState(
      final TransactionSelector selector, final S initialState, final D duplicator) {
    checkState(
        !blockSelectionStarted, "Cannot create selector state after block selection is started");
    committedState.put(selector, new SelectorState<>(initialState, duplicator));
  }

  /**
   * Called at the start of block selection, when the initialization is done, to prepare a new
   * working state based on the initial state.
   *
   * <p>After this method is called, it is not possible to call anymore {@link
   * #createSelectorState(TransactionSelector, Object, StateDuplicator)}
   */
  public void blockSelectionStarted() {
    blockSelectionStarted = true;
    uncommitedStates.add(duplicateLastState());
  }

  private Map<TransactionSelector, SelectorState<?, ? extends StateDuplicator<?>>>
      duplicateLastState() {
    return getLast().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().duplicated()));
  }

  /**
   * Get the working state for the specified selector
   *
   * @param selector the selector
   * @return the working state of the selector
   * @param <T> the type of the selector state
   */
  @SuppressWarnings("unchecked")
  public <T> T getSelectorWorkingState(final TransactionSelector selector) {
    return (T) uncommitedStates.getLast().get(selector).get();
  }

  /**
   * set the working state for the specified selector
   *
   * @param selector the selector
   * @param newState the new state
   * @param <T> the type of the selector state
   */
  @SuppressWarnings("unchecked")
  public <T> void setSelectorWorkingState(final TransactionSelector selector, final T newState) {
    ((SelectorState<T, StateDuplicator<T>>) uncommitedStates.getLast().get(selector)).set(newState);
  }

  /**
   * Get the committed state for the specified selector
   *
   * @param selector the selector
   * @return the committed state of the selector
   * @param <T> the type of the selector state
   */
  @SuppressWarnings("unchecked")
  public <T> T getSelectorCommittedState(final TransactionSelector selector) {
    return (T) committedState.get(selector).get();
  }

  /**
   * Commit the current working state and prepare a new working state based on the just committed
   * state
   */
  public void commit() {
    committedState = getLast();
    uncommitedStates.clear();
    uncommitedStates.add(duplicateLastState());
  }

  /**
   * Discards the current working state and prepare a new working state based on the just committed
   * state
   */
  public void rollback() {
    uncommitedStates.clear();
    uncommitedStates.add(duplicateLastState());
  }

  private Map<TransactionSelector, SelectorState<?, ? extends StateDuplicator<?>>> getLast() {
    if (uncommitedStates.isEmpty()) {
      return committedState;
    }
    return uncommitedStates.getLast();
  }

  /**
   * A function that create a duplicate of the input object. The duplication must be a deep copy.
   *
   * @param <T> the type of the object
   */
  @FunctionalInterface
  public interface StateDuplicator<T> extends UnaryOperator<T> {
    /**
     * Duplicate the input object
     *
     * @param t the input object to duplicate
     * @return a deep copy of the input object
     */
    T duplicate(T t);

    @Override
    default T apply(final T t) {
      return duplicate(t);
    }

    /**
     * Utility to duplicate a long
     *
     * @param l a long
     * @return a copy of the long
     */
    static long duplicateLong(final long l) {
      return l;
    }
  }

  /**
   * A selector state object is one that is able to return, update and duplicate the state it
   * contains
   *
   * @param <S> the type of the state
   */
  private static class SelectorState<S, D extends StateDuplicator<S>> {
    private final D duplicator;
    private S state;

    /**
     * Create a selector state with the initial value
     *
     * @param initialState the initial initialState
     */
    public SelectorState(final S initialState, final D duplicator) {
      this.state = initialState;
      this.duplicator = duplicator;
    }

    /**
     * The method that concrete classes must implement to create a deep copy of the state
     *
     * @return a new selector state with a deep copy of the state
     */
    private SelectorState<S, D> duplicated() {
      return new SelectorState<>(duplicator.duplicate(state), duplicator);
    }

    /**
     * Get the current state
     *
     * @return the current state
     */
    public S get() {
      return state;
    }

    /**
     * Replace the current state with the new one
     *
     * @param newState the new state
     */
    public void set(final S newState) {
      this.state = newState;
    }
  }
}
