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
package org.hyperledger.besu.datatypes;

import java.util.Objects;

import org.apache.tuweni.units.bigints.UInt256;

/**
 * Manages access events that are done to the stateless AccessWitness. An access mode is passed to
 * the witness representing the type of access being done on the binary trie leaf node: read, write
 * to an existing leaf (reset) or write to a non-existent leaf (set). The access mode is then
 * evaluated against structures that track previous access events, to avoid double charging of a
 * warm account or slot. What results is the **new** access event schedule that is then used for gas
 * charging.
 *
 * <p>This class can represent a leaf or a branch access event. Each leaf access event has a
 * reference to its corresponding branch event, so that accesses to leaves and branches can be
 * reverted synchronously.
 *
 * @param <T> key type for the access event
 */
public abstract class AccessEvent<T> {

  private static final long WITNESS_BRANCH_COST = 1900;
  private static final long WITNESS_CHUNK_COST = 200;
  private static final long SUBTREE_EDIT_COST = 3000;
  private static final long CHUNK_EDIT_COST = 500;
  private static final long CHUNK_FILL_COST = 6200;

  /** Bit mask for NONE */
  public static final int NONE = 0;

  /** Bit mask for BRANCH READ */
  public static final int BRANCH_READ = 1;

  /** Bit mask for BRANCH WRITE */
  public static final int BRANCH_WRITE = 2;

  /** Bit mask for LEAF READ */
  public static final int LEAF_READ = 4;

  /** Bit mask for LEAF RESET */
  public static final int LEAF_RESET = 8;

  /** Bit mask for LEAF SET */
  public static final int LEAF_SET = 16;

  /**
   * These fields comprise the key to the event, hence only these are used for computing equals and
   * hashcode. Because an instance of this object can serve as a key and a value, key and values
   * fields are packed together.
   */
  protected final T key;

  private final UInt256 index;

  /** These are the actual values for the event, they are not to be used in equals and hashcode. */
  private long accessCounter;

  private int accessFlags = NONE;

  private int hashcode;
  private boolean hashcodeIsZero = false;

  /**
   * AccessEvent only constructor.
   *
   * @param key to be used for the access event to distinguish access events.
   * @param index the index in the stateless trie used to distinguish between access events.
   */
  public AccessEvent(final T key, final UInt256 index) {
    this.key = key;
    this.index = index;
  }

  /**
   * Gets the access event for the branch in the tree associated with this access event.
   *
   * @return access event for the branch.
   */
  public abstract AccessEvent<?> getBranchEvent();

  /**
   * Checks if a branch read event has occurred.
   *
   * @return true if bit mask contains a branch read event, false otherwise
   */
  public boolean isBranchRead() {
    return (accessFlags & BRANCH_READ) != 0;
  }

  /** Adds a branch read event to the bit mask of access events. */
  public void branchRead() {
    accessFlags |= BRANCH_READ;
  }

  /**
   * Checks if a branch write event has occurred.
   *
   * @return true if bit mask contains a branch write event, false otherwise
   */
  public boolean isBranchWrite() {
    return (accessFlags & BRANCH_WRITE) != 0;
  }

  /** Adds a branch write event to the bit mask of access events. */
  public void branchWrite() {
    accessFlags |= BRANCH_WRITE;
  }

  /**
   * Checks if a leaf read event has occurred.
   *
   * @return true if bit mask contains a leaf read event, false otherwise
   */
  public boolean isLeafRead() {
    return (accessFlags & LEAF_READ) != 0;
  }

  /** Adds a leaf read event to the bit mask of access events. */
  public void leafRead() {
    accessFlags |= LEAF_READ;
  }

  /**
   * Checks if a leaf reset event has occurred.
   *
   * @return true if bit mask contains a leaf reset event, false otherwise
   */
  public boolean isLeafReset() {
    return (accessFlags & LEAF_RESET) != 0;
  }

  /** Adds a leaf reset event to the bit mask of access events. */
  public void leafReset() {
    accessFlags |= LEAF_RESET;
  }

  /**
   * Checks if a leaf set event has occurred.
   *
   * @return true if bit mask contains a leaf set event, false otherwise
   */
  public boolean isLeafSet() {
    return isLeafSet(accessFlags);
  }

  /**
   * Checks if an access mode has the bit set for editing leaves.
   *
   * @param accessMode bitmap of the access mode to check.
   * @return true if the given access mode has the LEAF_SET bit turned on.
   */
  public static boolean isLeafSet(final int accessMode) {
    return (accessMode & LEAF_SET) != 0;
  }

  /** Adds a leaf set event to the bit mask of access events. */
  public void leafSet() {
    accessFlags |= LEAF_SET;
  }

  /**
   * Copies all the access flags from another access event. Bits from the other access flag will be
   * set on this instance.
   *
   * @param other access event to copy the bitmap from.
   */
  public void mergeFlags(final AccessEvent<?> other) {
    accessFlags |= other.accessFlags;
  }

  /**
   * Checks if a write event has occurred.
   *
   * @param accessMode bit mask of all access events so far
   * @return true if bit mask contains any of the write events, false otherwise
   */
  public static boolean isWrite(final int accessMode) {
    return (accessMode & BRANCH_WRITE) != 0
        || (accessMode & LEAF_SET) != 0
        || (accessMode & LEAF_RESET) != 0;
  }

  /**
   * The branch read event cost.
   *
   * @return cost of a branch read event.
   */
  public static long getBranchReadCost() {
    return WITNESS_BRANCH_COST;
  }

  /**
   * The leaf read event cost.
   *
   * @return cost of a leaf read event.
   */
  public static long getLeafReadCost() {
    return WITNESS_CHUNK_COST;
  }

  /**
   * The branch write event cost.
   *
   * @return cost of a branch write event.
   */
  public static long getBranchWriteCost() {
    return SUBTREE_EDIT_COST;
  }

  /**
   * The leaf reset event cost.
   *
   * @return cost of a leaf reset event.
   */
  public static long getLeafResetCost() {
    return CHUNK_EDIT_COST;
  }

  /**
   * The leaf set event cost.
   *
   * @return cost of a leaf set event.
   */
  public static long getLeafSetCost() {
    return CHUNK_FILL_COST;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    } else if (o instanceof AccessEvent<?> other) {
      return Objects.equals(key, other.key) && Objects.equals(index, other.index);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = this.hashcode;
    if (hash == 0 && !hashcodeIsZero) {
      hash = Objects.hash(key, index);
    }
    if (hash == 0) {
      this.hashcodeIsZero = true;
    } else {
      this.hashcode = hash;
    }
    return hash;
  }

  /**
   * Increments the access counter for this event, meaning that the witness was touched by this
   * event. This is useful for reverting back witnesses.
   */
  public void seenAccess() {
    accessCounter++;
  }

  /**
   * Decrements the access counter in a case of reverting the access to the witness through this
   * event.
   *
   * @return the current access counter of this event.
   */
  public long rollbackAccessAndGet() {
    return --accessCounter;
  }

  /**
   * Returns the cost schedule of this access for debugging purposes.
   *
   * @return the string with the cost schedule
   */
  public abstract String costSchedulePrettyPrint();

  /**
   * Get the key that is associated with this access event.
   *
   * @return the type T that represents the key.
   */
  public T getKey() {
    return key;
  }

  /**
   * Get the stateless tree index corresponding to this access event.
   *
   * @return an unsigned int representing the index.
   */
  public UInt256 getIndex() {
    return index;
  }
}
