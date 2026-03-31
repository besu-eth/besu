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
package org.hyperledger.besu.evm.gascalculator;

import static org.hyperledger.besu.datatypes.Address.BLS12_MAP_FP2_TO_G2;

import org.hyperledger.besu.datatypes.AccessEvent;

/**
 * Gas Calculator for BinTrie (EIP-4762 Stateless Gas Costs)
 *
 * <p>Implements the new gas schedule for Binary Trie based state access:
 *
 * <ul>
 *   <li>WITNESS_BRANCH_COST = 1900 - Cost to access a new branch (stem)
 *   <li>WITNESS_CHUNK_COST = 200 - Cost to access a new leaf (chunk)
 *   <li>SUBTREE_EDIT_COST = 3000 - Cost to write to a new branch
 *   <li>CHUNK_EDIT_COST = 500 - Cost to reset/modify an existing leaf
 *   <li>CHUNK_FILL_COST = 6200 - Cost to fill a previously empty leaf
 * </ul>
 *
 * <p>These costs are defined in EIP-4762 and are used for stateless gas accounting where access
 * witnesses track which parts of the state tree have been accessed.
 *
 * @see AccessEvent for the gas cost constants
 */
public class BinTrieGasCalculator extends OsakaGasCalculator {

  /** Instantiates a new BinTrie Gas Calculator. */
  public BinTrieGasCalculator() {
    super(BLS12_MAP_FP2_TO_G2.getBytes().toArrayUnsafe()[19]);
  }

  /**
   * Instantiates a new BinTrie Gas Calculator
   *
   * @param maxPrecompile the max precompile
   */
  protected BinTrieGasCalculator(final int maxPrecompile) {
    super(maxPrecompile);
  }

  /**
   * Returns the cost to read a new branch (stem) in the Binary Trie.
   *
   * @return the witness branch cost (1900)
   */
  public long getWitnessBranchCost() {
    return AccessEvent.getBranchReadCost();
  }

  /**
   * Returns the cost to read a new leaf (chunk) in the Binary Trie.
   *
   * @return the witness chunk cost (200)
   */
  public long getWitnessChunkCost() {
    return AccessEvent.getLeafReadCost();
  }

  /**
   * Returns the cost to write to a new branch (subtree edit).
   *
   * @return the subtree edit cost (3000)
   */
  public long getSubtreeEditCost() {
    return AccessEvent.getBranchWriteCost();
  }

  /**
   * Returns the cost to reset/modify an existing leaf.
   *
   * @return the chunk edit cost (500)
   */
  public long getChunkEditCost() {
    return AccessEvent.getLeafResetCost();
  }

  /**
   * Returns the cost to fill a previously empty leaf.
   *
   * @return the chunk fill cost (6200)
   */
  public long getChunkFillCost() {
    return AccessEvent.getLeafSetCost();
  }

  /**
   * Calculates the gas cost for accessing an account in the Binary Trie. This includes both the
   * branch access cost and the basic data leaf access cost.
   *
   * @param isNewBranch true if this is the first access to this stem/branch
   * @param isNewLeaf true if this is the first access to this leaf
   * @return the gas cost for the account access
   */
  public long accountAccessGasCost(final boolean isNewBranch, final boolean isNewLeaf) {
    long cost = 0;
    if (isNewBranch) {
      cost += getWitnessBranchCost();
    }
    if (isNewLeaf) {
      cost += getWitnessChunkCost();
    }
    return cost;
  }

  /**
   * Calculates the gas cost for writing to an account in the Binary Trie.
   *
   * @param isNewBranch true if this creates a new branch
   * @param wasEmpty true if the leaf was previously empty
   * @return the gas cost for the account write
   */
  public long accountWriteGasCost(final boolean isNewBranch, final boolean wasEmpty) {
    long cost = 0;
    if (isNewBranch) {
      cost += getSubtreeEditCost();
    }
    if (wasEmpty) {
      cost += getChunkFillCost();
    } else {
      cost += getChunkEditCost();
    }
    return cost;
  }

  /**
   * Calculates the gas cost for accessing a storage slot in the Binary Trie.
   *
   * @param isNewBranch true if this is the first access to this storage stem
   * @param isNewLeaf true if this is the first access to this storage leaf
   * @return the gas cost for the storage access
   */
  public long storageAccessGasCost(final boolean isNewBranch, final boolean isNewLeaf) {
    return accountAccessGasCost(isNewBranch, isNewLeaf);
  }

  /**
   * Calculates the gas cost for writing to a storage slot in the Binary Trie.
   *
   * @param isNewBranch true if this creates a new storage branch
   * @param wasEmpty true if the storage slot was previously empty
   * @return the gas cost for the storage write
   */
  public long storageWriteGasCost(final boolean isNewBranch, final boolean wasEmpty) {
    return accountWriteGasCost(isNewBranch, wasEmpty);
  }

  /**
   * Calculates the gas cost for accessing code in the Binary Trie.
   *
   * @param isNewBranch true if this is the first access to this code stem
   * @param codeChunks the number of code chunks being accessed
   * @return the gas cost for the code access
   */
  public long codeAccessGasCost(final boolean isNewBranch, final int codeChunks) {
    long cost = 0;
    if (isNewBranch) {
      cost += getWitnessBranchCost();
    }
    cost += (long) codeChunks * getWitnessChunkCost();
    return cost;
  }
}
