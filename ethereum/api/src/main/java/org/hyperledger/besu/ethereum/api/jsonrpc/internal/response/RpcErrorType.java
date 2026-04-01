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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.response;

import org.hyperledger.besu.plugin.services.rpc.RpcMethodError;

import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;

public enum RpcErrorType implements RpcMethodError {
  // Standard errors
  PARSE_ERROR(-32700, "Parse error"),
  INVALID_REQUEST(-32600, "Invalid Request"),
  METHOD_NOT_FOUND(-32601, "Method not found"),

  INVALID_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid params"),
  INVALID_ACCOUNT_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid account params"),
  INVALID_ADDRESS_HASH_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid address hash params"),
  INVALID_ADDRESS_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid address params"),
  INVALID_BLOB_COUNT(
      INVALID_PARAMS_ERROR_CODE,
      "Invalid blob count (blob transactions must have at least one blob)"),
  INVALID_BLOB_GAS_USED_PARAMS(
      INVALID_PARAMS_ERROR_CODE, "Invalid blob gas used param (missing or invalid)"),
  INVALID_BLOCK_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid block param (block not found)"),
  INVALID_BLOCK_COUNT_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid block count params"),
  INVALID_BLOCK_HASH_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid block hash params"),
  INVALID_BLOCK_INDEX_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid block index params"),
  INVALID_BLOCK_NUMBER_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid block number params"),
  INVALID_CALL_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid call params"),
  INVALID_CONSOLIDATION_REQUEST_PARAMS(
      INVALID_PARAMS_ERROR_CODE, "Invalid consolidation request params"),
  INVALID_DATA_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid data params"),
  INVALID_DATA_HASH_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid data hash params"),
  INVALID_DEPOSIT_REQUEST_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid deposit request"),
  INVALID_ENGINE_EXCHANGE_TRANSITION_CONFIGURATION_PARAMS(
      INVALID_PARAMS_ERROR_CODE, "Invalid engine exchange transition configuration params"),
  INVALID_ENGINE_FORKCHOICE_UPDATED_PARAMS(
      INVALID_PARAMS_ERROR_CODE, "Invalid engine forkchoice updated params"),
  INVALID_ENGINE_FORKCHOICE_UPDATED_PAYLOAD_ATTRIBUTES(
      INVALID_PARAMS_ERROR_CODE, "Invalid engine payload attributes parameter"),
  INVALID_ENGINE_NEW_PAYLOAD_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid engine payload parameter"),
  INVALID_ENGINE_PREPARE_PAYLOAD_PARAMS(
      INVALID_PARAMS_ERROR_CODE, "Invalid engine prepare payload parameter"),
  INVALID_ENGINE_GET_BLOBS_TOO_LARGE_REQUEST(-38004, "Too large request"),
  INVALID_ENODE_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid enode params"),
  INVALID_EXCESS_BLOB_GAS_PARAMS(
      INVALID_PARAMS_ERROR_CODE, "Invalid excess blob gas params (missing or invalid)"),
  INVALID_EXECUTION_REQUESTS_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid execution requests params"),
  INVALID_SLOT_NUMBER_PARAMS(
      INVALID_PARAMS_ERROR_CODE, "Invalid slot number params (missing or invalid)"),
  INVALID_EXTRA_DATA_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid extra data params"),
  INVALID_FILTER_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid filter params"),
  INVALID_HASH_RATE_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid hash rate params"),
  INVALID_ID_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid ID params"),
  INVALID_RETURN_COMPLETE_TRANSACTION_PARAMS(
      INVALID_PARAMS_ERROR_CODE, "Invalid return complete transaction params"),
  INVALID_LOG_FILTER_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid log filter params"),
  INVALID_LOG_LEVEL_PARAMS(
      INVALID_PARAMS_ERROR_CODE, "Invalid log level params (missing or incorrect)"),
  INVALID_MAX_RESULTS_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid max results params"),
  INVALID_METHOD_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid method params"),
  INVALID_MIN_GAS_PRICE_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid min gas price params"),
  INVALID_MIN_PRIORITY_FEE_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid min priority fee params"),
  INVALID_MIX_HASH_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid mix hash params"),
  INVALID_NONCE_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid nonce params"),
  INVALID_PARENT_BEACON_BLOCK_ROOT_PARAMS(
      INVALID_PARAMS_ERROR_CODE, "Invalid parent beacon block root (missing or incorrect)"),
  INVALID_PARAM_COUNT(INVALID_PARAMS_ERROR_CODE, "Invalid number of params"),
  INVALID_PAYLOAD_ID_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid payload id params"),
  INVALID_PENDING_TRANSACTIONS_PARAMS(
      INVALID_PARAMS_ERROR_CODE, "Invalid pending transactions params"),
  INVALID_PLUGIN_NAME_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid plug in name params"),
  INVALID_POSITION_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid position params"),
  INVALID_POW_HASH_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid pow hash params"),
  INVALID_PRIVATE_FROM_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid private from params"),
  INVALID_PRIVATE_FOR_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid private for params"),
  INVALID_PROPOSAL_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid proposal params"),
  INVALID_REMOTE_CAPABILITIES_PARAMS(
      INVALID_PARAMS_ERROR_CODE, "Invalid remote capabilities params"),
  INVALID_REWARD_PERCENTILES_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid reward percentiles params"),
  INVALID_REQUESTS_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid requests params"),
  INVALID_SEALER_ID_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid sealer ID params"),
  INVALID_STORAGE_KEYS_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid storage keys params"),
  INVALID_SUBSCRIPTION_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid subscription params"),
  INVALID_TARGET_GAS_LIMIT_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid target gas limit params"),
  INVALID_TIMESTAMP_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid timestamp parameter"),
  INVALID_TRACE_CALL_MANY_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid trace call many params"),
  INVALID_TRACE_NUMBERS_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid trace numbers params"),
  INVALID_TRACE_TYPE_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid trace type params"),
  INVALID_TRANSACTION_PARAMS(
      INVALID_PARAMS_ERROR_CODE, "Invalid transaction params (missing or incorrect)"),
  INVALID_TRANSACTION_HASH_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid transaction hash params"),
  INVALID_TRANSACTION_ID_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid transaction id params"),
  INVALID_TRANSACTION_INDEX_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid transaction index params"),
  INVALID_TRANSACTION_LIMIT_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid transaction limit params"),
  INVALID_TRANSACTION_TRACE_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid transaction trace params"),
  INVALID_VERSIONED_HASH_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid versioned hash params"),
  INVALID_VERSIONED_HASHES_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid versioned hashes params"),
  INVALID_VOTE_TYPE_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid vote type params"),
  INVALID_WITHDRAWALS_PARAMS(INVALID_PARAMS_ERROR_CODE, "Invalid withdrawals"),

  INTERNAL_ERROR(-32603, "Internal error"),
  TIMEOUT_ERROR(-32603, "Timeout expired"),

  METHOD_NOT_ENABLED(-32604, "Method not enabled"),

  // Resource unavailable error
  TX_POOL_DISABLED(
      -32002,
      "Transaction pool not enabled. (Either txpool explicitly disabled, or node not yet in sync)."),

  // eth_getBlockByNumber specific error message
  UNKNOWN_BLOCK(-39001, "Unknown block"),
  PRUNED_HISTORY_UNAVAILABLE(4444, "Pruned history unavailable"),
  BLOCK_ACCESS_LIST_NOT_AVAILABLE_FOR_PRE_AMSTERDAM_BLOCKS(
      4445, "Block access list not available for pre-Amsterdam blocks"),

  // eth_sendTransaction specific error message
  ETH_SEND_TX_NOT_AVAILABLE(
      -32604,
      "The method eth_sendTransaction is not supported. Use eth_sendRawTransaction to send a signed transaction to Besu."),
  ETH_SEND_TX_ALREADY_KNOWN(-32000, "Known transaction"),
  ETH_SEND_TX_REPLACEMENT_UNDERPRICED(-32000, "Replacement transaction underpriced"),
  // P2P related errors
  P2P_DISABLED(-32000, "P2P has been disabled. This functionality is not available"),
  DISCOVERY_DISABLED(-32000, "Discovery has been disabled. This functionality is not available"),
  P2P_NETWORK_NOT_RUNNING(-32000, "P2P network is not running"),

  // Filter & Subscription Errors
  FILTER_NOT_FOUND(-32000, "Filter not found"),
  LOGS_FILTER_NOT_FOUND(-32000, "Logs filter not found"),
  SUBSCRIPTION_NOT_FOUND(-32000, "Subscription not found"),
  NO_MINING_WORK_FOUND(-32000, "No mining work available yet"),

  // Transaction validation failures
  NONCE_TOO_LOW(-32001, "Nonce too low"),
  INVALID_TRANSACTION_SIGNATURE(-32002, "Invalid signature"),
  INVALID_TRANSACTION_TYPE(INVALID_PARAMS_ERROR_CODE, "Invalid transaction type"),
  INTRINSIC_GAS_EXCEEDS_LIMIT(-32003, "Intrinsic gas exceeds gas limit"),
  TRANSACTION_UPFRONT_COST_EXCEEDS_BALANCE(-32004, "Upfront cost exceeds account balance"),
  EXCEEDS_BLOCK_GAS_LIMIT(-32005, "Transaction gas limit exceeds block gas limit"),
  EXCEEDS_TRANSACTION_GAS_LIMIT(-32005, "Transaction gas limit cap exceeded"),
  EXCEEDS_RPC_MAX_BLOCK_RANGE(-32005, "Requested range exceeds maximum RPC range limit"),
  EXCEEDS_RPC_MAX_BATCH_SIZE(-32005, "Number of requests exceeds max batch size"),
  NONCE_TOO_HIGH(-32006, "Nonce too high"),
  TX_SENDER_NOT_AUTHORIZED(-32007, "Sender account not authorized to send transactions"),
  CHAIN_HEAD_WORLD_STATE_NOT_AVAILABLE(-32008, "Initial sync is still in progress"),
  GAS_PRICE_TOO_LOW(-32009, "Gas price below configured minimum gas price"),
  GAS_PRICE_BELOW_CURRENT_BASE_FEE(-32009, "Gas price below current base fee"),

  BLOB_GAS_PRICE_BELOW_CURRENT_BLOB_BASE_FEE(-32009, "blob gas price below current blob base fee"),
  WRONG_CHAIN_ID(-32000, "Wrong chainId"),
  REPLAY_PROTECTED_SIGNATURES_NOT_SUPPORTED(-32000, "ChainId not supported"),
  REPLAY_PROTECTED_SIGNATURE_REQUIRED(-32000, "ChainId is required"),
  TX_FEECAP_EXCEEDED(-32000, "Transaction fee cap exceeded"),
  REVERT_ERROR(
      3,
      "Execution reverted",
      data -> JsonRpcErrorResponse.decodeRevertReason(Bytes.fromHexString(data))),
  TRANSACTION_NOT_FOUND(-32000, "Transaction not found"),
  MAX_PRIORITY_FEE_PER_GAS_EXCEEDS_MAX_FEE_PER_GAS(
      -32000, "Max priority fee per gas exceeds max fee per gas"),
  NONCE_TOO_FAR_IN_FUTURE_FOR_SENDER(
      -32000, "Transaction nonce is too distant from current sender nonce"),
  LOWER_NONCE_INVALID_TRANSACTION_EXISTS(
      -32000, "An invalid transaction with a lower nonce exists"),
  TOTAL_BLOB_GAS_TOO_HIGH(-32000, "Total blob gas too high"),
  PLUGIN_TX_VALIDATOR(-32000, "Plugin has marked the transaction as invalid"),
  PLUGIN_TX_POOL_VALIDATOR(
      -32000, "Plugin has marked the transaction as not acceptable by the transaction pool"),
  EXECUTION_HALTED(-32000, "Transaction processing could not be completed due to an exception"),

  // Execution engine failures
  UNKNOWN_PAYLOAD(-32001, "Payload does not exist / is not available"),
  INVALID_TERMINAL_BLOCK(-32002, "Terminal block doesn't satisfy terminal block conditions"),
  INVALID_FORKCHOICE_STATE(-38002, "Invalid forkchoice state"),
  INVALID_PAYLOAD_ATTRIBUTES(-38003, "Invalid payload attributes"),
  INVALID_RANGE_REQUEST_TOO_LARGE(-38004, "Too large request"),
  UNSUPPORTED_FORK(-38005, "Unsupported fork"),
  // Miner failures
  COINBASE_NOT_SET(-32010, "Coinbase not set. Unable to start mining without a coinbase"),
  NO_HASHES_PER_SECOND(-32011, "No hashes being generated by the current node"),
  TARGET_GAS_LIMIT_MODIFICATION_UNSUPPORTED(
      -32011, "The node is not attempting to target a gas limit so the target can't be changed"),

  // Wallet errors
  COINBASE_NOT_SPECIFIED(-32000, "Coinbase must be explicitly specified"),

  // Account errors
  NO_ACCOUNT_FOUND(-32000, "Account not found"),

  // Worldstate errors
  WORLD_STATE_UNAVAILABLE(-32000, "World state unavailable"),

  // Debug failures
  BLOCK_NOT_FOUND(-32000, "Block not found"),
  PARENT_BLOCK_NOT_FOUND(-32000, "Parent block not found"),
  GENESIS_BLOCK_NOT_TRACEABLE(-32000, "genesis is not traceable"),

  // Permissioning/Account allowlist errors
  ACCOUNT_ALLOWLIST_NOT_ENABLED(-32000, "Account allowlist has not been enabled"),
  ACCOUNT_ALLOWLIST_EMPTY_ENTRY(-32000, "Request contains an empty list of accounts"),
  ACCOUNT_ALLOWLIST_INVALID_ENTRY(-32000, "Request contains an invalid account"),
  ACCOUNT_ALLOWLIST_DUPLICATED_ENTRY(-32000, "Request contains duplicate accounts"),
  ACCOUNT_ALLOWLIST_EXISTING_ENTRY(-32000, "Cannot add an existing account to allowlist"),
  ACCOUNT_ALLOWLIST_ABSENT_ENTRY(-32000, "Cannot remove an absent account from allowlist"),

  // Permissioning/Node allowlist errors
  NODE_ALLOWLIST_NOT_ENABLED(-32000, "Node allowlist has not been enabled"),
  NODE_ALLOWLIST_EMPTY_ENTRY(-32000, "Request contains an empty list of nodes"),
  NODE_ALLOWLIST_INVALID_ENTRY(-32000, "Request contains an invalid node"),
  NODE_ALLOWLIST_DUPLICATED_ENTRY(-32000, "Request contains duplicate nodes"),
  NODE_ALLOWLIST_EXISTING_ENTRY(-32000, "Cannot add an existing node to allowlist"),
  NODE_ALLOWLIST_MISSING_ENTRY(-32000, "Cannot remove an absent node from allowlist"),
  NODE_ALLOWLIST_FIXED_NODE_CANNOT_BE_REMOVED(
      -32000, "Cannot remove a fixed node (bootnode or static node) from allowlist"),

  // Permissioning/persistence errors
  ALLOWLIST_PERSIST_FAILURE(
      -32000, "Unable to persist changes to allowlist configuration file. Changes reverted"),
  ALLOWLIST_FILE_SYNC(
      -32000,
      "The permissioning allowlist configuration file is out of sync.  The changes have been applied, but not persisted to disk"),
  ALLOWLIST_RELOAD_ERROR(
      -32000,
      "Error reloading permissions file. Please use perm_getAccountsAllowlist and perm_getNodesAllowlist to review the current state of the allowlists"),
  PERMISSIONING_NOT_ENABLED(-32000, "Node/Account allowlist has not been enabled"),
  NON_PERMITTED_NODE_CANNOT_BE_ADDED_AS_A_PEER(-32000, "Cannot add a non-permitted node as a peer"),

  // Permissioning/Authorization errors
  UNAUTHORIZED(-40100, "Unauthorized"),

  CANT_CONNECT_TO_LOCAL_PEER(-32100, "Cannot add local node as peer."),
  CANT_RESOLVE_PEER_ENODE_DNS(-32100, "Cannot resolve enode DNS hostname"),
  DNS_NOT_ENABLED(-32100, "Enode DNS support is disabled"),

  // Invalid input errors
  ENODE_ID_INVALID(
      -32000,
      "Invalid node ID: node ID must have exactly 128 hexadecimal characters and should not include any '0x' hex prefix."),
  JSON_RPC_NOT_CANONICAL_ERROR(-32000, "Invalid input"),

  /** Plugins error */
  PLUGIN_NOT_FOUND(-60000, "Plugin not found"),
  PLUGIN_INTERNAL_ERROR(-32603, "Plugin internal error"),

  // Retesteth Errors

  BLOCK_RLP_IMPORT_ERROR(-32000, "Could not decode RLP for Block"),
  BLOCK_IMPORT_ERROR(-32000, "Could not import Block"),

  UNKNOWN(-32603, "Unknown internal error"),

  INVALID_BLOBS(-32603, "blobs failed kzg validation");
  private final int code;
  private final String message;
  private final Function<String, Optional<String>> dataDecoder;

  RpcErrorType(final int code, final String message) {
    this(code, message, null);
  }

  RpcErrorType(
      final int code, final String message, final Function<String, Optional<String>> dataDecoder) {
    this.code = code;
    this.message = message;
    this.dataDecoder = dataDecoder;
  }

  @Override
  public int getCode() {
    return code;
  }

  @Override
  public String getMessage() {
    return message;
  }

  @Override
  public Optional<String> decodeData(final String data) {
    return dataDecoder == null ? Optional.empty() : dataDecoder.apply(data);
  }
}
