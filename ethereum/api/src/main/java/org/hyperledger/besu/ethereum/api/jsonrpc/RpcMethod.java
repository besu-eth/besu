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
package org.hyperledger.besu.ethereum.api.jsonrpc;

import java.util.Collection;
import java.util.HashSet;

public enum RpcMethod {
  ADMIN_ADD_PEER("admin_addPeer"),
  ADMIN_NODE_INFO("admin_nodeInfo"),
  ADMIN_PEERS("admin_peers"),
  ADMIN_REMOVE_PEER("admin_removePeer"),
  ADMIN_CHANGE_LOG_LEVEL("admin_changeLogLevel"),
  ADMIN_GENERATE_LOG_BLOOM_CACHE("admin_generateLogBloomCache"),
  ADMIN_LOGS_REPAIR_CACHE("admin_logsRepairCache"),
  ADMIN_LOGS_REMOVE_CACHE("admin_logsRemoveCache"),
  DEBUG_ACCOUNT_AT("debug_accountAt"),
  DEBUG_ACCOUNT_RANGE("debug_accountRange"),
  DEBUG_METRICS("debug_metrics"),
  DEBUG_RESYNC_WORLDSTATE("debug_resyncWorldState"),
  DEBUG_SET_HEAD("debug_setHead"),
  DEBUG_REPLAY_BLOCK("debug_replayBlock"),
  DEBUG_STORAGE_RANGE_AT("debug_storageRangeAt"),
  DEBUG_TRACE_BLOCK("debug_traceBlock"),
  DEBUG_TRACE_BLOCK_BY_HASH("debug_traceBlockByHash"),
  DEBUG_TRACE_BLOCK_BY_NUMBER("debug_traceBlockByNumber"),
  DEBUG_STANDARD_TRACE_BLOCK_TO_FILE("debug_standardTraceBlockToFile"),
  DEBUG_STANDARD_TRACE_BAD_BLOCK_TO_FILE("debug_standardTraceBadBlockToFile"),
  DEBUG_TRACE_TRANSACTION("debug_traceTransaction"),
  DEBUG_TRACE_CALL("debug_traceCall"),
  DEBUG_BATCH_RAW_TRANSACTION("debug_batchSendRawTransaction"),
  DEBUG_GET_BAD_BLOCKS("debug_getBadBlocks"),
  DEBUG_GET_RAW_HEADER("debug_getRawHeader"),
  DEBUG_GET_RAW_BLOCK("debug_getRawBlock"),
  DEBUG_GET_RAW_RECEIPTS("debug_getRawReceipts"),
  DEBUG_GET_RAW_TRANSACTION("debug_getRawTransaction"),
  ENGINE_GET_BLOBS_V1("engine_getBlobsV1"),
  ENGINE_GET_BLOBS_V2("engine_getBlobsV2"),
  ENGINE_GET_BLOBS_V3("engine_getBlobsV3"),
  ENGINE_GET_PAYLOAD_V1("engine_getPayloadV1"),
  ENGINE_GET_PAYLOAD_V2("engine_getPayloadV2"),
  ENGINE_GET_PAYLOAD_V3("engine_getPayloadV3"),
  ENGINE_GET_PAYLOAD_V4("engine_getPayloadV4"),
  ENGINE_GET_PAYLOAD_V5("engine_getPayloadV5"),
  ENGINE_GET_PAYLOAD_V6("engine_getPayloadV6"),
  ENGINE_NEW_PAYLOAD_V1("engine_newPayloadV1"),
  ENGINE_NEW_PAYLOAD_V2("engine_newPayloadV2"),
  ENGINE_NEW_PAYLOAD_V3("engine_newPayloadV3"),
  ENGINE_NEW_PAYLOAD_V4("engine_newPayloadV4"),
  ENGINE_NEW_PAYLOAD_V5("engine_newPayloadV5"),
  ENGINE_FORKCHOICE_UPDATED_V1("engine_forkchoiceUpdatedV1"),
  ENGINE_FORKCHOICE_UPDATED_V2("engine_forkchoiceUpdatedV2"),
  ENGINE_FORKCHOICE_UPDATED_V3("engine_forkchoiceUpdatedV3"),
  ENGINE_FORKCHOICE_UPDATED_V4("engine_forkchoiceUpdatedV4"),
  ENGINE_EXCHANGE_TRANSITION_CONFIGURATION("engine_exchangeTransitionConfigurationV1"),
  ENGINE_GET_CLIENT_VERSION_V1("engine_getClientVersionV1"),
  ENGINE_GET_PAYLOAD_BODIES_BY_HASH_V1("engine_getPayloadBodiesByHashV1"),
  ENGINE_GET_PAYLOAD_BODIES_BY_HASH_V2("engine_getPayloadBodiesByHashV2"),
  ENGINE_GET_PAYLOAD_BODIES_BY_RANGE_V1("engine_getPayloadBodiesByRangeV1"),
  ENGINE_GET_PAYLOAD_BODIES_BY_RANGE_V2("engine_getPayloadBodiesByRangeV2"),
  ENGINE_EXCHANGE_CAPABILITIES("engine_exchangeCapabilities"),
  ENGINE_PREPARE_PAYLOAD_DEBUG("engine_preparePayload_debug"),
  ETH_ACCOUNTS("eth_accounts"),
  ETH_BLOCK_NUMBER("eth_blockNumber"),
  ETH_CALL("eth_call"),
  ETH_CHAIN_ID("eth_chainId"),
  ETH_COINBASE("eth_coinbase"),
  ETH_CONFIG("eth_config"),
  ETH_ESTIMATE_GAS("eth_estimateGas"),
  ETH_CREATE_ACCESS_LIST("eth_createAccessList"),
  ETH_FEE_HISTORY("eth_feeHistory"),
  ETH_GAS_PRICE("eth_gasPrice"),
  ETH_BLOB_BASE_FEE("eth_blobBaseFee"),
  ETH_GET_BALANCE("eth_getBalance"),
  ETH_GET_BLOCK_ACCESS_LIST("eth_getBlockAccessList"),
  ETH_GET_BLOCK_BY_HASH("eth_getBlockByHash"),
  ETH_GET_BLOCK_BY_NUMBER("eth_getBlockByNumber"),
  ETH_GET_BLOCK_RECEIPTS("eth_getBlockReceipts"),
  ETH_GET_BLOCK_TRANSACTION_COUNT_BY_HASH("eth_getBlockTransactionCountByHash"),
  ETH_GET_BLOCK_TRANSACTION_COUNT_BY_NUMBER("eth_getBlockTransactionCountByNumber"),
  ETH_GET_CODE("eth_getCode"),
  ETH_GET_FILTER_CHANGES("eth_getFilterChanges"),
  ETH_GET_FILTER_LOGS("eth_getFilterLogs"),
  ETH_GET_LOGS("eth_getLogs"),
  ETH_GET_MAX_PRIORITY_FEE_PER_GAS("eth_maxPriorityFeePerGas"),
  ETH_GET_PROOF("eth_getProof"),
  ETH_GET_STORAGE_AT("eth_getStorageAt"),
  ETH_GET_TRANSACTION_BY_BLOCK_HASH_AND_INDEX("eth_getTransactionByBlockHashAndIndex"),
  ETH_GET_TRANSACTION_BY_BLOCK_NUMBER_AND_INDEX("eth_getTransactionByBlockNumberAndIndex"),
  ETH_GET_TRANSACTION_BY_HASH("eth_getTransactionByHash"),
  ETH_GET_TRANSACTION_COUNT("eth_getTransactionCount"),
  ETH_GET_TRANSACTION_RECEIPT("eth_getTransactionReceipt"),
  ETH_GET_UNCLE_BY_BLOCK_HASH_AND_INDEX("eth_getUncleByBlockHashAndIndex"),
  ETH_GET_UNCLE_BY_BLOCK_NUMBER_AND_INDEX("eth_getUncleByBlockNumberAndIndex"),
  ETH_GET_UNCLE_COUNT_BY_BLOCK_HASH("eth_getUncleCountByBlockHash"),
  ETH_GET_UNCLE_COUNT_BY_BLOCK_NUMBER("eth_getUncleCountByBlockNumber"),
  ETH_MINING("eth_mining"),
  ETH_NEW_BLOCK_FILTER("eth_newBlockFilter"),
  ETH_NEW_FILTER("eth_newFilter"),
  ETH_NEW_PENDING_TRANSACTION_FILTER("eth_newPendingTransactionFilter"),
  ETH_PROTOCOL_VERSION("eth_protocolVersion"),
  ETH_SEND_RAW_TRANSACTION("eth_sendRawTransaction"),
  ETH_SEND_TRANSACTION("eth_sendTransaction"),
  ETH_SIMULATE_V1("eth_simulateV1"),
  ETH_SUBSCRIBE("eth_subscribe"),
  ETH_SYNCING("eth_syncing"),
  ETH_UNINSTALL_FILTER("eth_uninstallFilter"),
  ETH_UNSUBSCRIBE("eth_unsubscribe"),
  IBFT_DISCARD_VALIDATOR_VOTE("ibft_discardValidatorVote"),
  IBFT_GET_PENDING_VOTES("ibft_getPendingVotes"),
  IBFT_GET_VALIDATORS_BY_BLOCK_HASH("ibft_getValidatorsByBlockHash"),
  IBFT_GET_VALIDATORS_BY_BLOCK_NUMBER("ibft_getValidatorsByBlockNumber"),
  IBFT_PROPOSE_VALIDATOR_VOTE("ibft_proposeValidatorVote"),
  IBFT_GET_SIGNER_METRICS("ibft_getSignerMetrics"),
  QBFT_DISCARD_VALIDATOR_VOTE("qbft_discardValidatorVote"),
  QBFT_GET_PENDING_VOTES("qbft_getPendingVotes"),
  QBFT_GET_VALIDATORS_BY_BLOCK_HASH("qbft_getValidatorsByBlockHash"),
  QBFT_GET_VALIDATORS_BY_BLOCK_NUMBER("qbft_getValidatorsByBlockNumber"),
  QBFT_PROPOSE_VALIDATOR_VOTE("qbft_proposeValidatorVote"),
  QBFT_GET_SIGNER_METRICS("qbft_getSignerMetrics"),
  QBFT_GET_REQUEST_TIMEOUT_SECONDS("qbft_getRequestTimeoutSeconds"),
  MINER_CHANGE_TARGET_GAS_LIMIT("miner_changeTargetGasLimit"),
  MINER_START("miner_start"),
  MINER_STOP("miner_stop"),
  MINER_GET_MIN_PRIORITY_FEE("miner_getMinPriorityFee"),
  MINER_SET_MIN_PRIORITY_FEE("miner_setMinPriorityFee"),
  MINER_GET_MIN_GAS_PRICE("miner_getMinGasPrice"),
  MINER_SET_MIN_GAS_PRICE("miner_setMinGasPrice"),
  MINER_GET_EXTRA_DATA("miner_getExtraData"),
  MINER_SET_EXTRA_DATA("miner_setExtraData"),
  NET_ENODE("net_enode"),
  NET_LISTENING("net_listening"),
  NET_PEER_COUNT("net_peerCount"),
  NET_SERVICES("net_services"),
  NET_VERSION("net_version"),
  PERM_ADD_ACCOUNTS_TO_WHITELIST("perm_addAccountsToWhitelist"),
  PERM_ADD_ACCOUNTS_TO_ALLOWLIST("perm_addAccountsToAllowlist"),
  PERM_ADD_NODES_TO_WHITELIST("perm_addNodesToWhitelist"),
  PERM_ADD_NODES_TO_ALLOWLIST("perm_addNodesToAllowlist"),
  PERM_GET_ACCOUNTS_WHITELIST("perm_getAccountsWhitelist"),
  PERM_GET_ACCOUNTS_ALLOWLIST("perm_getAccountsAllowlist"),
  PERM_GET_NODES_WHITELIST("perm_getNodesWhitelist"),
  PERM_GET_NODES_ALLOWLIST("perm_getNodesAllowlist"),
  PERM_RELOAD_PERMISSIONS_FROM_FILE("perm_reloadPermissionsFromFile"),
  PERM_REMOVE_ACCOUNTS_FROM_ALLOWLIST("perm_removeAccountsFromAllowlist"),
  PERM_REMOVE_NODES_FROM_ALLOWLIST("perm_removeNodesFromAllowlist"),
  RPC_MODULES("rpc_modules"),
  TRACE_BLOCK("trace_block"),
  TRACE_CALL("trace_call"),
  TRACE_CALL_MANY("trace_callMany"),
  TRACE_GET("trace_get"),
  TRACE_FILTER("trace_filter"),
  TRACE_RAW_TRANSACTION("trace_rawTransaction"),
  TRACE_REPLAY_BLOCK_TRANSACTIONS("trace_replayBlockTransactions"),
  TRACE_TRANSACTION("trace_transaction"),
  TX_POOL_BESU_STATISTICS("txpool_besuStatistics"),
  TX_POOL_BESU_TRANSACTIONS("txpool_besuTransactions"),
  TX_POOL_BESU_PENDING_TRANSACTIONS("txpool_besuPendingTransactions"),
  TX_POOL_STATUS("txpool_status"),
  TX_POOL_CONTENT_FROM("txpool_contentFrom"),
  TX_POOL_CONTENT("txpool_content"),
  TX_POOL_INSPECT("txpool_inspect"),
  WEB3_CLIENT_VERSION("web3_clientVersion"),
  WEB3_SHA3("web3_sha3"),
  PLUGINS_RELOAD_CONFIG("plugins_reloadPluginConfig"),
  TESTING_BUILD_BLOCK_V1("testing_buildBlockV1");

  private final String methodName;

  private static final Collection<String> allMethodNames;

  public String getMethodName() {
    return methodName;
  }

  static {
    allMethodNames = new HashSet<>();
    for (RpcMethod m : RpcMethod.values()) {
      allMethodNames.add(m.getMethodName());
    }
  }

  RpcMethod(final String methodName) {
    this.methodName = methodName;
  }

  public static boolean rpcMethodExists(final String rpcMethodName) {
    return allMethodNames.contains(rpcMethodName);
  }
}
