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
package org.hyperledger.besu.evmtool;

import static java.nio.charset.StandardCharsets.UTF_8;
import static picocli.CommandLine.ScopeType.INHERIT;

import org.hyperledger.besu.config.NetworkDefinition;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.tracing.StreamingOperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.MetricsSystemModule;
import org.hyperledger.besu.util.LogConfigurator;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import io.vertx.core.json.JsonObject;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * This class, EvmToolCommand, serves as the main command for the EVM (Ethereum Virtual Machine)
 * tool. The EVM tool is used to execute Ethereum transactions and contracts in a local environment.
 *
 * <p>EvmToolCommand implements the Runnable interface, making it the entrypoint for PicoCLI to
 * execute this command.
 *
 * <p>The class provides various options for setting up and executing EVM transactions. These
 * options include, but are not limited to, setting the gas price, sender address, receiver address,
 * and the data to be sent with the transaction.
 *
 * <p>Key methods in this class include 'run()' for executing the command, 'execute()' for setting
 * up and running the EVM transaction, and 'dumpWorldState()' for outputting the current state of
 * the Ethereum world state.
 */
@Command(
    description = "This command evaluates EVM transactions.",
    abbreviateSynopsis = true,
    name = "evm",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class,
    sortOptions = false,
    header = "Usage:",
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription:%n%n",
    optionListHeading = "%nOptions:%n",
    footerHeading = "%n",
    footer = "Besu is licensed under the Apache License 2.0",
    subcommands = {
      BenchmarkSubCommand.class,
      B11rSubCommand.class,
      BlockchainTestSubCommand.class,
      StateTestSubCommand.class,
      T8nSubCommand.class,
      T8nServerSubCommand.class
    })
public class EvmToolCommand implements Runnable {

  @Option(
      names = {"--code"},
      paramLabel = "<code>",
      description = "Byte stream of code to be executed.")
  void setBytes(final String optionValue) {
    codeBytes = Bytes.fromHexString(optionValue.replace(" ", ""));
  }

  private Bytes codeBytes = Bytes.EMPTY;

  @Option(
      names = {"--gas"},
      description = "Amount of gas for this invocation.",
      paramLabel = "<int>")
  private final Long gas = 10_000_000_000L;

  @Option(
      names = {"--intrinsic-gas"},
      description = "Calculate and charge intrinsic and tx content gas. Default is not to charge.",
      scope = INHERIT,
      negatable = true)
  final Boolean chargeIntrinsicGas = false;

  @Option(
      names = {"--price"},
      description = "Price of gas (in GWei) for this invocation",
      paramLabel = "<int>")
  private final Wei gasPriceGWei = Wei.ZERO;

  @Option(
      names = {"--blob-price"},
      description = "Price of blob gas for this invocation",
      paramLabel = "<int>")
  private final Wei blobGasPrice = Wei.ZERO;

  @Option(
      names = {"--sender"},
      paramLabel = "<address>",
      description = "Calling address for this invocation.")
  private final Address sender = Address.fromHexString("0x73656e646572");

  @Option(
      names = {"--receiver"},
      paramLabel = "<address>",
      description = "Receiving address for this invocation.")
  private final Address receiver = Address.fromHexString("0x7265636569766572");

  @Option(
      names = {"--create"},
      description = "Run call should be a create instead of a call operation.")
  private final Boolean createTransaction = false;

  @Option(
      names = {"--contract"},
      paramLabel = "<address>",
      description = "The address holding the contract code.")
  private final Address contract = Address.fromHexString("0x7265636569766572");

  @Option(
      names = {"--coinbase"},
      paramLabel = "<address>",
      description = "Coinbase for this invocation.")
  private final Address coinbase = Address.ZERO;

  @Option(
      names = {"--input"},
      paramLabel = "<code>",
      description = "The CALLDATA for this invocation")
  private final Bytes callData = Bytes.EMPTY;

  @Option(
      names = {"--value"},
      description = "The amount of ether attached to this invocation",
      paramLabel = "<int>")
  private final Wei ethValue = Wei.ZERO;

  @Option(
      names = {"--json", "--trace"},
      description = "Trace each opcode as a json object.",
      scope = INHERIT)
  final Boolean showJsonResults = false;

  @Option(
      names = {"--json-alloc"},
      description = "Output the final allocations after a run.",
      scope = INHERIT)
  final Boolean showJsonAlloc = false;

  @Option(
      names = {"--noeip-3155", "--trace.noeip-3155"},
      description = "Produce a trace with types strictly compatible with EIP-3155.",
      scope = INHERIT,
      negatable = true)
  final Boolean eip3155strict = true;

  @Option(
      names = {"--memory", "--trace.memory"},
      description =
          "Show the full memory output in tracing for each op. Default is not to show memory.",
      scope = INHERIT,
      negatable = true)
  final Boolean showMemory = false;

  @Option(
      names = {"--trace.nostack"},
      description = "Show the operand stack in tracing for each op. Default is to show stack.",
      scope = INHERIT,
      negatable = true)
  final Boolean hideStack = false;

  @Option(
      names = {"--trace.returndata"},
      description =
          "Show the return data in tracing for each op when present. Default is to show return data.",
      scope = INHERIT,
      negatable = true)
  final Boolean showReturnData = false;

  @Option(
      names = {"--trace.storage"},
      description =
          "Show the updated storage slots for the current account. Default is to not show updated storage.",
      scope = INHERIT,
      negatable = true)
  final Boolean showStorage = false;

  @Option(
      names = {"--notime"},
      description = "Don't include time data in summary output.",
      scope = INHERIT,
      negatable = true)
  final Boolean noTime = false;

  @Option(
      names = {"--prestate", "--genesis"},
      description = "The genesis file containing account data for this invocation.")
  private final File genesisFile = null;

  @Option(
      names = {"--chain"},
      description = "Name of a well known network that will be used for this invocation.")
  private final NetworkDefinition network = null;

  @Option(
      names = {"--repeat"},
      description = "Number of times to repeat for benchmarking.")
  private final Integer repeat = 0;

  @Option(
      names = {"-v", "--version"},
      versionHelp = true,
      description = "display version info")
  boolean versionInfoRequested;

  Integer getRepeatCount() {
    return repeat;
  }

  static final Joiner STORAGE_JOINER = Joiner.on(",\n");
  private final EvmToolCommandOptionsModule daggerOptions = new EvmToolCommandOptionsModule();
  PrintWriter out;
  InputStream in;

  /**
   * Default constructor for the EvmToolCommand class. It initializes the input stream with an empty
   * byte array and the output stream with the standard output.
   */
  public EvmToolCommand() {
    this(
        new ByteArrayInputStream(new byte[0]),
        new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out, UTF_8)), true));
  }

  /**
   * Constructor for the EvmToolCommand class with custom input and output streams.
   *
   * @param in The input stream to be used.
   * @param out The output stream to be used.
   */
  public EvmToolCommand(final InputStream in, final PrintWriter out) {
    this.in = in;
    this.out = out;
  }

  void execute(final String... args) {
    execute(System.in, new PrintWriter(System.out, true, UTF_8), args);
  }

  void execute(final InputStream input, final PrintWriter output, final String[] args) {
    final CommandLine commandLine = new CommandLine(this).setOut(output);
    out = output;
    in = input;

    // don't require exact case to match enum values
    commandLine.setCaseInsensitiveEnumValuesAllowed(true);

    // add dagger-injected options
    commandLine.addMixin("Dagger Options", daggerOptions);

    // add sub commands here
    commandLine.registerConverter(Address.class, Address::fromHexString);
    commandLine.registerConverter(Bytes.class, Bytes::fromHexString);
    commandLine.registerConverter(Wei.class, arg -> Wei.of(Long.parseUnsignedLong(arg)));

    // change negation regexp so --nomemory works.  See
    // https://picocli.info/#_customizing_negatable_options
    commandLine.setNegatableOptionTransformer(
        new CommandLine.RegexTransformer.Builder()
            .addPattern("^--no(\\w(-|\\w)*)$", "--$1", "--[no]$1")
            .addPattern("^--trace.no(\\w(-|\\w)*)$", "--trace.$1", "--trace.[no]$1")
            .addPattern("^--(\\w(-|\\w)*)$", "--no$1", "--[no]$1")
            .addPattern("^--trace.(\\w(-|\\w)*)$", "--trace.no$1", "--trace.[no]$1")
            .build());

    // Enumerate forks to support execution-spec-tests
    addForkHelp(commandLine.getSubcommands().get("t8n"));
    addForkHelp(commandLine.getSubcommands().get("t8n-server"));

    commandLine.setExecutionStrategy(
        parseResult -> {
          if (daggerOptions.isEvmV2Enabled()) {
            out.println("EVM v2 (long[] stack) enabled");
          }
          return new CommandLine.RunLast().execute(parseResult);
        });
    commandLine.execute(args);
  }

  private static void addForkHelp(final CommandLine subCommandLine) {
    subCommandLine
        .getHelpSectionMap()
        .put("forks_header", help -> help.createHeading("%nKnown Forks:%n"));
    subCommandLine
        .getHelpSectionMap()
        .put(
            "forks",
            help ->
                help.createTextTable(
                        Arrays.stream(EvmSpecVersion.values())
                            .collect(
                                Collectors.toMap(
                                    EvmSpecVersion::getName,
                                    EvmSpecVersion::getDescription,
                                    (a, b) -> b,
                                    LinkedHashMap::new)))
                    .toString());
    List<String> keys = new ArrayList<>(subCommandLine.getHelpSectionKeys());
    int index = keys.indexOf(CommandLine.Model.UsageMessageSpec.SECTION_KEY_FOOTER_HEADING);
    keys.add(index, "forks_header");
    keys.add(index + 1, "forks");

    subCommandLine.setHelpSectionKeys(keys);
  }

  /**
   * Returns the fork name provided by the Dagger options. If no fork is provided, it returns the
   * name of the default EVM specification version.
   *
   * @return The fork name.
   */
  public String getFork() {
    return daggerOptions.provideFork().orElse(EvmSpecVersion.defaultVersion().getName());
  }

  /**
   * Checks if a fork is provided in the Dagger options.
   *
   * @return True if a fork is provided, false otherwise.
   */
  public boolean hasFork() {
    return daggerOptions.provideFork().isPresent();
  }

  /**
   * Returns the EVM configuration options.
   *
   * @return The EVM configuration.
   */
  public EvmConfiguration getEvmConfiguration() {
    return daggerOptions.provideEvmConfiguration();
  }

  @Override
  public void run() {
    LogConfigurator.setLevel("", "OFF");
    try {
      GenesisFileModule genesisFileModule;
      if (network != null) {
        genesisFileModule = GenesisFileModule.createGenesisModule(network);
      } else if (genesisFile != null) {
        genesisFileModule = GenesisFileModule.createGenesisModule(genesisFile);
      } else {
        genesisFileModule = GenesisFileModule.createGenesisModule();
      }
      final EvmToolComponent component =
          DaggerEvmToolComponent.builder()
              .dataStoreModule(new DataStoreModule())
              .genesisFileModule(genesisFileModule)
              .evmToolCommandOptionsModule(daggerOptions)
              .metricsSystemModule(new MetricsSystemModule())
              .build();

      int remainingIters = this.repeat;
      final ProtocolSpec protocolSpec = component.getProtocolSpec();
      final Transaction tx =
          new Transaction.Builder()
              .nonce(0)
              .gasPrice(Wei.ZERO)
              .gasLimit(Long.MAX_VALUE)
              .to(createTransaction ? null : receiver)
              .value(Wei.ZERO)
              .payload(callData)
              .sender(sender)
              .build();

      long txGas = gas;
      if (chargeIntrinsicGas) {
        final long accessListCost =
            tx.getAccessList()
                .map(list -> protocolSpec.getGasCalculator().accessListGasCost(list))
                .orElse(0L);

        final long delegateCodeCost =
            protocolSpec.getGasCalculator().delegateCodeGasCost(tx.codeDelegationListSize());

        final long intrinsicGasCost =
            protocolSpec
                .getGasCalculator()
                .transactionIntrinsicGasCost(tx, accessListCost + delegateCodeCost);
        txGas -= intrinsicGasCost;
      }

      final EVM evm = protocolSpec.getEvm();
      if (codeBytes.isEmpty() && !createTransaction) {
        codeBytes = component.getWorldState().get(receiver).getCode();
      }
      Code code = new Code(codeBytes);

      final Stopwatch stopwatch = Stopwatch.createUnstarted();
      long lastTime = 0;
      do {
        final boolean lastLoop = remainingIters == 0;

        final OperationTracer tracer = // You should have picked Mercy.
            lastLoop && showJsonResults
                ? new StreamingOperationTracer(
                    out,
                    OpCodeTracerConfigBuilder.create()
                        .traceMemory(showMemory)
                        .traceStack(!hideStack)
                        .traceReturnData(showReturnData)
                        .traceStorage(showStorage)
                        .traceOpcodes(Collections.emptySet())
                        .eip3155Strict(eip3155strict)
                        .build())
                : OperationTracer.NO_TRACING;

        WorldUpdater updater = component.getWorldUpdater();
        updater.getOrCreate(sender);
        if (!createTransaction) {
          updater.getOrCreate(receiver);
        }
        var contractAccount = updater.getOrCreate(contract);
        contractAccount.setCode(codeBytes);

        final Set<Address> addressList = new HashSet<>(Address.SIZE);
        addressList.add(sender);
        addressList.add(contract);
        if (EvmSpecVersion.SHANGHAI.compareTo(evm.getEvmVersion()) <= 0) {
          addressList.add(coinbase);
        }
        final BlockHeader blockHeader =
            BlockHeaderBuilder.create()
                .parentHash(Hash.EMPTY)
                .coinbase(coinbase)
                .difficulty(
                    Difficulty.fromHexString(
                        genesisFileModule.providesGenesisConfig().getDifficulty()))
                .number(0)
                .gasLimit(genesisFileModule.providesGenesisConfig().getGasLimit())
                .timestamp(0)
                .ommersHash(Hash.EMPTY_LIST_HASH)
                .stateRoot(Hash.EMPTY_TRIE_HASH)
                .transactionsRoot(Hash.EMPTY)
                .receiptsRoot(Hash.EMPTY)
                .logsBloom(LogsBloomFilter.empty())
                .gasUsed(0)
                .extraData(Bytes.EMPTY)
                .mixHash(Hash.ZERO)
                .nonce(0)
                .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
                .baseFee(
                    component
                        .getBlockchain()
                        .getChainHeadHeader()
                        .getBaseFee()
                        .or(() -> genesisFileModule.providesGenesisConfig().getBaseFeePerGas())
                        .orElse(
                            protocolSpec.getFeeMarket().implementsBaseFee() ? Wei.of(0xa) : null))
                .buildBlockHeader();

        Address contractAddress =
            createTransaction ? Address.contractAddress(receiver, 0) : receiver;
        MessageFrame initialMessageFrame =
            MessageFrame.builder()
                .type(
                    createTransaction
                        ? MessageFrame.Type.CONTRACT_CREATION
                        : MessageFrame.Type.MESSAGE_CALL)
                .worldUpdater(updater.updater())
                .initialGas(txGas)
                .contract(contractAddress)
                .address(contractAddress)
                .originator(sender)
                .sender(sender)
                .gasPrice(gasPriceGWei)
                .blobGasPrice(blobGasPrice)
                .inputData(createTransaction ? codeBytes.slice(code.getSize()) : callData)
                .value(ethValue)
                .apparentValue(ethValue)
                .code(code)
                .blockValues(blockHeader)
                .completer(c -> {})
                .miningBeneficiary(blockHeader.getCoinbase())
                .blockHashLookup(
                    protocolSpec
                        .getPreExecutionProcessor()
                        .createBlockHashLookup(component.getBlockchain(), blockHeader))
                .eip2930AccessListWarmAddresses(addressList)
                .build();
        Deque<MessageFrame> messageFrameStack = initialMessageFrame.getMessageFrameStack();

        stopwatch.start();
        while (!messageFrameStack.isEmpty()) {
          final MessageFrame messageFrame = messageFrameStack.peek();
          protocolSpec.getTransactionProcessor().process(messageFrame, tracer);
          if (messageFrameStack.isEmpty()) {
            stopwatch.stop();
            if (lastTime == 0) {
              lastTime = stopwatch.elapsed().toNanos();
            }
            if (lastLoop) {
              messageFrame
                  .getExceptionalHaltReason()
                  .ifPresent(haltReason -> out.println(haltReason));
              messageFrame.getRevertReason().ifPresent(bytes -> out.println(bytes.toHexString()));
            }
          }
        }
        lastTime = stopwatch.elapsed().toNanos();
        stopwatch.reset();
        if (lastLoop) {
          initialMessageFrame.getSelfDestructs().forEach(updater::deleteAccount);
          updater.clearAccountsThatAreEmpty();
          updater.commit();
          MutableWorldState worldState = component.getWorldState();
          final long evmGas = txGas - initialMessageFrame.getRemainingGas();
          final JsonObject resultLine = new JsonObject();
          resultLine
              .put("stateRoot", worldState.rootHash().getBytes().toHexString())
              .put("output", initialMessageFrame.getOutputData().toHexString())
              .put("gasUsed", "0x" + Long.toHexString(evmGas))
              .put("pass", initialMessageFrame.getExceptionalHaltReason().isEmpty())
              .put("fork", protocolSpec.getHardforkId().description());
          if (!noTime) {
            resultLine.put("timens", lastTime).put("time", lastTime / 1000);
          }
          out.println(resultLine);

          if (showJsonAlloc) {
            dumpWorldState(worldState, out);
          }
        }
      } while (remainingIters-- > 0);

    } catch (final IOException e) {
      System.err.println("Unable to create Genesis module");
      e.printStackTrace(System.out);
    }
  }

  /**
   * Dumps the current state of the Ethereum world state to the provided PrintWriter. The state
   * includes account balances, nonces, codes, and storage. The output is in JSON format.
   *
   * @param worldState The Ethereum world state to be dumped.
   * @param out The PrintWriter to which the state is dumped.
   */
  public static void dumpWorldState(final MutableWorldState worldState, final PrintWriter out) {
    out.println("{");
    worldState
        .streamAccounts(Bytes32.ZERO, Integer.MAX_VALUE)
        .sorted(
            Comparator.comparing(o -> o.getAddress().orElse(Address.ZERO).getBytes().toHexString()))
        .forEach(
            a -> {
              var account = worldState.get(a.getAddress().get());
              out.println(" \"" + account.getAddress().getBytes().toHexString() + "\": {");
              if (account.getCode() != null && !account.getCode().isEmpty()) {
                out.println("  \"code\": \"" + account.getCode().toHexString() + "\",");
              }
              var storageEntries =
                  account.storageEntriesFrom(Bytes32.ZERO, Integer.MAX_VALUE).values().stream()
                      .map(
                          e ->
                              Map.entry(
                                  e.getKey().orElse(UInt256.ZERO),
                                  account.getStorageValue(UInt256.fromBytes(e.getKey().get()))))
                      .filter(e -> !e.getValue().isZero())
                      .sorted(Map.Entry.comparingByKey())
                      .toList();
              if (!storageEntries.isEmpty()) {
                out.println("  \"storage\": {");
                out.println(
                    STORAGE_JOINER.join(
                        storageEntries.stream()
                            .map(
                                e ->
                                    "   \""
                                        + e.getKey().toQuantityHexString()
                                        + "\": \""
                                        + e.getValue().toQuantityHexString()
                                        + "\"")
                            .toList()));
                out.println("  },");
              }
              out.print("  \"balance\": \"" + account.getBalance().toDecimalString() + "\"");
              if (account.getNonce() != 0) {
                out.println(",");
                out.println("  \"nonce\": " + account.getNonce());
              } else {
                out.println();
              }
              out.println(" },");
            });
    out.println("}");
    out.flush();
  }
}
