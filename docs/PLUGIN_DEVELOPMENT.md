# Besu Plugin Development Guide

This guide outlines the principles and best practices for developing plugins for Hyperledger Besu, following the structural improvements made to the Plugin API.

## Core Design Principles

1.  **Strict Lifecycle Management**: Plugins must respect the lifecycle phases. Never attempt to access runtime services (like `BesuEvents`) during the `register()` phase.
2.  **Explicit Dependencies**: While current dependency management is implicit, future versions will move towards declarative dependencies. Always check for service presence via `getService().isPresent()`.
3.  **API Versioning**: Every plugin must specify its targeted API version via `getApiVersion()`. The current version is `1`.
4.  **Minimal Core Modification**: Use plugins to extend functionality without modifying the core Besu codebase.

## Plugin Lifecycle

| Phase | Method | Description | Available Services |
| :--- | :--- | :--- | :--- |
| **Registration** | `register(context)` | Initial discovery. Register CLI options here. | `PicoCLIOptions`, `StorageService`, `SecurityModuleService` |
| **Startup Phase 1** | `beforeExternalServices()` | Called before metrics and HTTP servers start. | Most internal services. |
| **Startup Phase 2** | `start()` | **Main entry point.** Register listeners and start background tasks. | `BesuEvents`, `RpcEndpointService`, `SynchronizationService` |
| **Shutdown** | `stop()` | Cleanup resources, stop threads, and remove listeners. | N/A |

## API Organization

To maintain a clean API, new interfaces should be placed in sub-packages of `org.hyperledger.besu.plugin.services` based on their domain:

-   `.blockchain`: Services related to block data, headers, and world state.
-   `.p2p`: Services related to networking and peer discovery.
-   `.rpc`: Services related to JSON-RPC and internal API exposure.
-   `.txpool`: Services related to transaction management and validation.

## Best Practices for Layer 2 & Private Networks

When building for Layer 2 (like Linea) or private networks:
-   **Event Filtering**: Use `BesuEvents` log listeners to track specific bridge or system contracts.
-   **System Transactions**: Be aware that L2s often have zero-gas system transactions. Ensure your plugin logic handles these without dividing by zero or assuming gas costs.
-   **Custom Storage**: Use `StorageService` to persist plugin-specific data (like bridge states) using Besu's optimized storage engines.

## Example: Watchdog Plugin
Refer to the `plugins/watchdog` module for a complete reference implementation demonstrating CLI integration, lifecycle tracking, and RPC exposure.
