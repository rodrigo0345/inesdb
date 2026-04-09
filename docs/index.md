# InesDB - Indexed Node Engine Storage

Welcome to the InesDB documentation. InesDB is a sophisticated on-disk database engine written in Go with focus on **row-major, interchangeable algorithms** for storage backends, efficient memory management, and robust transaction support.

## Quick Overview

InesDB is database engine featuring:

- **Row-Based Storage**: Structured data organization with slotted pages
- **Multiple Access Methods**: B+ Tree and LSM Tree backends
- **Advanced Buffer Management**: Intelligent caching with multiple replacement policies
- **ACID Transactions**: Full transaction support with multiple isolation levels
- **Concurrency Control**: MVCC, Optimistic Concurrency Control, and Two-Phase Locking
- **Durability**: Write-Ahead Logging (WAL) for crash recovery

## Key Features

### Storage Backends
- **B+ Tree**: Traditional disk-based B+ tree with automatic page management
- **LSM Tree**: Write-optimized log-structured merge tree for high-throughput scenarios

### Transaction Support
- Multi-Version Concurrency Control (MVCC)
- Optimistic Concurrency Control (OCC)
- Two-Phase Locking (2PL)
- Comprehensive rollback with undo logs

### Concurrency Management
- Deadlock detection using wait-for graphs
- Multiple cache replacement policies (LRU, Clock)
- Fine-grained transaction isolation levels

## Architecture Highlights

```
┌─────────────────────────────────────┐
│      Application Layer              │
└────────────┬────────────────────────┘
             │
┌────────────▼────────────────────────┐
│    Transaction Manager (ACID)       │
├─────────────┬───────────────────────┤
│   Isolation │   Lock Manager        │
│   Manager   │   (Deadlock Detect)   │
├─────────────┴───────────────────────┤
│    Write-Ahead Logger (WAL)         │
├─────────────────────────────────────┤
│     Access Methods Layer            │
│  ┌──────────────┬──────────────┐   │
│  │  B+ Tree     │  LSM Tree    │   │
│  └───────┬──────┴────────┬─────┘   │
└──────────┼───────────────┼──────────┘
           │               │
┌──────────▼───────────────▼──────────┐
│    Buffer Pool Manager              │
│  ┌─ Clock/LRU Replacement Policy ─┐ │
│  │ - Page Caching                 │ │
│  └────────────────────────────────┘ │
└──────────┬───────────────────────────┘
           │
┌──────────▼───────────────────────────┐
│    Disk Manager (Persistent Store)   │
└─────────────────────────────────────┘
```

## Documentation Structure

- **[Architecture](architecture/overview.md)**: Detailed design documentation for each component
- **[Features](features/index.md)**: Overview of storage engine capabilities
- **[API Reference](api-reference/index.md)**: Complete API documentation
- **[Packages](packages/overview.md)**: Internal package structure

## Getting Started

To use OMAG in your project:

1. Import the package: `import "github.com/rodrigo0345/omag"`
2. Choose your backend (B+ Tree or LSM Tree)
3. Configure the buffer pool settings
4. Start managing transactions

## Project Information

- **Repository**: [GitHub](https://github.com/rodrigo0345/omag)
- **Language**: Go 1.25+
- **License**: See repository for details

For detailed documentation on specific topics, navigate using the menu on the left.
