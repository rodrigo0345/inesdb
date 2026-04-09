# InesDB - Indexed Node Engine Storage

## Overview

InesDB is a sophisticated on-disk database engine written in Go with a primary focus on **row-major, interchangeable algorithms** for storage backends. It provides multiple access methods (B+ Tree and LSM Tree) with efficient memory management, robust transaction support, and advanced concurrency control.

## Key Features

- **Row-Based Storage**: Structured data with slotted pages
- **Interchangeable Backends**: B+ Tree and LSM Tree access methods
- **ACID Transactions**: Full transaction support with multiple isolation levels
- **Advanced Concurrency**: MVCC, OCC, and Two-Phase Locking
- **Smart Caching**: Multiple replacement policies with Write-Ahead Logging

## Getting Started

For comprehensive documentation, architecture details, and API reference, visit the [documentation](docs/index.md).

