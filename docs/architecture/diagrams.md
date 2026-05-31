# Architecture Class Diagrams

This page contains automatically generated class diagrams from the codebase. These diagrams are regenerated on every push to `main` branch.

## Transaction & Isolation System

Class diagram for the transaction management and isolation strategies:

```plantuml
!include docs/diagrams/txn_classes.puml
```

### Isolation Implementations

Detailed class diagram for isolation manager implementations (MVCC, 2PL, OCC):

```plantuml
!include docs/diagrams/isolation_classes.puml
```

---

## Storage System

### Overall Storage Architecture

```plantuml
!include docs/diagrams/storage_classes.puml
```

### B+ Tree Implementation

Detailed class diagram for B+ Tree backend:

```plantuml
!include docs/diagrams/btree_classes.puml
```

### LSM Tree Implementation

Detailed class diagram for LSM Tree backend:

```plantuml
!include docs/diagrams/lsm_classes.puml
```

---

## Concurrency Control

Class diagram for concurrency control and replacement policies:

```plantuml
!include docs/diagrams/concurrency_classes.puml
```

---

## Full Architecture Overview

Complete class diagram for all internal components:

```plantuml
!include docs/diagrams/full_architecture.puml
```

---

## Generating Diagrams Locally

To regenerate diagrams on your machine:

```bash
# Install GoPlantUML
go install github.com/jfeliu007/goplantuml/cmd/goplantuml@latest

# Generate all diagrams
mkdir -p docs/diagrams
goplantuml -d ./src/engine > docs/diagrams/txn_classes.puml
goplantuml -d ./src/engine/mvcc > docs/diagrams/isolation_classes.puml
goplantuml -d ./src/storage > docs/diagrams/storage_classes.puml
goplantuml -d ./src/storage/btree > docs/diagrams/btree_classes.puml
goplantuml -d ./src/storage/lsm > docs/diagrams/lsm_classes.puml
goplantuml -d ./src/storage/replacer > docs/diagrams/concurrency_classes.puml
goplantuml -d ./src > docs/diagrams/full_architecture.puml
```

Or simply run:
```bash
make diagrams
```

## Viewing Diagrams

- **VS Code**: Install "PlantUML" extension to preview `.puml` files
- **Online**: Use [PlantUML Online Editor](http://www.plantuml.com/plantuml/uml/)
- **MkDocs**: Published automatically to this site on every commit to `main`
