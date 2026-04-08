#!/usr/bin/env bash
# Migration script to reorganize project structure
# Run from project root: bash scripts/migrate.sh

set -e

echo "🔄 Starting project structure migration..."

# Ensure we're in the project root
if [ ! -f "go.mod" ]; then
    echo "❌ Error: go.mod not found. Run this script from project root."
    exit 1
fi

# Create backup (skip if it already exists)
echo "📦 Creating backup..."
if [ ! -d ".backup" ]; then
    mkdir -p .backup
    # Use find to copy all files except git and backup directories
    find . -mindepth 1 -maxdepth 1 \( -not -name '.git' -not -name '.backup' -not -name '.gitignore' \) -exec cp -r {} .backup/ \;
    echo "✅ Backup created in .backup/"
else
    echo "⚠️  .backup/ already exists, skipping backup"
fi

echo ""
echo "📋 Migration Steps:"
echo "1. Move storage engine files..."
mkdir -p internal/storage/{btree,lsm,buffer,page}

# Copy buffermanager
echo "   - buffermanager → internal/storage/buffer"
if [ -d "buffermanager" ]; then
    cp buffermanager/*.go internal/storage/buffer/
fi

# Copy resource_page
echo "   - resource_page → internal/storage/page"
if [ -d "resource_page" ]; then
    cp resource_page/*.go internal/storage/page/
fi

# Copy bplus_tree_backend
echo "   - bplus_tree_backend → internal/storage/btree"
if [ -d "bplus_tree_backend" ]; then
    cp bplus_tree_backend/*.go internal/storage/btree/
fi

# Copy lsm_tree_backend
echo "   - lsm_tree_backend → internal/storage/lsm"
if [ -d "lsm_tree_backend" ]; then
    cp lsm_tree_backend/*.go internal/storage/lsm/
fi

echo ""
echo "2. Move transaction layer files..."
mkdir -p internal/txn/{isolation,lock,log,undo}

# Copy transaction files
echo "   - transaction_manager/* → internal/txn/*"
if [ -d "transaction_manager" ]; then
    # Core txn files
    cp transaction_manager/transaction.go internal/txn/
    cp transaction_manager/transaction_manager.go internal/txn/manager.go
    cp transaction_manager/operations.go internal/txn/undo/
    cp transaction_manager/undo_log.go internal/txn/undo/log.go
    cp transaction_manager/rollback_manager.go internal/txn/undo/rollback.go
    cp transaction_manager/write_handler.go internal/txn/undo/
    
    # Isolation
    cp transaction_manager/*isolation*.go internal/txn/isolation/ 2>/dev/null || true
    cp transaction_manager/*phase*.go internal/txn/isolation/ 2>/dev/null || true
    cp transaction_manager/*mvcc*.go internal/txn/isolation/ 2>/dev/null || true
    cp transaction_manager/*optimistic*.go internal/txn/isolation/ 2>/dev/null || true
fi

# Copy from backup
echo "   - transaction_manager_bk/* → internal/txn/lock/*"
if [ -d "transaction_manager_bk" ]; then
    cp transaction_manager_bk/lock_manager.go internal/txn/lock/manager.go 2>/dev/null || true
    cp transaction_manager_bk/wait_for_graph.go internal/txn/lock/ 2>/dev/null || true
fi

# Copy logmanager
echo "   - logmanager → internal/txn/log"
if [ -d "logmanager" ]; then
    cp logmanager/*.go internal/txn/log/
fi

echo ""
echo "3. Move concurrency files..."
mkdir -p internal/concurrency
echo "   - replacer/* → internal/concurrency/*"
if [ -d "replacer" ]; then
    cp replacer/*.go internal/concurrency/
fi

echo ""
echo "4. Move CLI..."
echo "   - main.go → cmd/cli/main.go"
if [ -f "main.go" ]; then
    cp main.go cmd/cli/
fi

echo ""
echo "✅ Files copied to new structure!"
echo ""
echo "🗑️  Cleaning up old directories..."

# List of old directories to remove (preserving .vscode, .github, .git)
OLD_DIRS=(
    "buffermanager"
    "resource_page"
    "transaction_manager"
    "transaction_manager_bk"
    "logmanager"
    "replacer"
    "b_tree"
    "bplus_tree_backend"
    "lsm_tree_backend"
    "storage_engine"
)

for dir in "${OLD_DIRS[@]}"; do
    if [ -d "$dir" ]; then
        rm -rf "$dir"
        echo "   ✅ Removed $dir/"
    fi
done

# Remove old main.go if it exists
if [ -f "main.go" ]; then
    rm main.go
    echo "   ✅ Removed main.go"
fi

echo ""
echo "✅ Cleanup complete!"
echo ""
echo "⚠️  Next steps:"
echo "1. Update package names in new files (buffermanager → buffer, etc.)"
echo "2. Update all import paths throughout codebase"
echo "3. Run tests: go test ./..."
echo "4. Verify build: go build ./cmd/cli"
echo ""
echo "📚 See PROJECT_STRUCTURE.md for detailed migration guide."
