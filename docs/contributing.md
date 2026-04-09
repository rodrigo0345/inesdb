# Contributing

Thank you for your interest in contributing to InesDB! This guide will help you get started.

## Getting Started

### Prerequisites

- Go 1.25 or later
- Git
- Linux, macOS, or WSL2 on Windows

### Setting Up Development Environment

```bash
# Clone the repository
git clone https://github.com/rodrigo0345/omag-db.git
cd omag-db

# Install dependencies
go mod download

# Run tests to verify setup
go test ./...
```

## Development Workflow

### Branch Naming Convention

- Feature: `feature/description`
- Bug fix: `fix/description`
- Documentation: `docs/description`
- Refactoring: `refactor/description`

Example: `feature/add-bloom-filter` or `fix/deadlock-detection`

### Commit Messages

Follow conventional commits format:

```
<type>(<scope>): <subject>

<body>

<footer>
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `refactor`: Code restructuring
- `test`: Adding or updating tests
- `perf`: Performance improvements
- `ci`: CI/CD changes

Example:
```
feat(buffer): implement clock replacement policy

Implement the clock algorithm for page replacement as an
alternative to LRU. Reduces overhead while maintaining
good performance for typical workloads.

Closes #42
```

## Testing

### Running Tests

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Run specific test
go test -run TestName ./path/to/package

# Run benchmarks
go test -bench=. ./internal/concurrency/
```

### Test Coverage Requirements

- Minimum 80% code coverage required
- All public APIs must have tests
- Focus on edge cases and error conditions

### Writing Tests

```go
func TestFeatureName(t *testing.T) {
    // Arrange: Setup test data
    buffer := NewBufferPool(1000)

    // Act: Execute the feature
    page, err := buffer.GetPage(1)

    // Assert: Verify results
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if page == nil {
        t.Error("expected page, got nil")
    }
}

func BenchmarkOperation(b *testing.B) {
    buffer := NewBufferPool(1000)
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        buffer.GetPage(pageID(i))
    }
}
```

## Code Style

### Go Style Guide

We follow the official [Go Code Review Comments](https://golang.org/doc/effective_go) and [Google's Go Style Guide](https://google.github.io/styleguide/go/).

Key Points:
- Use `gofmt` before committing
- Keep functions small and focused
- Document exported functions/types
- Use meaningful variable names
- Avoid unused variables and imports

### Example

```go
// Good: Descriptive, follows conventions
func (bpm *BufferPoolManager) GetPage(pageID PageID) (*Page, error) {
    if pageID == InvalidPageID {
        return nil, ErrInvalidPageID
    }

    cached, ok := bpm.cache[pageID]
    if ok {
        cached.increaseRefCount()
        return cached, nil
    }

    return bpm.loadFromDisk(pageID)
}

// Bad: Unclear, doesn't follow conventions
func (b *BufferPoolManager) get(p int) *Page {
    x, _ := b.m[p]
    if x != nil {
        x.rc++
    }
    return x
}
```

## Documentation

### Code Comments

- Document all exported functions, types, and constants
- Include examples in documentation comments
- Explain the "why", not just the "what"

```go
// PageID represents a unique identifier for a database page.
// Valid PageID values start from 1; 0 is reserved as InvalidPageID.
type PageID uint32

// NewBufferPoolManager creates a new buffer pool manager with the specified
// frame count. It initializes the LRU replacement policy and creates a disk
// manager for persistent storage.
//
//  Example:
//   bpm, err := NewBufferPoolManager(1000)
//   if err != nil {
//       log.Fatal(err)
//   }
//   page, err := bpm.GetPage(1)
//
func NewBufferPoolManager(frameCount int) (*BufferPoolManager, error) {
    // ...
}
```

### Documentation Files

When adding significant features:
1. Update relevant architecture documentation
2. Add API reference if introducing new interfaces
3. Include examples and use cases
4. Document configuration parameters

## Performance Considerations

### Benchmarking

Before submitting performance-critical changes:

```bash
# Run benchmarks
go test -bench=. -benchmem ./path/to/package

# Compare benchmarks
go test -bench=. -benchmem ./path/to/package > new.txt
# Use benchstat tool to compare: benchstat old.txt new.txt
```

### Guidelines

- Minimize allocations in hot paths
- Use sync.Pool for frequently allocated objects
- Profile code using pprof: `go test -cpuprofile=cpu.prof`
- Reduce lock contention in concurrent code

## Pull Request Process

1. **Fork and Create Branch**
   ```bash
   git checkout -b feature/your-feature
   ```

2. **Make Changes**
   - Write tests for new functionality
   - Update documentation
   - Run `go fmt`, `go vet`, `golint`

3. **Push and Create PR**
   ```bash
   git push origin feature/your-feature
   ```

4. **PR Checklist**
   - [ ] Tests added/updated
   - [ ] Documentation updated
   - [ ] Code follows style guide
   - [ ] No new linter warnings
   - [ ] Commit messages follow convention
   - [ ] Coverage ≥ 80%

5. **Review Process**
   - Address reviewer comments
   - Push updates (don't force push to preserve history)
   - Request re-review when ready

## Reporting Issues

### Bug Reports

Include:
- Go version (`go version`)
- OS and architecture
- Reproduction steps
- Expected vs actual behavior
- Error messages and stack traces

```markdown
## Bug Report

### Environment
- Go: 1.26.1
- OS: Linux x86_64

### Description
Page eviction causes panic in cache validation

### Steps to Reproduce
1. Create buffer pool with 10 frames
2. Insert 20 different pages
3. Access page 1 again

### Expected Behavior
Page 1 should be restored from disk

### Actual Behavior
Panic: invalid page state

### Error Output
```
panic: invalid page state
goroutine 10 [running]:
...
```

### Feature Requests

Include:
- Motivation and use case
- Proposed solution (if any)
- Alternatives considered
- Expected performance impact

## Resources

- [Go Official Documentation](https://golang.org/doc/)
- [Effective Go](https://golang.org/doc/effective_go)
- [Google Go Style Guide](https://google.github.io/styleguide/go/)
- [Database Systems Papers](https://github.com/rodrigo0345/omag-db/tree/main/docs/papers)

## Community

- Ask questions in GitHub Issues
- Join discussions for design decisions
- Help review other PRs

Thank you for contributing to InesDB! 🙏
