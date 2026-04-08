# Documentation Versioning

OMAG documentation supports multiple versions, allowing you to view documentation for different releases of the storage engine.

## Accessing Different Versions

### Version Selector

When browsing the documentation site, you'll see a version selector in the top navigation bar. Click it to view:

- **Latest**: Current development version on `main` branch
- **Stable Releases**: Official tagged releases (v0.1.0, v0.2.0, etc.)

The version selector appears in the header alongside the search functionality.

### Direct Version URLs

You can also access specific versions directly:

```
https://rodrigo0345.github.io/omag/                    # Latest (main branch)
https://rodrigo0345.github.io/omag/0.1.0/              # Version 0.1.0
https://rodrigo0345.github.io/omag/0.2.0/              # Version 0.2.0
```

## Release Documentation

Each release has its own complete documentation including:

- Architecture documentation for that version
- API reference matching that release
- Feature documentation for available features
- Configuration options for that version

### What Changes Between Versions

- **New Features**: Documented in latest version
- **Deprecated APIs**: Marked in newer versions
- **Bug Fixes**: Noted in release section
- **Breaking Changes**: Highlighted in migration guides

## Version Management

### How Versions Are Created

When you push a git tag to main:

```bash
git tag -a v0.1.0 -m "Release version 0.1.0"
git push origin v0.1.0
```

The CI/CD pipeline automatically:

1. Builds documentation for that version
2. Publishes it under `/omag/0.1.0/` path
3. Updates the version selector
4. Keeps older versions accessible

### Setting a Version as Default

The latest version on `main` is always set as the default landing page. When you release a new stable version, it becomes the default documentation visitors see.

## Migration Between Versions

Each version includes information about:

- **Upgrading**: How to upgrade from previous versions
- **Compatibility**: Breaking changes and migrations needed
- **Deprecations**: APIs/features being removed in future versions

### Finding Migration Guides

Look for "Upgrade Guide" or "Migration Guide" in:
- Architecture documentation
- API reference changes
- Feature deprecation notices

## Development vs. Stable

### Latest (Development)

- Documentation from `main` branch
- Contains new features in development
- May be unstable or subject to change
- Used for cutting-edge integration

URL: `https://rodrigo0345.github.io/omag/latest/`

### Stable Releases

- Official tagged versions
- Thoroughly tested and released
- Recommended for production use
- API stability guarantees (within major version)

URL: `https://rodrigo0345.github.io/omag/v0.1.0/`

## Using Versioned APIs

When integrating OMAG, always reference the specific version's documentation:

```bash
# Clone specific version
git checkout v0.1.0

# Import Go package (version independent)
import "github.com/rodrigo0345/omag/internal/storage/btree"

# Refer to documentation for that version
# https://rodrigo0345.github.io/omag/0.1.0/
```

## Documentation Quality Assurance

Each version's documentation is:

- ✅ Generated from specific code version
- ✅ Tested for broken links
- ✅ Validated for API accuracy
- ✅ Kept immutable after release

## Version Lifecycle

```
Development (main)
    ↓
    ├─ v0.1.0 (release)
    ├─ v0.2.0 (release)
    ├─ v1.0.0 (major release)
    └─ ... (more versions)
    
Each version has its own documentation path
All versions remain accessible
```

## Deprecation Policy

### How Deprecations Work

1. **Deprecated in**: Feature marked as deprecated
2. **Documentation**: Warns in that version's docs
3. **Duration**: Remains for typically 3-4 releases or 1+ year
4. **Removed in**: Finally removed in future major version

### Finding Deprecation Info

In API documentation, look for:

```go
// Deprecated: Use NewBufferPoolManager instead of NewBufferPool
// Deprecated since: v0.2.0
// Remove in: v1.0.0
func NewBufferPool() { }
```

## Accessing Changelog

Each version documentation includes:

- **Release Notes**: What changed in that version
- **Upgrade Path**: Steps to upgrade from previous version
- **Breaking Changes**: APIs that changed
- **New Features**: Additions in that version

## Archive and Long-Term Support

### How Long Versions Are Kept

- All releases remain accessible indefinitely
- Documentation never changes after release
- Can always refer to old version docs

### LTS (Long-Term Support) Versions

Certain versions may be marked as LTS:

- v0.1.0-LTS
- v1.0.0-LTS

These receive:
- Extended security support
- Additional documentation updates
- Compatibility guarantees

## Building Local Documentation for Specific Version

```bash
# Checkout specific version
git checkout v0.1.0

# Install dependencies
pip install -r docs/requirements-docs.txt

# Build documentation
mkdocs serve -f docs/mkdocs.yml

# View at http://localhost:8000
```

## Troubleshooting Version Issues

### "API Not Found" Error

- Check which version's documentation you're reading
- API may not exist in that version
- Try newer or older version
- Check migration guide for your version

### Version Mismatch

When code and documentation versions don't match:

1. Identify your OMAG version: `git describe --tags`
2. Visit documentation for that version
3. Report issue if documentation is outdated

### Missing Older Versions

All versions should be accessible. If not:

1. Check GitHub releases page
2. Verify git tags: `git tag -l`
3. Report missing version on GitHub Issues

## Contributing Version Updates

When contributing to documentation:

- **Latest only**: PRs update `main` branch only
- **Release notes only**: Add to release when tagging
- **No backporting**: Old versions never updated
- **Future consistency**: Changes apply to next release

---

**Version Documentation Deployed**: Every push to `main` and every release tag automatically generates and deploys documentation with proper versioning support.

**Current Versions Available**: Check the version selector in the navigation bar to see all available versions.
