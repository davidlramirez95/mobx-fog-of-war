# Changelog

## 0.11.0

### Features

- **rxBatch**: Add parallel batch processing with `maxConcurrency` option
  - Changed from sequential (`concatMap`) to parallel (`mergeMap`) batch processing
  - New `maxConcurrency` option (default: 10) controls maximum concurrent batch requests
  - Set `maxConcurrency: 1` for previous sequential behavior
  - Significantly improves performance when many batches are processed simultaneously

### Breaking Changes

None. The default behavior now processes up to 10 batches concurrently instead of sequentially, which should be a performance improvement for most use cases. Set `maxConcurrency: 1` to restore previous sequential behavior if needed.

## 0.10.0 and earlier

See git history for previous changes.
