# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- Corrected accumulator type strings from 'INDIVIDUAL_OOP' to 'IND_OOP' in scenarios
- Removed duplicate execute function in db.py
- Fixed family rollup anomaly check to only run for subscribers
- Updated CI to use init_db.py for schema validation
- Consolidated constants into single source
- Fixed double-issue generation for null numeric fields
- Added date validation in filename parsing
- Removed redundant CSV reads in validation
- Added missing database indexes
- Fixed various exception handling and logging

### Added
- Case notes table to schema
- Timezone-aware datetime handling
- Comprehensive test suite (27+ test files)

### Changed
- Updated project structure documentation
- Moved runbooks to docs/
- Updated README test count

### Removed
- Empty files (pandas_utils.py, streamlit_app.py)
- Duplicate imports and functions