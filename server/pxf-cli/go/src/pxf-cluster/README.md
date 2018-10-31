# `pxf cluster` CLI

## Getting Started

1. Ensure you are set up for PXF development by following the README.md at the
   root of this repository.

1. Go to the pxf-cluster folder and install dependencies
   ```
   cd pxf/cli/go/src/pxf-cluster
   make depend
   ```

1. Run the tests
   ```
   make test
   ```

1. Build the CLI
   ```
   make
   ```

## Adding New Dependencies

1. Import the dependency in some source file (otherwise `dep` will refuse to install it)

2. Add the dependency to Gopkg.toml

3. Run `make depend`.