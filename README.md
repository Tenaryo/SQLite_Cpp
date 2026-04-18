# TinySqlite

A minimal SQLite database file parser written in modern C++23. Reads `.db` files directly, traverses B-tree pages, and executes basic SQL queries — no SQLite library dependency.

## Features

- **SQLite binary format parsing** — reads page headers, cell pointers, varints, and serial types directly from the raw file format
- **B-tree traversal** — supports both interior and leaf pages for table and index B-trees
- **SQL query execution**
  - `SELECT col1, col2 FROM table`
  - `SELECT COUNT(*) FROM table`
  - `WHERE column = 'value'` filtering (full table scan and index scan)
- **Index scan optimization** — uses B-tree index search instead of full table scan when an index is available
- **Modern C++23** — `std::expected`, `std::ranges`, `std::unreachable()`, `constexpr`, designated initializers, structured bindings
- **Modular architecture** — separated into `util`, `sql_parser`, `database`, and `command` modules

## Requirements

- C++23 compiler (GCC 13+ / Clang 17+)
- CMake 3.21+
- Ninja (recommended)

## Build

```sh
./build.sh            # Debug build (default)
./build.sh Release    # Release build
```

Or manually:

```sh
cmake -B build -S . -G Ninja -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j$(nproc)
```

### Sanitizers

Enable Address Sanitizer and Undefined Behavior Sanitizer:

```sh
cmake -B build -S . -G Ninja -DENABLE_SANITIZERS=ON
cmake --build build -j$(nproc)
```

## Test

```sh
./run_tests.sh
```

Sample databases are needed for testing. Download them:

```sh
./download_sample_databases.sh
```

## Usage

```sh
./build/sqlite <database.db> "<command>"

# Meta commands
./build/sqlite sample.db ".dbinfo"
./build/sqlite sample.db ".tables"

# SQL queries
./build/sqlite sample.db "SELECT COUNT(*) FROM apples"
./build/sqlite sample.db "SELECT name, color FROM apples"
./build/sqlite sample.db "SELECT name FROM apples WHERE color = 'Yellow'"
```

## Project Structure

```
src/
  util.hpp          # Shared utilities (case-insensitive comparison, trim)
  sql_parser.hpp    # SQL SELECT statement parser
  database.hpp      # Core Database class (binary parsing, B-tree traversal)
  command.hpp       # Query execution and command dispatch
  main.cpp          # CLI entry point
tests/
  test_database.cpp # Test suite
```

## License

See [LICENSE](LICENSE).
