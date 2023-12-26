# Greenplum Backup

`gpbackup` and `gprestore` are Go utilities for performing Greenplum Database backups.  They are still currently in active development.

## Pre-Requisites

The project requires the Go Programming language version 1.11 or higher. Follow the directions [here](https://golang.org/doc/) for installation, usage and configuration instructions.
The project also has a dependency on `sqlite3`. This is installed by default on many platforms, but you must install it on your system if it is not present.

## Downloading

```bash
go get github.com/greenplum-db/gpbackup/...
```

This will place the code in `$GOPATH/github.com/greenplum-db/gpbackup`.

## Building and installing binaries

Make the `gpbackup` directory your current working directory and run:

```bash
make depend
make build
```

The `build` target will put the `gpbackup` and `gprestore` binaries in `$HOME/go/bin`.

This will also attempt to copy `gpbackup_helper` to the greenplum segments (retrieving hostnames from `gp_segment_configuration`). Pay attention to the output as it will indicate whether this operation was successful.

`make build_linux` is for cross compiling on macOS, and the target is Linux.

`make install` will scp the `gpbackup_helper` binary (used with -single-data-file flag) to all hosts

## Validation and code quality

### Test setup

Required for Greenplum Database 6 or higher, several tests require the `dummy_seclabel` Greenplum contrib module. This module exists only to support regression testing of the SECURITY LABEL statement. It is not intended to be used in production. Use the following commands to install the module.

```bash
pushd $(find ~/workspace/gpdb -name dummy_seclabel)
    make install
    gpconfig -c shared_preload_libraries -v dummy_seclabel
    gpstop -ra
    gpconfig -s shared_preload_libraries | grep dummy_seclabel
popd

```

### Test execution

**NOTE**: The integration and end_to_end tests require a running Greenplum Database instance.

To run all tests except end-to-end (linters, unit, and integration), use
```bash
make test
```
To run only unit tests, use
```bash
make unit
```
To run only integration tests
```bash
make integration
```
Integration test requirements
 - Running GPDB instance
 - GPDB's gpcloud extension
```bash
make -C gpcontrib/gpcloud/ install
```
 - GPDB configured with `--with-perl`

To run end to end tests (requires a running GPDB instance), use
```bash
make end_to_end
```

**We provide the following targets to help developers ensure their code fits Go standard formatting guidelines.**

To run a linting tool that checks for basic coding errors, use
```bash
make lint
```
This target runs [gometalinter](https://github.com/alecthomas/gometalinter).

Note: The lint target will fail if code is not formatted properly.


To automatically format your code and add/remove imports, use
```bash
make format
```
This target runs [goimports](https://godoc.org/golang.org/x/tools/cmd/goimports) and [gofmt](https://golang.org/cmd/gofmt/).
We will only accept code that has been formatted using this target or an equivalent `gofmt` call.

## Running the utilities

The basic command for gpbackup is
```bash
gpbackup --dbname <your_db_name>
```

The basic command for gprestore is
```bash
gprestore --timestamp <YYYYMMDDHHMMSS>
```

Run `--help` with either command for a complete list of options.

## Cleaning up

To remove the compiled binaries and other generated files, run
```bash
make clean
```

# More Information

The Greenplum Backup [wiki](https://github.com/greenplum-db/gpbackup/wiki) for this project has several articles providing a more in-depth explanation of certain aspects of gpbackup and gprestore.

# How to Contribute

See [CONTRIBUTING.md file](https://github.com/greenplum-db/gpbackup/blob/master/CONTRIBUTING.md).

# Code Formatting

We use `goimports` to format go code. See https://godoc.org/golang.org/x/tools/cmd/goimports
The following command formats the gpbackup codebase excluding the vendor directory and also lists the files updated.
```bash
goimports -w -l $(find . -type f -name '*.go' -not -path "./vendor/*")
```

# Troubleshooting

## Dummy Security Label module is not installed or configured

If you see errors in many integration tests (below), review the
Validation and code quality [Test setup](#Test setup) section above:

```
SECURITY LABEL FOR dummy ON TYPE public.testtype IS 'unclassified';
      Expected
          <pgx.PgError>: {
              Severity: "ERROR",
              Code: "22023",
              Message: "security label provider \"dummy\" is not loaded",
```

## Tablespace already exists

If you see errors indicating the `test_tablespace` tablespace already
exists (below), execute `psql postgres -c 'DROP TABLESPACE
test_tablespace'` to cleanup the environment and rerun the tests.

```
    CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir'
    Expected
        <pgx.PgError>: {
            Severity: "ERROR",
            Code: "42710",
            Message: "tablespace \"test_tablespace\" already exists",
```
