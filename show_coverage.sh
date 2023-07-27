#!/bin/bash

RESULTS="/tmp/results.out"

# The actual coverage numbers appear on lines in the form "coverage: ##.#% of statements in [package]", so the grep and awk statements parse those out and record them
# Note that this statement only generates coverage for integration tests that are actually executed, so e.g. running this against a 7 cluster won't show coverage for 5- or 6-specific code sections
go test -coverpkg=./backup,./filepath,./history,./options,./report,./restore,./toc,./utils ./integration -coverprofile=/tmp/coverage.out 2> /dev/null | grep "coverage:" | awk '{print "integration tests:\t" $2}' > $RESULTS

for PACKAGE in "backup" "filepath" "history" "options" "report" "restore" "toc" "utils" ; do
  # Generate unit test coverage statistics for all packages, write the statistics to a file, and print the coverage percentage to the shell
  go test -coverpkg "./$PACKAGE" "./$PACKAGE" -coverprofile="/tmp/unit_$PACKAGE.out" | grep "coverage:" | awk '{print $9 " unit tests:\t" $5}' >> $RESULTS
  # Filter out the first "mode: set" line from each coverage file and concatenate them all
  cat "/tmp/unit_$PACKAGE.out" | awk '{if($1!="mode:") {print $1 " " $2 " " $3}}' >> /tmp/coverage.out
done

# Print the total coverage percentage and generate a coverage HTML page
go tool cover -func=/tmp/coverage.out | awk '{if($1=="total:") {print $1 "\t\t\t" $3}}' >> $RESULTS
cat $RESULTS
rm $RESULTS
rm /tmp/unit_*.out
exit 0

# To display the results in HTML format, run the following command:
#
#	go tool cover -html=/tmp/coverage.out
