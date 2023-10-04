package main

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Options settable from command line
var testDir string = "."

// Path to global init file for gpdiff.pl
//
// This contains a global set of match/subs to apply across all test cases
var initFile string

// gpdiff.pl options for comparing actual results file with expected
//
// Unlike postgres/pg_regress, PXF includes
//
//	-w ignore all white space
//	-B ignore changes lines are all blank
//
// TODO: rather than match/sub DETAIL (GP5) for CONTEXT (see global_init_file), should we add "-I DETAIL:" and "-I CONTEXT:"
// TODO: rather than having to add start_ignore/end_ignore, should we add "-I HINT:"
var baseDiffOpts []string = []string{"-w", "-B", "-I", "NOTICE:", "-I", "GP_IGNORE", "-gpd_ignore_headers", "-U3"}

// internal variables
var gpdiffProg string
var tests []string

// summary file of all diffs
var regressionDiffsName string
var regressionDiffs *os.File

var successCount int = 0
var failCount int = 0

var logger *log.Logger

func init() {
	logger = log.New(os.Stdout, "pxf_regress ", log.LstdFlags|log.Lmicroseconds)
}

func validateArguments(args []string) {
	if len(args) < 2 {
		logger.Fatal("missing required argument: test-directory")
	}

	testDir = os.Args[1]
	tests = listTestQueries(testDir)

	gpdiffProg = findFile("gpdiff.pl", true)
	initFile = findFile("global_init_file", false)
}

func main() {
	validateArguments(os.Args)
	createResultFiles()
	initializeEnvironment()

	// Ready to run the tests
	logger.Printf("running regression queries in %s", testDir)
	for _, t := range tests {
		runTest(t)
	}

	// Emit nice-looking summary message
	summary := getSummary(successCount, failCount)
	logger.Println(summary)

	if failCount > 0 {
		logger.Fatalf("the differences that caused some tests to fail can be viewed in the file %q", regressionDiffsName)
	}

	deleteResultsFiles()
}

func runPsql(testname string) (string, string) {
	now := time.Now()
	inputSql := fmt.Sprintf("%s/sql/%s.sql", testDir, testname)
	outputFile := fmt.Sprintf("%s/output/%s_%s.out", testDir, testname, formatTimestamp(now))
	expectFile := fmt.Sprintf("%s/expected/%s.ans", testDir, testname)

	f, err := os.Create(outputFile)
	if err != nil {
		logger.Fatalf("unable to create file %s: %s", outputFile, err.Error())
	}
	defer f.Close()

	cmd := exec.Command("psql", "-X", "-a", "-f", inputSql)
	logger.Printf("running %q", cmd.String())
	cmd.Env = append(os.Environ(), "PGAPPNAME=pxf_regress/"+testname)
	cmd.Stdout = f
	cmd.Stderr = f
	err = cmd.Run()

	// err is nil if the command runs, has no problems copying stdin, stdout, and stderr, and exits with a zero exit status
	if err != nil {
		// err is of type *ExitError if the command starts but does not complete successfully
		exiterr := &exec.ExitError{}
		if !errors.As(err, &exiterr) {
			logger.Fatalf("cannot run psql command: %s\n", err.Error())
		}
		logger.Fatalf("psql command exited with non-zero status (%d): %s\n", exiterr.ExitCode(), err.Error())
	}

	return outputFile, expectFile
}

func directoryExists(dir string) bool {
	f, err := os.Stat(dir)
	if err == nil {
		return f.IsDir()
	}

	if !errors.Is(err, fs.ErrNotExist) {
		logger.Fatalf("could not stat directory %q: %s", dir, err.Error())
	}

	return false
}

func createDirectory(directory string) {
	if directoryExists(directory) {
		return
	}

	// make the directory with rwxr-xr-x permissions
	if err := os.Mkdir(directory, 0755); err != nil {
		logger.Fatalf("could not create directory %q: %s", directory, err.Error())
	}
}

// Find a file in our binary's directory and verify it is readable and
// optionally verify it is executable
//
// Unlike postgres/pg_regress, we do not verify its version since the gpdiff.pl
// in PXF exits with an error when run with '-v'
func findFile(target string, ensureExecutable bool) string {
	// Trim off program name and keep just directory and append the other
	// programs name
	executable, err := os.Executable()
	if err != nil {
		logger.Fatalf("cannot get path name for executable: %s", err.Error())
	}
	filePath := path.Join(path.Dir(executable), target)

	// Ensure that the file exists and is a regular file
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		logger.Fatalf("check for %q failed: %s", filePath, err.Error())
	}
	fileMode := fileInfo.Mode()
	if !fileMode.IsRegular() {
		logger.Fatalf("check for %q failed: not a regular file", filePath)
	}

	// Ensure that file is readable by its owner
	if fileMode&0400 == 0 {
		logger.Fatalf("check for %q failed: cannot read file (permission denied)", filePath)
	}

	// Ensure that file is executable by its owner
	if ensureExecutable && fileMode&0100 == 0 {
		logger.Fatalf("check for %q failed: cannot execute file (permission denied)", filePath)
	}

	return filePath
}

// Create the summary-output files (making them empty if already existing)
func createResultFiles() {
	// Create the diffs file as empty
	regressionDiffsName = fmt.Sprintf("%s/regression.diffs", testDir)
	f, err := os.Create(regressionDiffsName)
	if err != nil {
		logger.Fatalf("could not open file %q for writing: %s", regressionDiffsName, err.Error())
	}
	regressionDiffs = f
	// We don't keep the diffs file open continuously
	regressionDiffs.Close()

	// Also create the results directory if not present
	resultsDir := fmt.Sprintf("%s/output", testDir)
	createDirectory(resultsDir)
}

// Prepare environment variables for running regression tests
func initializeEnvironment() {
	// Set default application name. The test function may choose to
	// override this, but if it doesn't, we have something useful in place.
	os.Setenv("PGAPPNAME", "pxf_regress")

	// Set timezone and datestyle for datetime-related tests
	//
	// Unlike postgres/pg_regress, PXF's existing expected test outputs
	// have date and timestamps formatted with "ISO, MDY" instead of
	// "Postgres, MDY". Additionally, we do not explicitly set a time zone
	// for pxf_regress because this can cause pxf_regress and the PXF
	// service to be in different time zones, resulting in spurious diff;
	// instead we let pxf_regress and the PXF service use the time zone set
	// by the environment.
	os.Setenv("PGDATESTYLE", "ISO, MDY")

	// Set translation-related settings to English; otherwise psql will
	// produce translated messages and produce diffs.
	os.Setenv("LC_MESSAGES", "C")
	os.Unsetenv("PGCLIENTENCODING")
	os.Unsetenv("LANGUAGE")
	os.Unsetenv("LC_ALL")

	// Report what we're connecting to
	pgHost := os.Getenv("PGHOST")
	pgPort := os.Getenv("PGPORT")
	logger.Println(getConnectionSummary(pgHost, pgPort))
}

func getConnectionSummary(pgHost string, pgPort string) string {
	var connectionDetails strings.Builder
	if pgHost != "" {
		fmt.Fprintf(&connectionDetails, "using postmaster on %s", pgHost)
	} else {
		connectionDetails.WriteString("using postmaster on Unix socket")
	}

	if pgPort != "" {
		fmt.Fprintf(&connectionDetails, ", port %s", pgPort)
	} else {
		connectionDetails.WriteString(", default port")
	}

	return connectionDetails.String()
}

func runTest(test string) {
	startTime := time.Now()
	resultFile, expectFile := runPsql(test)

	// Compare actual results file with expected
	var outcome string
	if resultsDiffer(resultFile, expectFile) {
		outcome = "failed"
		failCount += 1
	} else {
		outcome = "ok"
		successCount += 1
	}

	logger.Printf("test %s ...%s %8d ms", test, outcome, time.Now().Sub(startTime).Milliseconds())
}

// Check the actual result file for the given test against the expected results
//
// Returns true if different (failure), false if they match.
// In the true case, the diff is appended to the diffs file.
func resultsDiffer(resultsFile string, expectFile string) bool {
	diffOpts := baseDiffOpts
	if initFile != "" {
		diffOpts = append(diffOpts, "--gpd_init", initFile)
	}
	diffOpts = append(diffOpts, resultsFile, expectFile)

	cmd := exec.Command(gpdiffProg, diffOpts...)
	logger.Printf("running %q", cmd.String())
	diffOutput, err := cmd.CombinedOutput()

	// if err is nil, the command exited with a zero exit status
	// exit status of diff is 0 if inputs are the same
	if err == nil {
		return false
	}

	// if the command starts but does not complete successfully,
	// the error is of type *ExitError
	exiterr := &exec.ExitError{}
	if !errors.As(err, &exiterr) {
		logger.Fatalf("diff command failed: %s\n", err.Error())
	}

	// exit status of diff is 1 if different, 2 if trouble
	if exiterr.ExitCode() > 1 {
		logger.Fatalf("diff command failed (%d): %s", exiterr.ExitCode(), exiterr.Error())
	}

	diffResults := strings.Replace(resultsFile, ".out", ".diff", 1)
	err = os.WriteFile(diffResults, diffOutput, 0644)
	if err != nil {
		logger.Fatalf("could not open file to write %q: %s", regressionDiffsName, err.Error())
	}

	// append to summary 'results.diff'
	summaryDiff, err := os.OpenFile(regressionDiffsName, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		logger.Fatalf("could not open file to write %q: %s", regressionDiffsName, err.Error())
	}
	defer summaryDiff.Close()

	diffHeader := fmt.Sprintf("diff %s %s %s\n", strings.Join(diffOpts, " "), resultsFile, expectFile)
	summaryDiff.Write([]byte(diffHeader))
	summaryDiff.Write(diffOutput)

	return true
}

// Return a list of test names found in the given directory
//
// the returned test names are the file names without the '.sql' extension
func listTestQueries(testDir string) []string {
	sqlDir := path.Join(testDir, "sql")
	if !directoryExists(sqlDir) {
		logger.Fatalf("directory %s does not exist", sqlDir)
	}

	// List all files with a ".sql" extension in sql/
	sqlFiles, err := filepath.Glob(path.Join(sqlDir, "*.sql"))
	if err != nil {
		logger.Fatalf("listing '.sql' files in %q failed: %s", testDir, err.Error())
	}

	// Remove ".sql" extension from file names
	n := len(".sql")
	tests := make([]string, len(sqlFiles))
	for i, sqlFilePath := range sqlFiles {
		sqlFileBase := path.Base(sqlFilePath)
		tests[i] = sqlFileBase[0 : len(sqlFileBase)-n]
	}

	// Sort the tests in ascending order
	sort.Strings(tests)

	return tests
}

func getSummary(successCount int, failCount int) string {
	if failCount == 0 {
		return fmt.Sprintf("all %d tests passed. ", successCount)
	}

	return fmt.Sprintf("%d of %d tests failed. ", failCount, successCount+failCount)
}

const nanosecondsPerMicrosecond = 1000

// Helper function to format timestamp including microseconds without '.'
//
// Go's time.Format cannot create strings with millisecond precision without
// '.' between
// the seconds and milliseconds so we have to manually remove it.
func formatTimestamp(t time.Time) string {
	microsecond := t.Nanosecond() / nanosecondsPerMicrosecond
	formatted := fmt.Sprintf("%04d%02d%02d%02d%02d%02d%06d",
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second(), microsecond)

	return formatted
}

func deleteResultsFiles() {
	os.Remove(regressionDiffsName)
}
