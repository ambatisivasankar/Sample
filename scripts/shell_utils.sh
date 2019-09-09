###############################################################
## Setup the virtual environment

# Stop at any error, treat unset vars as errors and make pipelines exit
# with a non-zero exit code if any command in the pipeline exits with a
# non-zero exit code.
set -o nounset
set +e
set +x

### Print message and exit 1
die() {
  >&2 printf '%s\n' "${1}"
  exit 1
}
export -f die

### Print a line the length of the terminal
printLine() {
  echo "------------------------------------------------------------------------"
}
export -f printLine

### Print a line the length of the terminal with a message
### Line before message, or line after message
printLineMsg() {
  if [ "$1" = "before" ]; then
    printLine
    echo "$2"
  elif [ "$1" = "after" ]; then
    echo "$2"
    printLine
  fi
}
export -f printLineMsg
