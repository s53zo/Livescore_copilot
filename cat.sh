#!/usr/bin/env bash

# Overwrite or create an empty output file
> output.txt

# Find .py files modified in the last 10 minutes
# and loop through them, printing filename + contents.
#find . -type f -mmin -10 -name "*.py" | while read -r file; do
find . -type f -name "*.py" | while read -r file; do
  echo "===== $file =====" >> output.txt
  cat "$file" >> output.txt
  echo "" >> output.txt  # blank line for readability
done
