#!/bin/bash

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <hdfs_dir> <output_file>"
  exit 1
fi

HDFS_DIR="$1"
OUTPUT_FILE="$2"

> "$OUTPUT_FILE"

hdfs dfs -ls "$HDFS_DIR" | awk '{print $8}' | while read -r file; do
  if [ -n "$file" ] && hdfs dfs -test -f "$file"; then
    hdfs dfs -cat "$file" >> "$OUTPUT_FILE"
  fi
done

echo "All files from $HDFS_DIR written to $OUTPUT_FILE"