for file in *; do
    # Skip directories to avoid errors
    if [ -f "$file" ]; then
      echo "===== $file =====" >> output.txt
      cat "$file" >> output.txt
      echo "" >> output.txt   # Add a blank line
    fi
done
