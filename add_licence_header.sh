#!/bin/bash

license_file="license_header.txt"

# Extend the license text of input string, each line is prefixed.
# It is used for extending the license text of comments to be inserted
# in a source file. 
# Parameters:
#   character to use for comments, e.g.: //, #,
extend_license_header() {
    local comment_char="$1"    

    # Read the license header from the file
    local license_header=$(cat "$license_file")

    # Extend each line of the license header with the specified character
    local extended_license_header=""
    while IFS= read -r line; do
        extended_license_header+="$comment_char $line\n"
    done <<< "$license_header"

    # return
    echo "$extended_license_header"
}

# Add license header to all files in a project
# Parameters:
#   file extension, e.g.: .go, .cpp,
#   a licence text as a string
add_license_to_files() {
    local file_extension="$1"
    local license_header="$2"

    # Get a list of all files in the project directory
    local all_files=($(find "." -type f -name "*$file_extension"))

    # Iterate over each file and add the license header
    for f in "${all_files[@]}"; do
        # Check if the license header is already present
        if ! grep -qz "$license_header" "$f"; then
            # Add the license header to the beginning of the file
            echo -e "$license_header\n$(cat "$f")" > "$f"
        fi
    done
}

add_license_to_files ".go" "$(extend_license_header '//')"
add_license_to_files "Jenkinsfile" "$(extend_license_header '//')"
add_license_to_files ".h" "$(extend_license_header '//')"
add_license_to_files ".cc" "$(extend_license_header '//')"
add_license_to_files "go.mod" "$(extend_license_header '//')"
add_license_to_files ".yml" "$(extend_license_header '#')"
add_license_to_files "BUILD" "$(extend_license_header '#')"