#!/bin/bash

#
# This script adds a license header to all files in this repository.
# The license text is read from 'license_header.txt' and added at
# the beginning of each file.
# Each line of the license file is prefixed with a comment sign
# valid for respective source code.
#
# The files to extend with the license header are defined at the end
# of this script. Should a new type of the file appears in this repository
# the script must be extended.
#
# This script recognises if the header file is already present,
# and in this case it does not modify the file.
#
# However, the script does not allow at the moment for updating
# the license header.
#

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

# Add license header to all files in
# this directory and all sub-directories.
# Parameters:
#   file extension, e.g.: .go, .cpp,
#   a license text as a string
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