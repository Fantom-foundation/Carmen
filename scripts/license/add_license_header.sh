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

# resolve the directory of the script, no matter where it is called from
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)

# resolve the root directory of the project
root_dir=$(readlink -f "$script_dir/../..")

# Extend the license text of input string, each line is prefixed.
# It is used for extending the license text of comments to be inserted
# in a source file. 
# Parameters:
#   character to use for comments, e.g.: //, #,
extend_license_header() {
    local comment_char="$1"    

    # Read the license header from the file
    local license_header=$(cat "$script_dir/$license_file")

    # Extend each line of the license header with the specified character
    local extended_license_header=""
    while IFS= read -r line; do
        extended_license_header+="$comment_char $line\n"
    done <<< "$license_header"

    # return
    echo "$extended_license_header"
}

# Add license header to all files in project
# root directory and all sub-directories.
# Parameters:
#   file extension, e.g.: .go, .cpp,
#   comment prefix, e.g.: //, #,
add_license_to_files() {
    local file_extension="$1"
    local prefix="$2"
    local license_header="$(extend_license_header "$prefix")"

    # Get a list of all files in the project directory
    mapfile -t -d $'\0' all_files < <(find "$root_dir" -type f -name "*$file_extension" -print0)

    # Iterate over all files and add the license header if needed
    for f in "${all_files[@]}"; do
        # iterate over each line of the license header
        # and validate that it is present in the file
        # on the same line number, the presumption is that
        # the license header is at the beginning of the file
        local line_number=1
        local add_header=false
        while read -r line; do
            # compare the line from the license header with the line in the file on the same line number
            # whitespaces are trimmed (from the beginning and end of the line)
            if [[ "$(sed "$line_number!d" "$f" | xargs echo -n)" != "$(echo "$line" | xargs echo -n)" ]]; then
                add_header=true
                break
            fi
            line_number=$((line_number+1))
        done <<< "$(echo -e "$license_header")"

        # if the license header matched so far, check following line in the file,
        # it should be empty or contain only whitespaces
        if [[ $add_header == false ]]; then
            if [[ -n "$(sed "$line_number!d" "$f" | xargs echo -n)" ]]; then
                add_header=true
            fi
        fi

        # header should be added
        if [[ $add_header == true ]]; then
            # extract first line number, that does not match the license header prefix
            # in case obsolete header is present, the script will skip it
            start_from=$(grep -vnE "^$prefix" "$f" | cut -d : -f 1 | head -n 1)
            # if start_from is greater than 1, then the file contains obsolete header and we should
            # continue from `start_from + 1`, so that we don't leave double line endings
            if [[ $start_from -gt 1 ]]; then
                start_from=$((start_from+1))
            fi
            # shellcheck disable=SC2086
            echo -e "$license_header\n$(tail -n +$start_from $f)" > "$f"
        fi
    done
}

add_license_to_files ".go" "//"
add_license_to_files "Jenkinsfile" "//"
add_license_to_files ".h" "//"
add_license_to_files ".cc" "//"
add_license_to_files "go.mod" "//"
add_license_to_files ".yml" "#"
add_license_to_files "BUILD" "#"