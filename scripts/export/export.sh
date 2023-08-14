#!/bin/bash

#
# This script migrates Carmen repository to its public version.
#
# It removes all experimental, alternative and unfinished features
# and keep only to be published ones.
#
# In particular, this version exports GoLang implementation of file-based Index/Store StateDB
# with LevelDB Archive database.
#
# This script checkouts fresh Carmen repository, filters out unnecessary parts and pushes
# resulting files into another repository.
#
# Input and output repositories as well as other configurations are customisable
# via constants at the beginning of this file.
#
# The script uses 'Git', and 'Git filter-repo' commands, which must
# be installed before running this script.
#
#
# Directories and files to be included in the output version
# are configured in the file 'filter.txt'.
#
# Directories and files in the directory 'extra-files' will
# be additionally added to the output version.
#
#

#
# Temporary folder to checkout Carmen into, and use it as worskpace for
# modifications.
#
REPO_DIR=${TMPDIR-/tmp}/_carmen_temp

#
# Source repository with Carmen.
#
SOURCE_REPO=git@github.com:Fantom-foundation/Carmen.git

#
# Git branch name to checkout specific version of Carmen.
#
SOURCE_BRANCH="kjezek/migration-scripts"

## Program starts here

#
# Clone the repo to a new directory
#
mkdir "$REPO_DIR" || exit
git clone $SOURCE_REPO "$REPO_DIR"

ORIGINAL_DIR=$(pwd)

cd "$REPO_DIR" || exit

#
# Checkout to a required branch,
# and remove link to origin
#
git checkout $SOURCE_BRANCH
git remote rm origin

#
# Filter out unnecessary parts
#
git filter-repo --force --path go/ --path-rename go/:
git filter-repo --force --paths-from-file "$ORIGINAL_DIR/scripts/export/filter.txt"

#
# Push to the new repository, while adding required extra files.
#
d=$(date)
cp -r "$ORIGINAL_DIR/scripts/export/extra-files/" .
git add -A
git commit -a -m "migrates to public repository at $d"

#
# Clean-up
#

cd "$ORIGINAL_DIR" || exit

echo "Result stored in '$REPO_DIR'"