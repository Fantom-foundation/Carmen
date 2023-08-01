# Introduction

TODO ...

# Migration for Open-Sourcing

Scripts are provided to migrate Carmen repository into a public repository.
These scripts remove all experimental, alternative and unfinished features 
and keep only to be productized ones. 

In particular, current version exports GoLang implementation of file-based Index/Store StateDB
with LevelDB Archive.

The migration script uses `git-filter-repo`, which must be installed. 
 * MacOs: `brew install git-filter-repo`

The script to migrate the repository is executed as:
```
./scripts/migration/migrate.sh
```

The script can be configured by modifying variables in the script:

* `REPO_DIR` - Temporary folder to checkout Carmen into, and use it as worskpace for modifications
* `SOURCE_REPO` - Source repository with Carmen
* `DEST_REPO` - Destination repository, where modified Carmen will be stored into.
* `SOURCE_BRANCH` - Git branch name to checkout specific version of Carmen.
* `DEST_BRANCH` - Git branch where the modified Carmen will be pushed into.





