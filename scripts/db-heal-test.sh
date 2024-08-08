#!/bin/bash

###########################
#--- Dynamic variables ---#
###########################

# Aida paths
aida_path=''
aida_db_path=''
tmp_path=''

# Block variables
sync_block=20000
kill_block=11000
restore_block=10000
final_block=21000

##########################
#--- Static variables ---#
##########################

carmen_root=$(cd ../go && pwd)


################
#--- Script ---#
################

command="./build/aida-vm-sdb substate --validate --db-tmp "$tmp_path" --carmen-schema 5 --db-impl carmen --aida-db "$aida_db_path" --no-heartbeat-logging --track-progress --archive --archive-variant s5 --archive-query-rate 200 --carmen-cp-interval "$restore_block" 0 "$sync_block""

# Run the command in the background and redirect stdout and stderr to a log file
log_file="$(pwd)/output.log"
current=$(pwd)

cd $aida_path
$command &> "$log_file" &
command_pid=$!
cd $current

echo "Starting aida-vm-sdb with interrupt."

# Function to monitor the log file
monitor_log() {
  tail -F "$log_file" | while read LINE; do
    echo "$LINE" | grep -q "block $kill_block"
    if [ $? -eq 0 ]; then
      echo "Interrupting."
      kill $command_pid
      exit 0
    fi
  done
}

# Start monitoring the log file
monitor_log

# Wait for the command to complete
wait $command_pid

# Find working dir
working_dir=$(ls -td "$tmp_path"/*/ | head -1)
archive="${working_dir}archive"
live="${working_dir}live"

(cd $carmen_root && go run ./database/mpt/tool reset --force-unlock "$archive" "$restore_block")

genesis="${tmp_path}test_genesis.dat"

echo "Restoration complete. Exporting LiveDB genesis."
(cd $carmen_root && go run ./database/mpt/tool export --block "$restore_block" "$archive" "$genesis")

echo "Export complete. Applying LiveDB genesis."
rm -rf "$live"
(cd $carmen_root && go run ./database/mpt/tool import-live-db "$genesis" "$live")

echo "Syncing to block "$final_block""
final_first=$((restore_block+1))
cmd="./build/aida-vm-sdb substate --validate --db-tmp "$tmp_path" --carmen-schema 5 --db-impl carmen --aida-db "$aida_db_path" --no-heartbeat-logging --track-progress --archive --archive-variant s5 --archive-query-rate 200 --db-src "$working_dir" --skip-priming "$final_first" "$final_block""
(cd $aida_path && $cmd >> "$log_file")

echo "Sync complete to block $final_block. Final db path: $(ls -td "$tmp_path"/*/ | head -1)."

rm $log_file