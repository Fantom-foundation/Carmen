#!/bin/bash
set -e

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

command="./build/aida-vm-sdb substate --validate --db-tmp "$tmp_path" --carmen-schema 5 --db-impl carmen --aida-db "$aida_db_path" --no-heartbeat-logging --track-progress --archive --archive-variant s5 --archive-query-rate 200 0 "$sync_block""

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

(cd $carmen_root && go run ./database/mpt/tool reset --force-unlock "$working_dir"archive "$restore_block")

echo "Restoration complete. Syncing to block $final_block."

final_first=$((restore+1))
(cd $aida_path && ./build/aida-vm-sdb substate --validate --db-tmp "$tmp_path" --carmen-schema 5 --db-impl carmen --aida-db "$aida_db_path" --no-heartbeat-logging --track-progress --archive --archive-variant s5 --archive-query-rate 200 --db-src "$working_dir" "$final_first" "$final_block")

echo "Sync complete to block $final_block. Final db path: $(ls -td "$tmp_path"/*/ | head -1)."