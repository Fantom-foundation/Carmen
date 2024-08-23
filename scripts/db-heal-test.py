import os
import subprocess
import tempfile
import time
import shutil
from pathlib import Path
import sys
import flag

# --- Dynamic variables --- #
aida_path_flag = flag.string("aida", "", "Path to Aida root.")
aida_db_path_flag = flag.string("aidadb", "", "Path to AidaDB.")
tmp_path_flag = flag.string("tmp", "", "Path to tmp dir.")

number_of_iterations_flag = flag.int("iter", 1000, "Number of iterations.")
incremental_block_flag = flag.int("increments", 1000, "Block size of each iteration - cannot be <200.")
flag.parse()

aida_path = aida_path_flag.value
aida_db_path = aida_db_path_flag.value
tmp_path = tmp_path_flag.value

number_of_iterations = number_of_iterations_flag.value
incremental_block = incremental_block_flag.value
if aida_path == "":
    print("please set Aida using --aida")
    exit(1)
if aida_db_path == "":
    print("please set AidaDB using --aidadb")
    exit(1)
if tmp_path == "":
    tmp_path = tempfile.gettempdir()
    print(f"tmp not set - using default {tmp_path}")

# Block variables
first_block = 0
last_block = 1000
kill_block = 900
restore_block = 800

# --- Static variables --- #

carmen_root = os.path.abspath('../go')

# --- Script --- #

# Log file path from which we read output to find kill_block
aida_log_file = Path.cwd() / 'aida.log'
carmen_log_file = Path.cwd() / 'carmen.log'
current_dir = Path.cwd()


# Function to monitor the log file and send terminate signal when kill_block occurs
def monitor_log():
    with aida_log_file.open('r') as f:
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.1)
                continue
            if f"block {kill_block}" in line:
                print("Interrupting.")
                process.terminate()
                return 0
            # If process ends with error (return code 1) or either 'fail' or 'exit status' occurs in line exit script
            if process.poll() == 1 or any(s in line for s in ["exit status", "fail"]):
                print("Error occurred - printing output.log:")
                with open(aida_log_file, 'r') as l:
                    text = l.read()
                    print(text)
                os.chdir(current_dir)
                return 1


# First iteration command
cmd = [
    './build/aida-vm-sdb', 'substate', '--validate',
    '--db-tmp', tmp_path, '--carmen-schema', '5', '--db-impl', 'carmen',
    '--aida-db', aida_db_path, '--no-heartbeat-logging', '--track-progress',
    '--archive', '--archive-variant', 's5', '--archive-query-rate', '200',
    '--carmen-cp-interval', '200', str(first_block), str(last_block)
]

os.chdir(aida_path)
with open(aida_log_file, 'w') as f:
    process = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)
os.chdir(current_dir)

print("Creating database with aida-vm-sdb...")

# Start monitoring the log file
status = monitor_log()

# Wait for the first command to complete
process.wait()

if status == 1:
    sys.exit(1)

# Find working directory
working_dir = max(Path(tmp_path).iterdir(), key=os.path.getmtime)
archive = working_dir / 'archive'
live = working_dir / 'live'

print("Testing db created, starting loop.")

# Dumb carmen's logs into a file to avoid spamming
c = open(carmen_log_file, 'r+')

for i in range(1, number_of_iterations + 1):
    last_working_dir = working_dir

    # Find working dir - Aida copies db-src
    working_dir = max(Path(tmp_path).iterdir(), key=os.path.getmtime)
    archive = working_dir / 'archive'
    live = working_dir / 'live'

    # Restore Archive
    result = subprocess.run(
        ['go', 'run', './database/mpt/tool', 'reset', '--force-unlock', str(archive), str(restore_block)],
        stdout=c,
        stderr=c,
        cwd=carmen_root)
    if result.returncode != 0:
        # Print carmen logs if interation fails
        text = c.read()
        print(text)
        sys.exit(1)

    # Export genesis to restore LiveDB
    genesis = Path(tmp_path) / 'test_genesis.dat'

    print(f"Restoration complete. Exporting LiveDB genesis block {restore_block}.")
    result = subprocess.run(
        ['go', 'run', './database/mpt/tool', 'export', '--block', str(restore_block), str(archive), str(genesis)],
        stdout=c,
        stderr=c,
        cwd=carmen_root)
    if result.returncode != 0:
        # Print carmen logs if interation fails
        text = c.read()
        print(text)
        sys.exit(1)

    # Restore LiveDB
    print("Export complete. Applying LiveDB genesis.")
    try:
        shutil.rmtree(live)
        print("Live directory removed successfully.")
    except FileNotFoundError:
        print(f"Directory {live} does not exist.")
        exit(1)
    except PermissionError:
        print(f"Permission denied to remove {live}.")
        exit(1)
    except Exception as e:
        print(f"An error occurred: {e}")
        exit(1)

    print("Importing LiveDB genesis.")
    result = subprocess.run(
        ['go', 'run', './database/mpt/tool', 'import-live-db', str(genesis), str(live)],
        stdout=c,
        stderr=c,
        cwd=carmen_root)
    if result.returncode != 0:
        # Print carmen logs if interation fails
        text = c.read()
        print(text)
        sys.exit(1)

    print(f"Iteration {i}/{number_of_iterations}")
    # We restored to block X, although we need to start the app at +1 block because X is already done
    first_block = restore_block + 1
    last_block += incremental_block
    restore_block += incremental_block
    kill_block += incremental_block

    print(f"Syncing to block {last_block}...")
    command = [
        './build/aida-vm-sdb', 'substate', '--validate',
        '--db-tmp', tmp_path, '--carmen-schema', '5', '--db-impl', 'carmen',
        '--aida-db', aida_db_path, '--no-heartbeat-logging', '--track-progress',
        '--archive', '--archive-variant', 's5', '--archive-query-rate', '200',
        '--carmen-cp-interval', '200', '--db-src', str(working_dir),
        '--skip-priming', str(first_block), str(last_block)
    ]

    os.chdir(aida_path)
    with open(aida_log_file, 'w') as f:
        process = subprocess.Popen(command, stdout=f, stderr=subprocess.STDOUT)
    os.chdir(current_dir)

    # Start monitoring the log file
    status = monitor_log()

    # Wait for the command to complete
    process.wait()

    if status == 1:
        sys.exit(1)

    if last_working_dir:
        print(f"Removing previous database {last_working_dir}")
        shutil.rmtree(last_working_dir, ignore_errors=True)

    genesis.unlink(missing_ok=True)
# Clear anything leftover

print(f"Clearing last database {working_dir} and log files.")
aida_log_file.unlink(missing_ok=True)
carmen_log_file.unlink(missing_ok=True)
shutil.rmtree(working_dir, ignore_errors=True)

sys.exit(0)
