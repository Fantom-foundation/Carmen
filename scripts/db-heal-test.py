import argparse
import os
import subprocess
import tempfile
import time
import shutil
from pathlib import Path
import sys

parser = argparse.ArgumentParser(prog="DB HEAL TEST SCRIPT",
                                 description="This script as serves as a test tool for 'db-heal' feature."
                                             "It tests recover and LiveDB export/import.")

# --- Parameters --- #
parser.add_argument('--aida', type=str, help="Path to Aida root.")
parser.add_argument('--aida-db', type=str, help="Path to AidaDB.")
parser.add_argument("--tmp", type=str, help="Path to tmp dir.")
parser.add_argument("--iter", type=int, help="Number of iterations.", default=1000)
parser.add_argument("--window", type=int,
                    help="Delay between start of sync process and forced termination (in seconds).", default=5)
parser.add_argument("--cp-granularity", type=int,
                    help="How often will Carmen create checkpoints (in blocks).", default=10)

args = parser.parse_args()

aida_path = args.aida
aida_db_path = args.aida_db
tmp_path = args.tmp
number_of_iterations = args.iter
window = args.window
checkpoint_granularity = args.cp_granularity

# Mark first checkpoint
latest_checkpoint = checkpoint_granularity

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
last_block = 60000000

# --- Static variables --- #

carmen_root = os.path.abspath('../go')

# --- Script --- #

# Log file path from which we read output to find kill_block
aida_log_file = Path.cwd() / 'aida.log'
carmen_log_file = Path.cwd() / 'carmen.log'
current_dir = Path.cwd()

print("Your settings:")
print(f"\tNumber of iterations: {number_of_iterations}.")
print(f"\tSync time before kill: {window} seconds.")
print(f"\tCheckpoint granularity: {checkpoint_granularity} blocks.")


# Function which checks programs return code, if program failed, log is printed and program is terminated.
def check_program_failure(code: int, log: str):
    if code != 0:
        print(log)
        sys.exit(1)


# Function which stops process after given sleep_time.
def terminate_process_after(sleep_time: int, checkpoint: int):
    start = 0.0
    with aida_log_file.open('r') as f:
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.1)
                continue
            if start > 0 and time.time() - start >= sleep_time:
                print("Interrupting...")
                process.terminate()
                return checkpoint
            if f"block {checkpoint + checkpoint_granularity}" in line:
                # First checkpoint was found, we should start timer as it means block processing is running.
                if start == 0.0:
                    start = time.time()
                checkpoint = checkpoint + checkpoint_granularity
                print(f"Found new checkpoint {checkpoint}.")  # TODO: Remove
            # If process ends with error (return code 1) or either 'fail' or 'exit status' occurs in line exit script
            if process.poll() == 1 or any(s in line for s in ["exit status", "fail"]):
                print("Error occurred - printing output.log:")
                with open(aida_log_file, 'r') as l:
                    text = l.read()
                    print(text)
                return -1


# First iteration command
cmd = [
    './build/aida-vm-sdb', 'substate', '--validate',
    '--db-tmp', tmp_path, '--carmen-schema', '5', '--db-impl', 'carmen',
    '--aida-db', aida_db_path, '--no-heartbeat-logging', '--track-progress',
    '--archive', '--archive-variant', 's5', '--archive-query-rate', '200',
    '--carmen-checkpoint-interval', str(checkpoint_granularity), '--tracker-granularity',
    str(checkpoint_granularity), str(first_block), str(last_block)
]

os.chdir(aida_path)
with open(aida_log_file, 'w') as f:
    process = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)
os.chdir(current_dir)

print("Creating database with aida-vm-sdb...")

# Start monitoring the log file
latest_checkpoint = terminate_process_after(window, latest_checkpoint)

# Wait for the first command to complete
process.wait()

if latest_checkpoint == -1:
    os.chdir(current_dir)
    sys.exit(1)

# Find working directory
working_dir = max(Path(tmp_path).iterdir(), key=os.path.getmtime)
archive = working_dir / 'archive'
live = working_dir / 'live'

print("Testing db created, starting loop.")

for i in range(1, number_of_iterations + 1):
    last_working_dir = working_dir

    # Find working dir - Aida copies db-src
    working_dir = max(Path(tmp_path).iterdir(), key=os.path.getmtime)
    archive = working_dir / 'archive'
    live = working_dir / 'live'

    # Dumb carmen's logs into a file to avoid spamming
    c = open(carmen_log_file, 'a+')

    # Restore Archive
    result = subprocess.run(
        ['go', 'run', './database/mpt/tool', 'reset', '--force-unlock', str(archive), str(latest_checkpoint)],
        stdout=c,
        stderr=c,
        cwd=carmen_root)

    log = c.read()
    print(log)

    check_program_failure(result.returncode, log)

    # Export genesis to restore LiveDB
    genesis = Path(tmp_path) / 'test_genesis.dat'

    print(f"Restoration complete. Exporting LiveDB genesis block {latest_checkpoint}.")
    result = subprocess.run(
        ['go', 'run', './database/mpt/tool', 'export', '--block', str(latest_checkpoint), str(archive), str(genesis)],
        stdout=c,
        stderr=c,
        cwd=carmen_root)
    check_program_failure(result.returncode, c.read())

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
    check_program_failure(result.returncode, c.read())

    print(f"Iteration {i}/{number_of_iterations}")
    # We restored to block X, although we need to start the app at +1 block because X is already done
    first_block = latest_checkpoint + 1

    print("Syncing restarted...")
    command = [
        './build/aida-vm-sdb', 'substate', '--validate',
        '--db-tmp', tmp_path, '--carmen-schema', '5', '--db-impl', 'carmen',
        '--aida-db', aida_db_path, '--no-heartbeat-logging', '--track-progress',
        '--archive', '--archive-variant', 's5', '--archive-query-rate', '200',
        '--carmen-checkpoint-interval', str(checkpoint_granularity), '--db-src',
        str(working_dir), '--skip-priming', '--tracker-granularity',
        str(checkpoint_granularity), str(first_block), str(last_block)
    ]

    os.chdir(aida_path)
    with open(aida_log_file, 'w') as f:
        process = subprocess.Popen(command, stdout=f, stderr=subprocess.STDOUT)
    os.chdir(current_dir)

    # Start monitoring the log file
    latest_checkpoint = terminate_process_after(window, latest_checkpoint)

    # Wait for the command to complete
    process.wait()

    if latest_checkpoint == -1:
        os.chdir(current_dir)
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

print("Success!")

sys.exit(0)


