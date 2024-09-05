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
                                             "It tests recover and LiveDB export/import.",
                                 usage="To use this script, please provide Aida root using --aida and path to"
                                       "AidaDb using --aida-db.")

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

if not aida_path or aida_path == "":
    print("please set Aida using --aida")
    sys.exit(1)
if not aida_db_path or aida_db_path == "":
    print("please set AidaDB using --aida-db")
    sys.exit(1)
if not tmp_path or tmp_path == "":
    tmp_path = tempfile.gettempdir()
    print(f"tmp not set - using default {tmp_path}")

# Block variables
first_block = 0
last_block = 60000000

carmen_root = os.path.abspath('../go')

# --- Script --- #

# Create working dir which gets deleted after the run
working_dir = os.path.join(tmp_path, 'db-heal-test')
if os.path.exists(working_dir):
    shutil.rmtree(working_dir)
os.makedirs(working_dir)

# Log file path from which we read output to find kill_block
aida_log_file = Path.cwd() / 'aida.log'
carmen_log_file = Path.cwd() / 'carmen.log'
genesis = os.path.join(working_dir, 'test_genesis.dat')

current_dir = Path.cwd()
carmen_root = os.path.join(current_dir, 'go')
if "Carmen/scripts" in str(current_dir):
    carmen_root = os.path.abspath('../go')

print("Your settings:")
print(f"\tNumber of iterations: {number_of_iterations}.")
print(f"\tSync time before kill: {window} seconds.")
print(f"\tCheckpoint granularity: {checkpoint_granularity} blocks.")


# Function which checks programs return code, if program failed, log is printed and True is returned.
def has_program_failed(code, log):
    if code != 0:
        log.close()
        with open(carmen_log_file, 'r') as l:
            text = l.read()
            print(text)
        return True
    return False


# Function which checks every line added to aida_log_file and behaves accordingly to the line.
def check_aida_log(sleep_time: int, checkpoint: int):
    start = 0.0
    with open(aida_log_file, 'r') as f:
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
            # If process ends with error (return code 1) or either 'fail' or 'exit status' occurs in line exit script
            if process.poll() == 1 or any(s in line for s in ["exit status", "fail"]):
                print("Error occurred - printing output.log:")
                with open(aida_log_file, 'r') as l:
                    text = l.read()
                    print(text)
                return -1


# Function which runs Carmen's info command and finds the latest checkpoint from created log
def get_latest_checkpoint_from_info():
    log = os.path.join(tmp_path, 'carmen-info.log')
    cp: str
    with open(log, 'w') as cl:
        r = subprocess.run(
            ['go', 'run', './database/mpt/tool', 'info', str(archive)],
            stdout=cl,
            stderr=cl,
            cwd=carmen_root)
        if has_program_failed(r.returncode, cl):
            shutil.rmtree(log)
            return -1

    with open(log, 'r') as cl:
        info_checkpoint = cl.readlines()[-1]
        # Return last word which is the block number
        cp = info_checkpoint.split()[-1]

    os.remove(log)
    return cp


# Function which tries to reset archive on archive_path to block reset_block. If command fails, false is returned.
def reset_archive(cl, archive_path, reset_block):
    return subprocess.run(
        ['go', 'run', './database/mpt/tool', 'reset', '--force-unlock', str(archive_path), str(reset_block)],
        stdout=cl,
        stderr=cl,
        cwd=carmen_root)


# First iteration command
cmd = [
    './build/aida-vm-sdb', 'substate', '--validate',
    '--db-tmp', working_dir, '--carmen-schema', '5', '--db-impl', 'carmen',
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
latest_checkpoint = check_aida_log(window, latest_checkpoint)

# Wait for the first command to complete
process.wait()

if latest_checkpoint == -1:
    os.chdir(current_dir)
    sys.exit(1)

# Find db directory
working_db = max(Path(working_dir).iterdir(), key=os.path.getmtime)
archive = os.path.join(working_db, 'archive')
live = os.path.join(working_db, 'live')

print("Testing db created, starting loop.")

has_failed = False

for i in range(1, number_of_iterations + 1):
    last_working_db = working_db

    # Find working dir - Aida copies db-src
    working_db = max(Path(working_dir).iterdir(), key=os.path.getmtime)
    archive = os.path.join(working_db, 'archive')
    live = os.path.join(working_db, 'live')

    # Dump carmen's logs into a file to avoid spamming
    c = open(carmen_log_file, 'w')

    block = int(latest_checkpoint)
    # Restore Archive
    result = reset_archive(c, archive, latest_checkpoint)
    if has_program_failed(result.returncode, c):
        print("Error occured during reset - looking for checkpoint using 'info' command...")
        # When checkpoint is required on an empty block, checkpoint is moved backwards
        # hence we must ask db what's the latest checkpoint.
        recovery_block = get_latest_checkpoint_from_info()
        if recovery_block == -1:
            has_failed = True
            break
        # Restore Archive again
        c = open(carmen_log_file, 'w')
        result = reset_archive(c, archive, recovery_block)
        if has_program_failed(result.returncode, c):
            # Next error is fatal
            has_failed = True
            break
        print(f"Success! Using block {recovery_block}")
        block = int(recovery_block)

    # Export genesis to restore LiveDB
    print(f"Restoration complete. Exporting LiveDB genesis block {block}.")
    result = subprocess.run(
        ['go', 'run', './database/mpt/tool', 'export', '--block', str(block), str(archive), str(genesis)],
        stdout=c,
        stderr=c,
        cwd=carmen_root)
    if has_program_failed(result.returncode, c):
        has_failed = True
        break

    # Restore LiveDB
    print("Export complete. Applying LiveDB genesis.")
    shutil.rmtree(live)

    result = subprocess.run(
        ['go', 'run', './database/mpt/tool', 'import-live-db', str(genesis), str(live)],
        stdout=c,
        stderr=c,
        cwd=carmen_root)
    if has_program_failed(result.returncode, c):
        has_failed = True
        break

    print(f"Iteration {i}/{number_of_iterations}")
    # We restored to block X, although we need to start the app at +1 block because X is already done
    first_block = block + 1

    print("Syncing restarted...")
    command = [
        './build/aida-vm-sdb', 'substate', '--validate',
        '--db-tmp', working_dir, '--carmen-schema', '5', '--db-impl', 'carmen',
        '--aida-db', aida_db_path, '--no-heartbeat-logging', '--track-progress',
        '--archive', '--archive-variant', 's5', '--archive-query-rate', '200',
        '--carmen-checkpoint-interval', str(checkpoint_granularity), '--db-src',
        str(working_db), '--skip-priming', '--tracker-granularity',
        str(checkpoint_granularity), str(first_block), str(last_block)
    ]

    os.chdir(aida_path)
    with open(aida_log_file, 'w') as f:
        process = subprocess.Popen(command, stdout=f, stderr=subprocess.STDOUT)
    os.chdir(current_dir)

    # Start monitoring the log file
    latest_checkpoint = check_aida_log(window, latest_checkpoint)

    # Wait for the command to complete
    process.wait()

    if latest_checkpoint == -1:
        os.chdir(current_dir)
        has_failed = True
        break

# Clear anything leftover
print(f"Clearing work directory {working_dir}.")
shutil.rmtree(working_dir, ignore_errors=True)

if has_failed:
    print("Fail")
    sys.exit(1)

print("Success!")
sys.exit(0)
