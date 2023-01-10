#!/bin/python3
import os
import re
import collections

# This script walks log files from Aida run-vm TeamCity build configuration
# stored in the "data" directory (downloaded using download.py script) and
# constructs CSV tables comparing the total time (totalTime.csv)
# and txs/sec (txsPerSec.csv) metrics from individual Aida runs (log files).

configs = set()
buildNumbers = set()
totalTimeTable = collections.defaultdict(dict)
buildTxsTable = collections.defaultdict(dict)
txsPerSecTable = collections.defaultdict(dict)
commentsTable = collections.defaultdict(dict)

for root, dirs, files in os.walk('data'):
	for i, file in enumerate(files):
		if file[-8:] == '.comment':
			delim = file.find('.')
			buildNumber = file[:delim]
			with open('data/' + file, 'r') as f:
				commentsTable[int(buildNumber)] = f.read()
		if file[-4:] != '.log':
			continue
		delim = file.find('-')
		buildNumber = file[:delim]
		config = file[delim+1:-4]
		print(buildNumber + ' ' + config)
		
		with open('data/' + file, 'r') as f:
			content = f.read()
			x = re.search('Total elapsed time: ([0-9\.]*) s, processed ([0-9\.]*) blocks \(~ ([0-9\.]*) Tx/s\)', content)
			if x == None:
				print('unrecognized total time')
				continue
			print(x.group(0))
			totalTime = x.group(1)
			txsCount = x.group(2)
			txsPerSec = x.group(3)
			configs.add(config)
			buildNumbers.add(int(buildNumber))
			totalTimeTable[config][int(buildNumber)] = totalTime
			buildTxsTable[int(buildNumber)] = txsCount
			txsPerSecTable[config][int(buildNumber)] = txsPerSec

buildNumbersSorted = sorted(buildNumbers)
configsSorted = sorted(configs)

with open('totalTime.csv', 'w') as f:
	f.write("build;txsCount;comment;" + ";".join(configsSorted) + "\n")
	for buildNumber in buildNumbersSorted:
		f.write(str(buildNumber) + ";" + buildTxsTable[buildNumber] + ";")
		if buildNumber in commentsTable:
			f.write(commentsTable[buildNumber])
		for config in configsSorted:
			try:
				f.write(";" + totalTimeTable[config][buildNumber])
			except KeyError:
				f.write(";")
		f.write("\n")

with open('txsPerSec.csv', 'w') as f:
	f.write("build;txsCount;comment;" + ";".join(configsSorted) + "\n")
	for buildNumber in buildNumbersSorted:
		f.write(str(buildNumber) + ";" + buildTxsTable[buildNumber] + ";")
		if buildNumber in commentsTable:
			f.write(commentsTable[buildNumber])
		for config in configsSorted:
			try:
				f.write(";" + txsPerSecTable[config][buildNumber])
			except KeyError:
				f.write(";")
		f.write("\n")

