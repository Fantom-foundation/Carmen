#!/bin/python3
import collections

import requests
from requests.auth import HTTPBasicAuth
import os
import xmltodict

# This scripts downloads benchmark results from specified jobs in Jenkins CI into "data" directory.

# This script needs environment properties USER and TOKEN with the Jenkins user and Access Token.
# The Access Token can be generated in Jenkins in Profile - Configure - API Token - Add new token.

USER = os.getenv('USER')
TOKEN = os.getenv('TOKEN')
auth = HTTPBasicAuth(USER, TOKEN)
headers = {
    'Content-Type': 'application/xml'
}

jenkins = 'https://xapi194.fantom.network'
jobs = [
    'CompareMainnet8M',
    'CompareMainnet44M',
    'CompareMainnet22M',
    'GoFileMainnet50M',
    'CppFileMainnet50M',
]

for job in jobs:
    jobUrl = jenkins + '/job/Aida/job/' + job
    os.makedirs('data/' + job, exist_ok=True)

    response = requests.get(jobUrl + '/api/xml?depth=1', headers=headers, auth=auth)
    if response.status_code != 200:
        print(response.content)
        quit(1)
    jobData = xmltodict.parse(response.content)

    # iterate builds
    for i, build in enumerate(jobData['workflowJob']['build']):
        if not(type(build) is collections.OrderedDict):
            continue
        buildId = build['id']
        print(buildId)

        # get console output into XX-phase.log
        response = requests.get(jobUrl + '/' + buildId + '/consoleText', auth=auth)
        stagesLogs = response.content.split(b'\n[Pipeline] stage\n')
        for stageLog in stagesLogs:
            nameStart = stageLog.find(b'(') + 1
            nameEnd = stageLog.find(b')')
            if nameStart == 0:
                continue
            name = buildId + '-' + stageLog[nameStart:nameEnd].decode() + '.log'
            print(' * ' + name)
            with open('data/' + job + '/' + name, 'wb') as f:
                f.write(stageLog)

        # get artifacts
        if 'artifact' in build:
            for j, artifact in enumerate(build['artifact']):
                name = artifact['relativePath']
                print(' * ' + name)

                if os.path.exists('data/' + job + '/' + name):
                    continue

                response = requests.get(jobUrl + '/' + buildId + '/artifact/' + name, auth=auth)
                with open('data/' + job + '/' + name, 'wb') as f:
                    f.write(response.content)

print('Downloading complete')
