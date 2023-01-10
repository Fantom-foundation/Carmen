#!/bin/python3
import requests
import os
import xmltodict

# This scripts downloads all artifacts from the build configuration in TeamCity
# into "data" directory.

# This script needs enviroment property TOKEN with the TeamCity Access Token.
# The Access Token can be generated in Teamcity in Profile - Access Tokens - Create access token.
# Required Permission scope is "View build runtime parameters and data" for project Aida.

TOKEN = os.getenv('TOKEN')
headers = {
    'Authorization': 'Bearer ' + TOKEN,
    'Content-Type': 'application/xml'
}

response = requests.get('https://team.fantom.network/app/rest/builds/?locator=buildType:Aida_RunVmComparison&fields=build(id,number,status,running,startDate,comment,artifacts(file(name,content)))', headers=headers)

if response.status_code != 200:
	print(response.content)
	quit(1)

dict_data = xmltodict.parse(response.content)

for i, build in enumerate(dict_data['builds']['build']):
	print(build['@number'])
	
	if 'comment' in build:
		print(build['comment']['text'])
		with open('data/' + build['@number'] + '.comment', 'w') as f:
			f.write(build['comment']['text'])
	
	for j, artifact in enumerate(build['artifacts']['file']):
		name = build['@number'] + '-' + artifact['@name']
		print(' * ' + name + ': ' + artifact['content']['@href'])
		
		if os.path.exists('data/' + name):
			continue
		
		response = requests.get('https://team.fantom.network' + artifact['content']['@href'], headers=headers)
		with open('data/' + name, 'wb') as f:
			f.write(response.content)

print('Downloading complete')

