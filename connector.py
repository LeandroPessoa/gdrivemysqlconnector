from odo import odo
import pandas as pd 

"""
Google drive things...
"""

import httplib2
import os
import io
from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools
from apiclient.http import MediaIoBaseDownload
from googleapiclient.http import MediaFileUpload

def get_credentials():
	"""Gets valid user credentials from storage.

	If nothing has been stored, or if the stored credentials are invalid,
	the OAuth2 flow is completed to obtain the new credentials.

	Returns:
					Credentials, the obtained credential.
	"""
	home_dir = os.path.expanduser('~')
	credential_dir = os.path.join(home_dir, '.credentials')
	if not os.path.exists(credential_dir):
		os.makedirs(credential_dir)

	credential_path = os.path.join(credential_dir,
								   'drive-python-quickstart.json')

	store = oauth2client.file.Storage(credential_path)
	credentials = store.get()

	if not credentials or credentials.invalid:
		flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
		flow.user_agent = APPLICATION_NAME
		if flags:
			credentials = tools.run_flow(flow, store, flags)

		else:  # Needed only for compatibility with Python 2.6
			credentials = tools.run(flow, store)
		print('Storing credentials to ' + credential_path)

	return credentials

def load_file(filename):
	print(filename)
	credentials = get_credentials()
	http = credentials.authorize(httplib2.Http())
	drive_service = discovery.build('drive', 'v3', http=http)
	# obtem id de arquivo em buffer
	idbuffer = None
	page_token = None
	response = drive_service.files().list(q="name='"+filename+"'",
										  spaces='drive',
										  fields='nextPageToken, files(id, name, webContentLink)',
										  pageToken=page_token).execute()
	for file in response.get('files', []):
		# Se algo for encontrado será o arquivo desejado.

		print("Buffer encontrado no drive. ID:" + file.get('id'), 'info')
		idbuffer = file.get('id')
		link = file.get('webContentLink')
	file_id = idbuffer
	request = drive_service.files().export(fileId=file_id,
	                                             mimeType='text/csv')
	fh = io.BytesIO()

	downloader = MediaIoBaseDownload(fh, request)
	done = False
	while done is False:
	    status, done = downloader.next_chunk()
	    print ("Download %d%%." % int(status.progress() * 100))

	file = open(filename+'.csv','wb')
	file.write(fh.getvalue())
	file.close()

def convert(filename):
	file = pd.read_csv(filename+'.csv',index_col='nome')
	odo(file, 'postgresql://meta:klapaucius@localhost:5432/metabase_db::Despesas')


def main():
	# my code here
	load_file('Despesas')
	convert('Despesas')

if __name__ == "__main__":
	main()