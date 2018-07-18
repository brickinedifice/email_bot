from __future__ import print_function
from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import datetime

# Setup the Sheets API
SCOPES = 'https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/gmail.modify'
store = file.Storage('.credentials/google_key_public.json')
creds = store.get()
if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets('.credentials/google_key_private.json', SCOPES)
    creds = tools.run_flow(flow, store)
service = build('gmail', 'v1', http=creds.authorize(Http()))
service = build('sheets', 'v4', http=creds.authorize(Http()))
