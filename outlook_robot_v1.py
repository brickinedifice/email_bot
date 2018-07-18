# Set up
import re
import sys
import time
import random
import math
import os
import functools
from datetime import datetime
from itertools import combinations
import logging
#
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
#
from bs4 import BeautifulSoup
#
import pandas as pd
import html2text
import numpy as np
import openpyxl
_p = print

from ewspy.ewspy import EWS_Client

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
import httplib2
import base64
import email

_p = print

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Log into outlook
username = 'morgancreekcap\dbiswas'
password = 'xdr55%%TGB'
ews_client=EWS_Client(username=username, password=password)

# Define which spreadsheet to parse
spreadsheet_id = '1PZSYBQdyI78w9Fv1tHN7vNVE2YhXtgWSpiybyYnnmsc'

# user id used for in google api calls
user_id='dbiswas.mccm.bot@gmail.com'

# Get Google credentials
credentials = Storage('./.credentials/google_key_public.json').get()
http = credentials.authorize(httplib2.Http())
services={
    'spreadsheet':None,
    'gmail':None,
}

discovery_url= ('https://sheets.googleapis.com/$discovery/rest?version=v4')
services['spreadsheet'] = discovery.build('spreadsheet', 'v4', http=http, discoveryServiceUrl=discovery_url)
services['gmail'] = discovery.build('gmail', 'v1', http=http)

# Define which spreadsheet to parse
range_name = 'category_actions!A:E'

# get the data from category_actions tab
_result = services['spreadsheet'].spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()

# create a data frame
dbc_file = pd.DataFrame(_result['values'])

# update column names
dbc_file.columns = ['address_field', 'address_filter', 'text_field', 'text_filter', 'categories']

# show dbc_file shape
logger.info('dbc_file_shape:{0}'.format(dbc_file.shape))


_email_bot_folder_ids = {
    'delete_domain':'AAMkAGNkYTQxYTM0LTdmMzQtNDBmYS1hM2UxLWQ2ZDk5MjA1MThiZQAuAAAAAAD6JGJ10tgMRqwokUswdNnkAQBl7EvVNr0ETaJXUaYcxvBtAACQTI45AAA=',
    'delete_user':'AAMkAGNkYTQxYTM0LTdmMzQtNDBmYS1hM2UxLWQ2ZDk5MjA1MThiZQAuAAAAAAD6JGJ10tgMRqwokUswdNnkAQBl7EvVNr0ETaJXUaYcxvBtAACQTI+JAAA=',
    'delete_subject':'AAMkAGNkYTQxYTM0LTdmMzQtNDBmYS1hM2UxLWQ2ZDk5MjA1MThiZQAuAAAAAAD6JGJ10tgMRqwokUswdNnkAQBl7EvVNr0ETaJXUaYcxvBtAACQTI8hAAA=',
}


# move item folder id is the 00_FUND_ITEMS folder
move_dbc_delete_items_folder_id = 'AAMkAGNkYTQxYTM0LTdmMzQtNDBmYS1hM2UxLWQ2ZDk5MjA1MThiZQAuAAAAAAD6JGJ10tgMRqwokUswdNnkAQACVbg9tcIJQ5kcVEGq/yCrAAAAF+K9AAA='

email_domain_reg_exp = re.compile('.+@(.+)')

_email_generator = EWS_Client.get_all_items_in_folder(ews_client, 'FolderId', _email_bot_folder_ids['delete_domain'], query=None)

try:

    for emails in _email_generator:

        emails = EWS_Client.get_items(ews_client, emails)    

        for index, email in emails.iterrows():

            _email_address = EWS_Client.get_attribute_from_EWS_response(['From', 'Mailbox', 'EmailAddress'], email[1])

            try:

                _email_domain = email_domain_reg_exp.match(_email_address).groups()[0]

                _new_filter_for_google = ['from', _email_domain, '', '', 'DBC_DELETE']

                _new_filter_for_pandas = {
                    'address_field':'from', 
                    'address_filter':_email_domain, 
                    'text_field':'', 
                    'text_filter':'', 
                    'categories':'DBC_DELETE',
                }

                services['spreadsheet'].spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueInputOption='RAW',
                    insertDataOption='INSERT_ROWS',
                    body={'values':[_new_filter_for_google]},
                ).execute()

                dbc_file = dbc_file.append([_new_filter_for_pandas], ignore_index=True)

                ews_client.client.service.MoveItem(
                        ToFolderId={'FolderId':{'Id':move_dbc_delete_items_folder_id}},
                        ItemIds={'_value_1':[{'ItemId':{'Id':index}}]},
                        _soapheaders={'RequestVersion':{'Version':'Exchange2010_SP2'}},
                    )

                logger.debug('\radding_new_filter:{0}'.format(_new_filter_for_pandas))

            except IndexError as ie:

                logger.exception(ie)

            except TypeError as te:

                logger.exception(te)

# if generator is em
except StopIteration as _e:

    logger.exception(_e)


# drop the duplicates
dbc_file.drop_duplicates(inplace=True)

# save to CSV first just in case some thing goes wrong with writing to Google
dbc_file.to_csv('00_archives/00_dbc_files/dbc_file_{0}.csv'.format(datetime.now().strftime('%Y-%m-%d-%H-%M-%s')))

# try to update the spreadsheet
try:

    filter_rows = [[*row_data] for _, (row_data) in dbc_file.iterrows()]

    services['spreadsheet'].spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        body={},
    ).execute()

    services['spreadsheet'].spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption='RAW',
        insertDataOption='OVERWRITE',
        body={'values': filter_rows},
    ).execute()


except Exception as e:
    print(e)


def get_match_score(filter_row, *args, **kwargs):   
#     logger.debug(filter_row)

#     logger.debug(args)

#     logger.debug(kwargs)

    try:

        if filter_row.get('address_field') == 'to':

#             _t = pd.Series(kwargs.get('email_to')).apply(re.compile('.*{0}$'.format(filter_row.get('address_filter'))).match).count()

#             return len(kwargs.get('email_to')) * bool((_t))  

            # turning off this feature
            return False

        elif filter_row.get('address_field') == 'cc':

#             _t = pd.Series(kwargs.get('email_cc')).apply(re.compile('.*{0}$'.format(filter_row.get('address_filter'))).match).count()

#             return len(kwargs.get('email_cc')) * bool((_t))  

            # turning off this feature
            return False

        elif filter_row.get('address_field') == 'from':

            _from_match = re.compile('.*{0}$'.format(filter_row.get('address_filter'))).match(kwargs.get('email_from'))

            if filter_row.get('text_field') == 'subject':

                _subject_match = re.compile('^{0}.*'.format(filter_row.get('text_filter'))).match(kwargs.get('email_subject'))

#                 return (len(kwargs.get('email_from')) + len(kwargs.get('email_subject'))) * (bool(_from_match) and bool(_subject_match))
                return bool(_from_match) and bool(_subject_match)

            else:

                return bool(_from_match)

    except KeyError as ke:

        return False  


# In[9]:


def get_recipients(field, email):

    #dict of recipients
    _d = EWS_Client.get_attribute_from_EWS_response([field, '_value_1'], email)

    recipient_list = []

    if _d:

        for i in range(len(_d)):

            recipient_list = recipient_list + [EWS_Client.get_attribute_from_EWS_response([i, 'Mailbox', 'EmailAddress'], _d)]

    return recipient_list


dbc_file.loc[:, 'match_function'] = dbc_file.apply(lambda row:functools.partial(get_match_score, row.iloc[:4].to_dict()), axis=1)

def agg(categories):

#     logger.debug(categories)

    parse_categories = []

    try:
        for category in categories:   

            parse_categories += map(str.strip, category.split(','))

        parse_categories = ','.join(parse_categories)

        return parse_categories

    except Exception as e:

        logger.exception(e)

        return 'DBC_ERROR'


# # Get emails from outlook folders

# In[12]:


# check all flag
check_all_flag=True

# if check all flag is True then
if check_all_flag:

    # do not filter out emails that have been checked already
    _query=None

# check all if False
else:

    # set query so that those with DBC_CHECKED are not in the query result
    _query='categories: (NOT DBC_CHECKED)'


# Prepare dataframe
outlook_df=EWS_Client.get_all_items_in_folder(ews_client, 'DistinguishedFolderId', 'inbox', query=_query)

# datetime now
datetime_now=datetime.now()

for _next_set_of_emails in outlook_df:

    emails = EWS_Client.get_items(ews_client, _next_set_of_emails)

    for counter, (index, email) in enumerate(emails.iterrows()):

        sys.stderr.write('\r{:5d} of {:5d}:index:{}, date:{}:'.format(counter, len(emails), index, email[0]))

        try:

            _email_to = get_recipients('ToRecipients', email[1])

            _email_cc = get_recipients('CcRecipients', email[1])

            _email_from = EWS_Client.get_attribute_from_EWS_response(['From', 'Mailbox', 'EmailAddress'], email[1])

            _email_subject = EWS_Client.get_attribute_from_EWS_response(['Subject'], email[1])

            _email_id = EWS_Client.get_attribute_from_EWS_response(['ItemId', 'Id'], email[1])

            _email_change_key = EWS_Client.get_attribute_from_EWS_response(['ItemId', 'ChangeKey'], email[1])

    #         logger.info('\r{0}'.format(_email_subject))

            _t = dbc_file.loc[:, 'match_function'].apply(
                lambda row:row(
                    email_to =_email_to, 
                    email_cc = _email_cc, 
                    email_from = _email_from,
                    email_subject = _email_subject
                )) 

            new_categories = agg(dbc_file[_t]['categories'].values.tolist())

            new_categories = ['DBC_CHECKED'] + new_categories.split(',')

#             new_categories = new_categories.split(',')



            """
            logger.info('{0} || {1}'.format(
                new_categories, 
                EWS_Client.get_attribute_from_EWS_response(['Categories', 'String'], email[1]),
            ))
            """

            ews_client.client.service.UpdateItem(
                ConflictResolution='AlwaysOverwrite',
                MessageDisposition='SaveOnly',
                ItemChanges=[{
                    'ItemChange':{
                        'ItemId':{
                                'Id':_email_id,
                                'ChangeKey':_email_change_key,
                        },
                        'Updates':{
                            '_value_1':[{
                                'SetItemField':{
                                    'FieldURI':{'FieldURI':'item:Categories'},
                                    'Message':{'Categories':{'String':new_categories}},
                                },
                            }],
                        },
                    },
                }],
                _soapheaders={'RequestVersion':{'Version':'Exchange2010_SP2'}},
            )

            if ('DBC_DELETE' in new_categories) | ((datetime_now.date() - email[0].date()).days > 45):

#                 logger.info('Moving email: from={0}, subject={1}'.format(_email_from, _email_subject))
#                 logger.info('Matched dbc_file_rows:{0}'.format(dbc_file[_t]))

                ews_client.client.service.MoveItem(
                    ToFolderId={'FolderId':{'Id':move_dbc_delete_items_folder_id}},
                    ItemIds={'_value_1':[{'ItemId':{'Id':_email_id}}]},
                    _soapheaders={'RequestVersion':{'Version':'Exchange2010_SP2'}},
                )
        except Exception as e:

            logger.exception(e)


# # code samples
