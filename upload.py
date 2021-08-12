import datetime
from datetime import date
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import gspread
from gspread_dataframe import set_with_dataframe 

import requests
import json
import os
import jobs
import pandas as pd
from dotmap import DotMap


# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']

def upload(filename, job):
    
    print("Uploading file to Google Drive...")
    
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    creds = None
    
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
            
    
    access_token = creds.token
    folder_id = job.drive_folder
    
    filepath = f'./{job.output}/{filename}'

    filesize = os.path.getsize(filepath)

    # 1. Retrieve session for resumable upload.
    headers = {"Authorization": "Bearer "+access_token, "Content-Type": "application/json"}
    params = {
        "name": f"{filename}",
        "parents": [folder_id],
        "mimeType": "text/csv"
    }
    r = requests.post(
        "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable",
        headers=headers,
        data=json.dumps(params)
    )
    location = r.headers['Location']

    # 2. Upload the file.
    headers = {"Content-Range": "bytes 0-" + str(filesize - 1) + "/" + str(filesize)}
    r = requests.put(
        location,
        headers=headers,
        data=open(filepath, 'rb')
    )
    if r.status_code == '200':
        print('yes!')

def publish(df, job):
    
    print("Publishing spreadsheet...")
    
    # ACCESS GOOGLE SHEET
    # Credential files stored in ~/.config/gspread as credentials.json and authorized_user.json
    # Need to set up in Docker file
    
    date_str = df['date'].max().strftime('%m-%d-%Y')
    todate = datetime.datetime.utcnow().date().strftime('%m-%d-%Y')
    
    gc = gspread.oauth()
    sh = gc.open_by_key(job.sheet_id)
    
    worksheet = sh.get_worksheet(0) #-> 0 - first sheet, 1 - second sheet etc. 
        
    
    # CLEAR SHEET CONTENT
    worksheet.clear()
    worksheet.update_title(f'Last updated: {todate}')
    
    # APPEND DATA TO SHEET
    if job.type == 'subgraphs':
        
        cols = jobs.columns_sheet[job.type]
        df_sheet = (df.loc[(df['date'] == df['date'].max()) & (df['active'] == True), cols]
                    .sort_values(['curator_apr_30d_estimate', 'signal'], ascending=False))
        
        set_with_dataframe(worksheet, df_sheet) #-> THIS EXPORTS YOUR DATAFRAME TO THE GOOGLE SHEET
        
        worksheet.format('A1:U1', {'textFormat': {'bold': True}})
        worksheet.freeze(rows=1)
        worksheet.format('D:S', {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}})
        worksheet.format('D:E', {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})
        worksheet.format('J:J', {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})
        worksheet.format('S:U', {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})
        worksheet.format('C:C', {"numberFormat": {"type": "DATE", "pattern": "yyyy-mm-dd"}})
        worksheet.format('H:I', {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}})
        worksheet.format('K:M', {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}})
        worksheet.format('O:R', {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}})

    elif job.type == 'global':
        
        cols = jobs.columns_sheet[job.type]
        df_sheet = df[cols].sort_values('date', ascending=False)
        
        set_with_dataframe(worksheet, df_sheet) #-> THIS EXPORTS YOUR DATAFRAME TO THE GOOGLE SHEET
        
        worksheet.format('A1:AL1', {'textFormat': {'bold': True}})
        worksheet.freeze(rows=1)
        worksheet.format('B:AG', {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}})
        
        for colrange in ['F:F', 'V:X', 'AE:AG']:
            worksheet.format(colrange, {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}})
        
        worksheet.format('AH:AH', {"numberFormat": {"type": "PERCENT", "pattern": "0.0000%"}})    
        worksheet.format('AI:AL', {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})
        worksheet.format('A:A', {"numberFormat": {"type": "DATE", "pattern": "yyyy-mm-dd"}})
    
def main():
    df = pd.read_csv('outputs/global_07_20_2021.csv', parse_dates=['date'])
    
    job = DotMap(jobs.jobs[1])
    
    publish(df, job)

if __name__ == "__main__":
    main()