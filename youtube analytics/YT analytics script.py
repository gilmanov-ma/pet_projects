from googleapiclient.discovery import build
import pandas as pd
import requests
import google_auth_oauthlib.flow
import googleapiclient.discovery
import google.oauth2.credentials
import os
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
import json
import psycopg2
import gspread
from sqlalchemy import create_engine
from oauth2client.service_account import ServiceAccountCredentials


# Функция для скачивания данных по ключу Youtube Data API
def get_channel_stats():
    API_KEY = YOUR_API_KEY
    api_service_name = "youtube"
    api_version = "v3"
    channelId = YOUR CHANNEL
    service = googleapiclient.discovery.build(api_service_name, api_version, developerKey=API_KEY)
    response_items = service.search().list(
        channelId=channelId,
        part="snippet",
        type='video',
        maxResults="50",
    ).execute()

    video_ids = []
    next_page_token = response_items.get('nextPageToken')
    status = True

    while status:
        if next_page_token is None:
            status = False
        response_items = service.search().list(
            channelId=channelId,
            part="snippet",
            type='video',
            maxResults="50",
            pageToken=next_page_token
        ).execute()

        for i in range(len(response_items['items'])):
            video_ids.append(response_items['items'][i]['id']['videoId'])

        next_page_token = response_items.get('nextPageToken')
    all_video = []
    for i in range(0, len(video_ids), 50):
        request = service.videos().list(
            part="snippet,statistics",
            id=','.join(video_ids[i: i + 50])
        )
        response = request.execute()
        for video in response['items']:
            video_sum = dict(video_id=video['id'],
                             title=video['snippet']['title'],
                             published_at=video['snippet']['publishedAt'])
            all_video.append(video_sum)
    return all_video

# Функция для скачивания данных по ключу oauth2 Youtube Analytics
def oauth_request():
    api_service_name = 'youtubeAnalytics'
    version = 'v2'
    credentials = cred_saves()
    youtube_analytics = googleapiclient.discovery.build(api_service_name, version, credentials=credentials)
    request = youtube_analytics.reports().query(
        dimensions="video",
        endDate="2022-11-13",
        ids="channel==MINE",
        maxResults=200,
        metrics="estimatedMinutesWatched,views,likes,subscribersGained,comments,averageViewDuration,cardClicks,cardTeaserClicks,cardImpressions",
        sort="-estimatedMinutesWatched",
        startDate="2021-10-12"
    )
    response = request.execute()
    video_details = []
    for i in range(len(response['rows'])):
        video_metrics = dict(video_id=response['rows'][i][0],
                            estimatedMinutesWatched=response['rows'][i][1],
                            views=response['rows'][i][2],
                            likes=response['rows'][i][3],
                            subscribersGained=response['rows'][i][4],
                            comments=response['rows'][i][5],
                            averageViewDuration=response['rows'][i][6],
                            cardClicks=response['rows'][i][7],
                            cardTeaserClicks=response['rows'][i][8],
                            cardImpressions=response['rows'][i][9])
        video_details.append(video_metrics)

    return video_details

# Функция для автоматической авторизации владельца канала
def cred_saves():
    scopes = ['https://www.googleapis.com/auth/youtube',
              'https://www.googleapis.com/auth/yt-analytics.readonly',
              'https://www.googleapis.com/auth/youtube.readonly',
              'https://www.googleapis.com/auth/yt-analytics-monetary.readonly',
              'https://www.googleapis.com/auth/youtubepartner'
    ]
    path = 'token.json'
    creds = None
    MY_OAUTH_TOKEN = 'client_secret_483785681501.json'
    if os.path.exists(path):
        creds = Credentials.from_authorized_user_file(path, scopes)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(MY_OAUTH_TOKEN, scopes)
            creds = flow.run_local_server(port=5000)
    # Save the credentials for the next run
    with open(path, 'w') as token:
        token.write(creds.to_json())
    return creds

# Функция для автоматического формирования отчета в Google Sheets
def insert_into_sheets():
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file",
             "https://www.googleapis.com/auth/drive"]
    df_basic_info = get_channel_stats()
    df_basic_info = pd.DataFrame(df_basic_info)
    df_detail_info = oauth_request()
    df_detail_info = pd.DataFrame(df_detail_info)
    df = pd.merge(df_basic_info, df_detail_info, on='video_id', how='inner')
    df.to_csv('video_data.csv', index=False)
    credentials = ServiceAccountCredentials.from_json_keyfile_name('youtube-stat-367815-9208f10cec8b.json', scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open('yt_dashboard')

    with open('video_data.csv', 'r') as file_obj:
        content = file_obj.read()
        client.import_csv(spreadsheet.id, data=content)


# Функция для добавления данных в postgresql
def insert_into_db():
    print('Началось обновление базы...')
    df_basic_info = get_channel_stats()
    df_basic_info = pd.DataFrame(df_basic_info)
    df_detail_info = oauth_request()
    df_detail_info = pd.DataFrame(df_detail_info)
    df = pd.merge(df_basic_info, df_detail_info, on='video_id', how='inner')
    engine = create_engine('postgresql://postgres:password@localhost:5432/YT')
    df.to_sql('detail_info', con=engine, if_exists='replace', index=False)
    print('Обновление базы завершено')


if __name__ == '__main__':
    insert_into_sheets()

