import slack
import os
import json
from datetime import datetime
from s3_handler.s3Handler import read_file_in_s3

S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
S3_FOLDER_NAME = os.environ.get('S3_FOLDER_NAME')
S3_FILE_NAME = os.environ.get('S3_FILE_NAME')

bucket_key = f'{S3_FOLDER_NAME}/{S3_FILE_NAME}'

def find_todays_holidays():
    today_date = datetime.today().strftime('%Y-%m-%d')
    text_to_print = []
    holidays_file = read_file_in_s3(bucket_key)
    if (holidays_file[str(today_date)]):
        num_of_holidays = len(holidays_file[str(today_date)])
        holiday_word = 'holiday' if num_of_holidays == 1 else 'holidays'

        text_to_print.append(f'Today, the world celebrates {num_of_holidays} {holiday_word} \n')
        count = 1
        for holidays in holidays_file[str(today_date)]:
            holiday_name = holidays['holiday_name']
            country_name = holidays['country_name']
            text_to_print.append(f'{count}. Holiday: {holiday_name} | Country: {country_name} \n')
            count += 1
    print(f"text_to_print - {text_to_print}")
    return text_to_print

def print_message_to_slack(channel='', text_to_print=''):
    client = slack.WebClient(token=os.environ['SLACK_TOKEN'])
    client.chat_postMessage(channel=channel, text=" ".join(text_to_print)    )

def handler(event, context):
    try:
        holidays_today_text = find_todays_holidays()
        slack_channel='#holiday-reminder'
        if len(holidays_today_text):
            print_message_to_slack(channel=slack_channel, text_to_print=holidays_today_text)
            return {
                "statusCode": 200, 
                "body": f"Successfully sent a message to {slack_channel}"
            }
        else:
            return {
                "statusCode": 200, 
                "body": f"No holidays today, no message sent to {slack_channel}"
            }
    except Exception as e:
        return {
            "statusCode": 500, 
            "body": f"Unable to send a message to {slack_channel}, an error occured: {e}"
        }
