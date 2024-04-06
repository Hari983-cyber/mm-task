import requests
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import datetime
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import schedule
import time

# Define your Slack token and channel
SLACK_TOKEN = "xoxe.xoxp-1-Mi0yLTY5MDI2NDIxNzAwODYtNjg5NDcxNDMwNzIwNy02OTExNzg3ODQzMzMxLTY5MjQ5NjQ0MzQ1MTYtYmFlMDM2MzkzYjQ5OTFkY2ZiN2JlZDgxY2IzZDFhNTU5YmQ3M2E3YTk4OTJkZWJkYzM3YjQ1MWE2YTdiMjYyOQ"
SLACK_CHANNEL = "https://mm-bmv6815.slack.com/archives/C06STN2P960"
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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"
channel_id = "#project"
token = "xoxb-6902642170086-6915912517974-hZGSP45f4YN5NeY5JkPdpBob"
client = WebClient(token=token)

# Store name and access token for Shopify API authentication
SHOPIFY_STORE_URL = "betterbody-co-test.myshopify.com"
SHOPIFY_ACCESS_TOKEN = "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"

# Construct the Shopify API URL
shopify_url = f'https://{SHOPIFY_STORE_URL}/admin/api/2023-01/'

# Headers for Shopify API requests
headers = {
    'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
    'Content-Type': 'application/json'
}


# Authenticate with Shopify API
def fetch_data_from_shopify(endpoint):
    response = requests.get(shopify_url + endpoint, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data from Shopify API. Status code: {response.status_code}")
        return None

# Define a function to send a Slack notification
def send_slack_notification(message):
    client = WebClient(token=token)
    response = client.chat_postMessage(channel=channel_id, text=message)

# Calculating the percentage change
def kpi_percentage_change(dataframe, column_name):
    col_difference = str(column_name+"_Difference")
    col_difference_percentage = str(col_difference+"_Percentage")
    dataframe[col_difference] = dataframe[column_name].diff()
    dataframe[col_difference_percentage] = dataframe[col_difference]/dataframe[column_name]*-1
    dataframe.drop([col_difference],axis=1,inplace=True)

    return dataframe[col_difference_percentage]


# Fetch data from Shopify API for orders
def fetch_and_update():
    orders_data = fetch_data_from_shopify('orders.json')
    if orders_data:
        # Extract specific data fields from orders_data and format for Google Sheets
        data_for_sheet = []  
        for order in orders_data['orders']:
            revenue = round(float(order['total_price']),2)
            date_only = order['created_at'][0:10]
            format_data = '%Y-%m-%d'
            date = datetime.strptime(date_only, format_data) 
            
            row = [
                date,
                1,
                revenue,  # Revenue
                order['order_number']
                ]
            data_for_sheet.append(row)
                

        df_temp = pd.DataFrame(data_for_sheet, columns=['Date','Session','Revenue',"Orders"])


        ############################ Group By on Date column  ################################
        agg_datewise=df_temp.groupby(['Date']).agg({'Session':'sum','Revenue':'sum','Orders':'count'})

        ############################ Index Resetting and Date Ordering ##################################
        agg_datewise.reset_index(inplace=True)
        agg_datewise.sort_values(by=['Date'],ascending=False,inplace=True)



        ################################# Calculating KPIs #########################################
        #### Calculating AOV
        agg_datewise["AOV"] = agg_datewise['Revenue']/agg_datewise["Orders"]

        #### Conversion Rate
        agg_datewise["Conversion_Rate"] = agg_datewise["Orders"]/agg_datewise["Session"]

        #### Percentage change in session count
        agg_datewise['Session_Difference_Percentage'] = kpi_percentage_change(agg_datewise, 'Session')

        #### Percentage change in session count
        agg_datewise['Session_Difference_Percentage'] = kpi_percentage_change(agg_datewise, 'Revenue')

        #### Percentage change in conversation rate
        agg_datewise['Conversion_Rate_Difference_Percentage'] = kpi_percentage_change(agg_datewise, 'Conversion_Rate')


        #### Percentage change in AOV
        agg_datewise['AOV_Difference_Percentage'] = kpi_percentage_change(agg_datewise, 'AOV')


        ################################## Reordering columns in Datafarme ##################################

        agg_datewise = agg_datewise[["Date",
                                    "Session",
                                    "Session_Difference_Percentage",
                                    "Revenue",
                                    "Revenue_Difference_Percentage",
                                    "Conversion_Rate",                            
                                    "Conversion_Rate_Difference_Percentage",
                                    "AOV",
                                    "AOV_Difference_Percentage"]]

        #     #Define the OAuth2 scopes required for accessing Google Sheets
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

        # #     # Load the credentials
        credentials_dict = {
                "type": "service_account",
                "project_id": "tricks-275313",
                "private_key_id": "8b6f081ebbc5a4853022e9e7ec54b927ea41efc2",
                "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCQQFpm3vlCjiYe\nydu7n/29qJjKEy/7mKCWUFkKA0Up0cZVoLISRSO6jGsnrTZH2ZnU0q9C11AbE3PA\nL4Q++SjKTznYhbwE/c9/pNNOwMkUyypO7mRm9rFng8Qdy6suopDW4E7SR1TkiwtN\nmsYaCWs5wq+Y96zBojXtGmKClGk12nMubeSE3tlgeJtTQRv4QXiM8doOwnJnyMKS\nK59ILjjbCz0nwll9MV1hKZaeCqupcWdvIhwPZ3P/eItaGs4UJkhT0hpgylPpdmfv\nYfy21Zy2tSdqh9HzrtP+fCtEv0MmU24ALD3VWh39uuXyLhL2cg5Pk/ZSMeiSwTQs\nU+rmzUFNAgMBAAECggEAC1MSBctxdBkmwvoD5xnrWK1YuVyCQtU7njFWnCOflWXj\nlStVb3vAD1AgxW+iuEdL/VpA6hVvM4D2cFsKncr/uco4d1ggZ5/pX1L4R9F4t8ov\nWxz6At9RFBhTGCe6G6IljsjzRyzUbFDCJWiGLtsYcONo17Bx/o2WgDO3EhtGmzSZ\n66+mPUknmwNXEiyvP4bcH+B/oBjIHm+zwq0oKRwakDlao4JsO09EhtQ1RZwHlfDB\n72TrZXc7onL3yI5+6LkJZB8EhXdaSzX6K0Jr8jPOQN2Q1NXyQxtHPrHLWWuSi5E2\n4dDQ+B+y9kPwhfdBk9lXzZbaSwE8VKcBMwGxV+5IYQKBgQDJUNHfPN/TYfd6NpRX\n7Q9OsL+dMSecIikQsbj5OBJYixFiDzfbLduPC/8w1UXrDd4H4oOQDZfao2Zpjccb\nLh+n5rPAzVz+O6oTDyTjm/0lisrJCEyNc0TuJiKXVKbqe+HFti7H5yK9mbZ+5xGw\nTKYf01t5NKl/QQZgQ6i86VoHtQKBgQC3b16GTfKKMLZEBA6qqDlLzrsYKkKbZgZI\nuKV1b+DH9wO27eVpkxRVd89JlMHoEQj0kGh4rqK2lTYxvnsIkb16Z20ImNtcBQ78\nsdXxBPb+YLd7eLuWDQAMD1RZ6RVQGcMv02Jg76BCloaHF3YrejuajGiozyOi/kVC\nXnLvz/2iOQKBgC5cAjiaWCDhipAGSZsF6GSCp12XmDuIUlSZ2LLriq4BOHuQbZh3\nsvv6E0Y3luZORzchnpJpzvJ2dnsQPy2vLXMI2ZQotqWFL+E7t48CuQUQNSqeUqKw\nTMta1NRslhOhe5iAH73BKGrpHvGSXKxwukDR91quGepIGyPH8O+v/R+RAoGAHS6Q\ngRlBbo6VUHi8xJWCl/bJeOywL/ypH0SQvgAQaH95jc6G4qbNY1NuHKyXtI2ROScI\nF7rgk99lAcDl3dUhqaluhSGcNCVx3u2DtZYunQVR60invxVOXpzJtIwKIfYXQTKF\nwhodCrrbp/4phH0rokiYFdLuPm3pIsfXzSLvnLECgYBjh3a3kUfk1xr7s+B0VwBv\nTzEAhR9FZQoYCCJXT38pQ8fiqfZneHVAZmMqRI1BE6BocxkofqhKHapaSA+4ItxB\negi+L9FuwFNEuuoLAT6SkFnsWB1gXPpatWHyKTntghzFRe/060/cT3mlXEXX1M2M\nootg5+w+zZG3bzXoER4Rrw==\n-----END PRIVATE KEY-----\n",
                "client_email": "mm-test@tricks-275313.iam.gserviceaccount.com",
                "client_id": "104333939086421421015",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/mm-test%40tricks-275313.iam.gserviceaccount.com",
                "universe_domain": "googleapis.com"
        }

        creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)

        client = gspread.authorize(creds)

            # Open the Google Sheet
        sheet = client.open('test').sheet1

        sheet.clear()
        set_with_dataframe(worksheet=sheet, dataframe=agg_datewise, include_index=False,
        include_column_header=True, resize=True)
        try:
            send_slack_notification("Data updated in Google Sheets successfully.")
            print("Data updated in Google Sheets successfully.")
        except SlackApiError as e:
                print(f"Error occured: {e.response['error']}") 
    else:
        send_slack_notification("Failed to fetch orders data from Shopify API")

# Schedule the job to run daily at 5 PM
schedule.every().day.at("00:14").do(fetch_and_update)

# Keep the script running to execute scheduled jobs
while True:
    schedule.run_pending()
    time.sleep(1)