import requests, json, sys, traceback, glob, time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from calendar import monthrange

# Function to return the month and year MM-yyyy from a epoch in milliseconds
def get_month_year_from_epoch(epoch):
    try:
        date = datetime.utcfromtimestamp(epoch).strftime('%m-%Y')
        return date
    except Exception as e:
        print(exception_detail(e, "Erro ao converter epoch para data"))
        return ''

# Function to print detailed exceptions
def exception_detail(e, custom_msg=None):
    _, _, exc_tb = sys.exc_info()
    detailed_exp = traceback.format_exc()
    return(f"{'Erro.' if custom_msg is None else custom_msg} (Linha {exc_tb.tb_lineno}: {e}).\nErro completo: {detailed_exp}")

# Function to load a json file into a dictionary if it exists
def load_json_file(file_name):
    try:
        with open(file_name, 'r', encoding='utf-8') as file:
            return json.load(file)
    except FileNotFoundError:
        return []

# Function to make request to the Pushshift API
# Param: data_type - 'comment' or 'submission'
# Other available params: title, after, before, size, author, sort_type, subreddit
def get_pushshift_data(data_type, **kwargs):
    try:
        base_url = f"https://api.pushshift.io/reddit/search/{data_type}/"
        payload = kwargs
        response = requests.get(base_url, params=payload)

        responseJsonDict = {}
        if response is not None and response.status_code == 200 and len(response.json()):
            responseJsonDict = response.json()
        return responseJsonDict
    except Exception as e:
        print(exception_detail(e, "Erro ao executar request get_pushshift_data"))
        return {}

# Function to convert date yyyy-mm-dd to epoch in milliseconds
def convert_date_to_epoch(date):
    try:
        date_split = date.split('-')
        year = int(date_split[0])
        month = int(date_split[1])
        day = int(date_split[2])
        epoch = (datetime(year, month, day) - datetime(1970, 1, 1)).total_seconds()
        return int(epoch)
    except Exception as e:
        print(exception_detail(e, "Erro ao converter data"))

# Function to convert epoch in milliseconds to date yyyy-mm-dd-hh-mm-ss GMT
def convert_epoch_to_date(epoch):
    try:
        date = datetime.utcfromtimestamp(epoch).strftime('%Y-%m-%d-%H:%M:%S')
        return date
    except Exception as e:
        print(exception_detail(e, "Erro ao converter epoch para data"))
        return ''

# Function to return the epoch in milliseconds of the last hour of the day
def get_last_hour_of_day(epoch):
    try:
        date = datetime.utcfromtimestamp(epoch).strftime('%Y-%m-%d')
        date_split = date.split('-')
        year = int(date_split[0])
        month = int(date_split[1])
        day = int(date_split[2])
        hour = int(datetime.utcfromtimestamp(epoch).strftime('%H'))
        return (datetime(year, month, day, hour) - datetime(1970, 1, 1)).total_seconds()
    except Exception as e:
        print(exception_detail(e, "Erro ao converter epoch para data"))
        return 0

# Function to verify if epoch in millis is last hour of the month
def is_last_hour_of_month(epoch):
    try:
        date = datetime.utcfromtimestamp(epoch).strftime('%Y-%m-%d')
        date_split = date.split('-')
        year = int(date_split[1])
        month = int(date_split[0])
        day = int(date_split[2])
        hour = int(datetime.utcfromtimestamp(epoch).strftime('%H'))
        if day == 31 and hour == 23:
            return True
        return False
    except Exception as e:
        print(exception_detail(e, "Erro ao converter epoch para data"))
        return False

# Function to receive 2 epochs and verify if month changed
def is_month_changed(start, end):
    start_month = datetime.utcfromtimestamp(start).strftime('%m')
    end_month = datetime.utcfromtimestamp(end).strftime('%m')
    return start_month != end_month

# Function to calculate difference in hours/days/months between two epochs in milliseconds and return an integer 
def requests_needed(type, start, end):   
    start_date = convert_date_to_epoch(start)
    end_date = convert_date_to_epoch(end)     
    
    # month
    ref_unity = 2678399

    if type == 'day':
        ref_unity = 86400
    elif type == 'hour':
        ref_unity = 3600

    diff = end_date - start_date
    return int(diff // ref_unity)    

# Function to verify if a item is already in the list
def is_item_in_list(item, list):
    for it in list:
        if item['id'] == it['id']:
            return True
    return False

# Function to verify amount of submissions and comments whithin a period of time. Type can be 'submission' or 'comment'
def countInstancesUsingMetadata(subreddit, type, after, before):
    url = f'https://api.pushshift.io/reddit/search/{type}/?subreddit={subreddit}&metadata=true&size=0&after={int(after)}&before={int(before)}'
    print(' Request URL: ' + url)
    response = requests.get(url)
    print(f' Response Code: {response.status_code}')
    data = response.json()
    total_results = data['metadata']['total_results']
    print(f" Metadata.total_results: {total_results}")
    return int(total_results)

def order_json_array_by_date(json_array):
    json_array.sort(key=lambda x: x['created_utc'])
    return json_array

def split_json_array_by_month(json_array):
    year_month_dict = {}
    
    for submission in json_array:
        obj_date = convert_epoch_to_date(submission['created_utc'])

        # parse date from string. example: '2018-04-01-11:51:07'
        obj_date = datetime.strptime(obj_date, "%Y-%m-%d-%H:%M:%S")

        # obj_date 
        year = obj_date.year
        month = obj_date.month

        year_month = f"{month}-{year}"
        
        if year_month not in year_month_dict:
            year_month_dict[year_month] = []

        year_month_dict[year_month].append(submission)
    
    return year_month_dict

def get_diff_in_months(date1, date2):
    try:
        return (date2.year - date1.year) * 12 + date2.month - date1.month
    except Exception as e:
        print(exception_detail(e, "Erro ao calcular diferenca de meses"))
        return 0

def get_date_list_month_diff(date, months):
    try:
        date_list = []
        date_list.append(date)
        for i in range(months):
            date_list.append(date + relativedelta(months=+1))
            date = date + relativedelta(months=+1)
        return date_list
    except Exception as e:
        print(exception_detail(e, "Erro ao calcular diferenca de meses"))
        return []

def get_last_day_of_month(date_value):
    return date_value.replace(day = monthrange(date_value.year, date_value.month)[1])