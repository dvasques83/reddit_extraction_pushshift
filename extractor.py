from pyrsistent import s
from utils import *
import argparse

currentSubmissions = []
currentComments = []
month_in_s = 2628000
default_time_window = 2628000
finished = False
recursion_stack_level = 0


def main():
    try:
        parser = argparse.ArgumentParser(description='Subreddit data extractor v1.0.0', usage='python extractor.py -name askhistorians -start 2020-03-11 -end 2020-12-31 -subs True -comments True', epilog='Script written by Rodrigo Quisen' )
        parser.add_argument('-name', metavar='SUBREDDIT_NAME', nargs=1, type=str, required=True, help='Subreddit name. (ex: -name askhistorians)')
        parser.add_argument('-start', metavar='YYYY-MM-DD', nargs=1, type=str,  required=True, help='Initial extraction date as string. (ex: -start 2022-03-15)')
        parser.add_argument('-end', metavar='YYYY-MM-DD', nargs=1, type=str, required=True, help='Final extraction date as string. (ex: -end 2022-03-31)')
        parser.add_argument('-subs', metavar=True, nargs=1, type=bool, default=True, help='Get submissions. (ex: -subs True)')
        parser.add_argument('-comments', metavar=True, nargs=1, type=bool, default=True, help='Get comments. (ex: -comments True)')

        args = parser.parse_args()     
        print("Argument values:")
        print(args)       
        processRedditDataSplitMonths(args.name, args.start, args.end, args.subs, args.comments)

    except Exception as e:
        print(exception_detail(
            e, f'Error parsing arguments: " {e}'))

def processRedditDataSplitMonths(subreddit, date_start, date_end, get_submissions=True, get_comments=True):
    date_start = datetime.strptime(date_start, "%Y-%m-%d")
    date_end   = datetime.strptime(date_end, "%Y-%m-%d")

    diff_months = get_diff_in_months(date_start, date_end)
    list_months = get_date_list_month_diff(date_start, diff_months)

    for month in range(len(list_months)):
        first_day_of_month = list_months[month].replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_day_of_month = get_last_day_of_month(list_months[month]).replace(hour=23, minute=59, second=59)
        processRedditData(subreddit, first_day_of_month.strftime('%Y-%m-%d'), last_day_of_month.strftime('%Y-%m-%d'), get_submissions, get_comments)

def get_submissions(sub_reddit, current_window_start, current_window_end):
    global currentSubmissions
    responseJson = get_pushshift_data(data_type="submission", after=int(current_window_start), before=int(current_window_end),
                                      size=100, sort_type="created_utc", sort="asc", subreddit=sub_reddit, metadata=True)

    if responseJson and len(responseJson['data']) > 0:
        received_amount_submissions = len(responseJson['data'])
        print(f' Response.total_submissions: {received_amount_submissions}')
        for submission in responseJson['data']:
            if(is_item_in_list(submission, currentSubmissions)):
                continue
            currentSubmissions.append(submission)


def get_comments(sub_reddit, current_window_start, current_window_end):
    global currentComments
    responseJson = get_pushshift_data(data_type="comment", after=int(current_window_start), before=int(current_window_end),
                                      size=100, sort_type="created_utc", sort="asc", subreddit=sub_reddit, metadata=True)

    if responseJson and len(responseJson['data']) > 0:
        received_amount_comments = len(responseJson['data'])
        print(f' Response.total_comments: {received_amount_comments}')
        for comment in responseJson['data']:
            if(is_item_in_list(comment, currentComments)):
                continue
            currentComments.append(comment)


def processRedditData(subreddit, date_start, date_end, get_submissions=True, get_comments=True):
    global currentSubmissions
    global currentComments
    global finished
    global default_time_window

    print('Starting requests to r/', subreddit)

    currentSubmissions = []
    currentComments = []

    date_start = convert_date_to_epoch(date_start)
    date_end = convert_date_to_epoch(date_end)

    if get_submissions:
        default_time_window = 2628000
        finished = False
        getSubredditData(subreddit, date_start, date_end, 'submission')
        months_dict = split_json_array_by_month(
            order_json_array_by_date(currentSubmissions))
        for key, value in months_dict.items():
            with open(f'{subreddit}_{key}_submissions.json', "w") as outfile:
                json.dump(value, outfile, indent=4)
        print("Finished processing submissions")

    if get_comments:
        default_time_window = 2628000
        finished = False
        getSubredditData(subreddit, date_start, date_end, 'comment')
        months_dict = split_json_array_by_month(
            order_json_array_by_date(currentComments))
        for key, value in months_dict.items():
            with open(f'{subreddit}_{key}_comments.json', "w") as outfile:
                json.dump(value, outfile, indent=4)
        print("Finished processing comments")


def getSubredditData(sub_reddit, start, hard_end, type, window_interval=default_time_window):
    global req_count
    global finished
    global currentSubmissions
    global currentComments
    global default_time_window
    global recursion_stack_level

    try:
        window_interval = default_time_window
        current_window_end = start + window_interval

        while start < hard_end and not finished:
            print(
                f'window[start]: {int(start)} - {convert_epoch_to_date(start)}')
            print(
                f'window[end]: {int(current_window_end)} - {convert_epoch_to_date(current_window_end)}')

            window_instances = countInstancesUsingMetadata(
                sub_reddit, type, start, start + window_interval)

            if window_instances < 100:
                current_window_end = start + default_time_window
                window_interval = default_time_window
                window_instances = countInstancesUsingMetadata(
                    sub_reddit, type, start, start + window_interval)
            else:
                current_window_end = start + window_interval

            if window_instances > 1000:
                default_time_window = default_time_window / 2

            time.sleep(1)
            if window_instances > 0:
                if window_instances > 100:
                    time_diff_part = window_interval / 2
                    while countInstancesUsingMetadata(sub_reddit, type, start, start + time_diff_part) > 100:
                        time.sleep(1)
                        time_diff_part = time_diff_part / 2
                    
                    window_interval = time_diff_part

                    if type == 'submission':
                        get_submissions(sub_reddit, start, start+window_interval if start+window_interval < hard_end else hard_end)
                    else:
                        get_comments(sub_reddit, start, start+window_interval if start+window_interval < hard_end else hard_end)

                    time.sleep(1)
                else:
                    if type == 'submission':
                        get_submissions(sub_reddit, start, start+window_interval if start+window_interval < hard_end else hard_end)
                    else:
                        get_comments(sub_reddit, start, start+window_interval if start+window_interval < hard_end else hard_end)

                    if current_window_end >= hard_end:
                        finished = True
                        break

                    time.sleep(1)

            start += window_interval
            current_window_end = start + window_interval

            if current_window_end > hard_end:
                current_window_end = hard_end

    except Exception as e:
        print(exception_detail(
            e, f'Error extracting data from r/" {sub_reddit}'))


if __name__ == '__main__':
    main()
