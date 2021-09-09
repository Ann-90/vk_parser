#!/usr/bin/env python
from pandas.core.frame import DataFrame
import requests
import pandas as pd
import time
import datetime as dt

# check runtime
start_time = time.time()

# need to get access_token
access_token = ""
user_fields = 'sex,bdate,city,education'
group_ids = {"Джаганат": 18632130,
             "РАДХИКА": 47571890,
             "Артём Хачатрян": 147400564}
earliest_date = dt.date(2008, 1, 1).year
latest_date = dt.date.today().year

# results
groups_posts_ids = {}
groups_desc_ids = {}
info_comments = {}
disc_comments = {}

# request function


def get_group(id, fields, offset, access_token, post, request):
    url = ""
    if request == "wall.get":
        url = f'https://api.vk.com/method/wall.get?owner_id=-{id}&extended=1&fields={fields}&offset={offset}&count=100&access_token={access_token}&v=5.80'
    elif request == "wall.getComments":
        url = f'https://api.vk.com/method/wall.getComments?owner_id=-{id}&post_id={post}&need_likes=1&extended=1&fields={fields}&offset={offset}&count=100&access_token={access_token}&v=5.80'
    elif request == "groups.getMembers":
        url = f'https://api.vk.com/method/groups.getMembers?group_id={id}&fields={fields}&offset={offset}&count=1000&access_token={access_token}&v=5.80'
    elif request == "board.getTopics":
        url = f'https://api.vk.com/method/board.getTopics?group_id={id}&offset={offset}&count=100&access_token={access_token}&v=5.80'
    elif request == "board.getComments":
        url = f'https://api.vk.com/method/board.getComments?group_id={id}&topic_id={post}&offset={offset}&count=100&need_likes=1&access_token={access_token}&v=5.80'

    json_response = requests.get(url).json()

    if json_response.get('error'):
        print(json_response.get('error'))
        return None

    return json_response['response']


# Get id of a group and id list if posts
def get_id_posts(json):
    if json.get("items"):
        for i in json["items"]:
            post_year = dt.datetime.fromtimestamp(i["date"]).year
            if post_year >= earliest_date and post_year <= latest_date:
                group_id = i["owner_id"]
                post_id = i["id"]
                if groups_posts_ids.get(group_id):
                    if post_id not in groups_posts_ids[group_id]:
                        groups_posts_ids[group_id].append(post_id)
                else:
                    groups_posts_ids[group_id] = [post_id]
            else:
                print("Too old post")
                return False


# Get id of a group and id list if topics
def get_id_topics(json, group_id):
    if json.get("items"):
        for i in json["items"]:
            group_id = group_id
            topic_id = i["id"]
            if groups_desc_ids.get(group_id):
                groups_desc_ids[group_id].append(topic_id)
            else:
                groups_desc_ids[group_id] = [topic_id]

        return groups_desc_ids


# Get messages
def get_comments_post(json, post_id, comment_type):
    result = info_comments if comment_type == "wall.getComments" else disc_comments

    if json.get("items"):
        for i in json["items"]:
            comment = {"from_id": i["from_id"], "date": i["date"],
                       "text": i["text"], "likes": i["likes"]["count"]}
            if result.get(post_id):
                result[post_id].append(comment)
            else:
                result[post_id] = [comment]
        print('Comments:', len(result[post_id]))

    else:
        print("No items in post")


# write down results (after each post or toppic)
def create_dataFrame(request, filename):
    id_list = groups_posts_ids if request == "wall.getComments" else groups_desc_ids
    comment_list = info_comments if request == "wall.getComments" else disc_comments

    dFrame = {"Группа, id": [], "Дата": [], "Отправитель, id": [],
              "Текст": [], "Количество лайков": []}

    for group in id_list:
        for post in id_list[group]:
            if comment_list.get(post) and iter(comment_list[post]):
                for message in comment_list[post]:
                    if dt.datetime.fromtimestamp(message["date"]).year < earliest_date:
                        print("Too old for writting")
                    else:
                        dFrame["Группа, id"].append(-group)
                        dFrame["Дата"].append(dt.datetime.fromtimestamp(
                            message["date"]))
                        dFrame["Отправитель, id"].append(message["from_id"])
                        dFrame["Текст"].append(message["text"])
                        dFrame["Количество лайков"].append(message["likes"])

        group_info = pd.DataFrame(dFrame)
        # CAUTION! CHECK PATH AND MODE!
        group_info.to_csv(f'{filename}.csv', sep=';')
        print('Written')


# main functions
# Getting information about users
def parse_users(groups, filename):
    dFrame = {"Группа, id": [], "Участник, id": [],
              "Пол (ж 1, м 2)": [], "Дата рождения": [], "Город": [], "Образование": [], "Факультет": []}

    for name in groups:
        step = 0
        length = 1000000

        while step < length + 1000:
            group_data = get_group(
                groups[name], user_fields, step, access_token, 0, "groups.getMembers")
            if step == 0:
                length = group_data["count"]

            print("name:", name, "step:", step, "length:", length,
                  "group_data[count]:", group_data["count"])

            if group_data.get("items"):
                for member in group_data["items"]:
                    bdate = member["bdate"] if member.get("bdate") else ""
                    city = member["city"]["title"] if member.get(
                        "city") else ""
                    education = member["university_name"] if member.get(
                        "university_name") else ""
                    faculty = member["faculty_name"] if member.get(
                        "faculty_name") else ""

                    dFrame["Группа, id"].append(groups[name])
                    dFrame["Участник, id"].append(member["id"])
                    dFrame["Пол (ж 1, м 2)"].append(member["sex"])
                    dFrame["Дата рождения"].append(
                        bdate)
                    dFrame["Город"].append(city)
                    dFrame["Образование"].append(education)
                    dFrame["Факультет"].append(faculty)

            time.sleep(0.2)
            step += 1000

    group_info = pd.DataFrame(dFrame)
    group_info.to_csv(f'{filename}.csv', sep=';')
    print("Written")


def parse_texts(groups, request, filename):
    print(request)
    requests = ["wall.get", "wall.getComments"] if request == "wall.getComments" else [
        "board.getTopics", "board.getComments"]
    id_list = groups_posts_ids if request == "wall.getComments" else groups_desc_ids

    try:
        for name in groups:
            step = 0
            length = 1000000

            while step < length + 100:
                print("getting ids")
                group_data = get_group(
                    groups[name], '', step, access_token, 0, requests[0])

                if step == 0:
                    length = group_data["count"]

                id_response = get_id_posts(group_data) if request == "wall.getComments" else get_id_topics(
                    group_data, groups[name])

                if id_response == False:
                    step = length
                else:
                    step += 100

                time.sleep(0.2)

            key = (-groups[name]
                   ) if request == "wall.getComments" else groups[name]

            for post in id_list[key]:
                counter = 0
                len = 1000000

                while counter < len + 100:
                    print('Request comments')
                    comment_data = get_group(
                        groups[name], '', counter, access_token, post, requests[1])

                    if counter == 0:
                        len = comment_data["count"]
                    print("group:", name, "post or topic id:", post)

                    get_comments_post(
                        comment_data, post, requests[1])

                    counter += 100
                    time.sleep(0.2)

                time.sleep(0.2)
                create_dataFrame(request, filename)

    except:
        print('check parse_texts')


# INIT PARSERS
# WARNING! Рекомендуется вызывать функции поочередно

parse_texts(group_ids, "board.getComments", "topics")
parse_texts(group_ids, "wall.getComments", "posts")
parse_users(group_ids, "users")

print("--- %s seconds ---" % (time.time() - start_time))
