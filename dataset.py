import datetime
import random

today = datetime.datetime.now()

date_time = today.strftime("%d-%m-%Y %H:%M:%S")
site_name = random.randint(1,100)
posa_continent = random.randint(1,10)
user_location_country = random.randint(1,300)
user_location_region = random.randint(1,1000)
user_location_city = random.randint(1,10000)
orig_destination_distance = round(random.uniform(100,10000), 4)
user_id = random.randint(1,10000)
is_mobile = random.choice([0, 1])
is_package = random.choice([0, 1])
channel = random.randint(1,10)
srch_ci = today.strftime("%d-%m-%Y")
srch_co = today.strftime("%d-%m-%Y")
srch_adults_cnt = random.randint(1,10)
srch_children_cnt = random.randint(1,10)
srch_rm_cnt = random.randint(1,10)
srch_destination_id = random.randint(1,10000)
srch_destination_type_id = random.randint(1,10)
is_booking = random.choice([0, 1])
cnt = random.randint(1,10)
hotel_continent = random.randint(1,10)
hotel_country = random.randint(1,300)
hotel_market = random.randint(1,1000)
hotel_cluster = random.randint(1,100)

dataset = [date_time, site_name, posa_continent, user_location_country, user_location_region,
           user_location_city, orig_destination_distance, user_id, is_mobile, is_package,
           channel, srch_ci, srch_co, srch_adults_cnt, srch_children_cnt, srch_rm_cnt,
           srch_destination_id, srch_destination_type_id, is_booking, cnt, hotel_continent,
           hotel_country, hotel_market, hotel_cluster]

def streaming_dataset():
    result = ','.join(str(i) for i in dataset)
    return result

print(streaming_dataset())
