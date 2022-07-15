from bs4 import BeautifulSoup as bs
import requests

import pandas as pd
from utils import *

def request_page(url, parser='lxml'):
    response = requests.get(url)
    soup = bs(response.content, parser)
    return soup

def get_city_information(url, parser='lxml'):
    soup = request_page(url)
    city_options = soup.select('#kode_kabupaten > option')

    city_info = [] 

    for i, city in enumerate(city_options):
        if i == 0:
            continue

        city_id = city.get('value')
        city_name, province = city.get_text().split(', ')
        data = {
            'city_id': city_id,
            'city_name': city_name,
            'province_name': province.replace('Prov. ', '')
        }
        city_info.append(data)
    
    return city_info

def get_kecamatan_information_single_city(city_id):
    data = {'kode_kabupaten': '{}'.format(city_id),}
    response = requests.post('https://sekolah.data.kemdikbud.go.id/index.php/Chome/kecamatan', data=data)
    soup = bs(response.content, 'lxml')
    kecamatan_list = soup.find_all('option')

    kecamatan_info = []
    for i, kecamatan in enumerate(kecamatan_list):
        if i in range(2):
            continue

        kecamatan_id = kecamatan.get('value')
        kecamatan_name, city_name = kecamatan.get_text().split(', ')

        data = {
            'kecamatan_id': kecamatan_id,
            'kecamatan_name': kecamatan_name,
            'city_name': city_name
        }
        kecamatan_info.append(data)
    
    return kecamatan_info

def kecamatan_info_all_city(city_id_list):
    kecamatan_list = []
    for city_id in city_id_list:
        kecamatan_info = get_kecamatan_information_single_city(city_id)
        for kecamatan in kecamatan_info:
            kecamatan_list.append(kecamatan)
    return kecamatan_list

def main(url):
    city_info_list = get_city_information(url)
    city_info_table = list_to_dataframe(city_info_list)
    city_id_list = city_info_table['city_id'].tolist()
    try:
        table_name = 'city_information'
        import_dataframe_to_mysql(path='mysql_config.ini', 
                                section='mysql_server', 
                                dataframe=city_info_table, 
                                table_name=table_name, 
                                if_exist='fail')
    except:
        print('{} table already existed'.format(table_name))

    kecamatan_info_list = kecamatan_info_all_city(city_id_list)
    kecamatan_info_table = list_to_dataframe(kecamatan_info_list)
    try:
        table_name = 'kecamatan_information'
        import_dataframe_to_mysql(path='mysql_config.ini', 
                                section='mysql_server', 
                                dataframe=kecamatan_info_table, 
                                table_name=table_name, 
                                if_exist='fail')
    except:
        print('{} table already existed'.format(table_name))

main(url)