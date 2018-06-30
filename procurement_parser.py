# -*- coding: utf-8 -*-

import csv
import os
from datetime import datetime, date
from slugify import slugify
from pymongo import MongoClient
from utils import Utils

# Connect to default local instance of mongo
client = MongoClient()

# Get database and collection
db = client.kosovoprocurements
collection = db.procurements
utils = Utils()
collection.remove({})

def parse():

    print "Importing procurements data."

    for filename in os.listdir('data/procurements'):
        city = filename
        for filename in os.listdir('data/procurements/'+filename):
            if(filename.endswith(".csv")):
                with open('data/procurements/'+city+"/" + filename, 'rb') as csvfile:
                    reader = csv.reader(csvfile, delimiter=',')
                    line_number = 0
                    for row in reader:
                        year = int(filename.replace('.csv', ''))
                        budget_type = convert_buget_type(row[0])
                        nr = convert_nr(row[1])
                        type_of_procurement = convert_procurement_type(row[2])
                        value_of_procurement = convert_procurement_value(row[3])
                        procurement_procedure = convert_procurement_procedure(row[4])
                        classification = int(convert_classification(row[5]))
                        activity_title_of_procurement = remove_quotes(row[6])
                        signed_date = convert_date(row[7], year, city)
                        #TODO: Convert this to Date
                        contract_value = convert_price(row[8])
                        contract_price = convert_price(row[9])
                        aneks_contract_price = convert_price(row[10])
                        company = remove_quotes(row[11])
                        komuna_slug = slugify(city)
                        company_address = remove_quotes(row[12])
                        company_address_fixed = utils.fix_city_name(company_address) 
                        company_address_slug = slugify(company_address)
                        company_address_slug_fixed = utils.fix_city_slug(company_address_slug)
                        tipi_operatorit = convert_company_type(row[13])
                        afati_kohor = convert_due_time(row[14])
                        kriteret_per_dhenje_te_kontrates = convert_criteria_type(row[15])

                        report = {
                            "komuna": {
                                "emri": city,
                                "slug": komuna_slug
                            },
                            "viti": int(year),
                            "tipiBugjetit": budget_type,
                            "numri": nr,
                            "tipi": type_of_procurement,
                            "vlera": value_of_procurement,
                            "procedura": procurement_procedure,
                            "klasifikimiFPP": classification,
                            "aktiviteti": activity_title_of_procurement,
                            "dataNenshkrimit": signed_date,
                            "kontrata": {
                                "vlera": contract_value,
                                "qmimi": contract_price,
                                "qmimiAneks": aneks_contract_price,
                                "afatiKohor": afati_kohor,
                                "kriteret": kriteret_per_dhenje_te_kontrates
                            },
                            "kompania": {
                                "emri": company,
                                "slug": slugify(company),
                                "selia": {
                                    "emri": company_address_fixed,
                                    "slug": company_address_slug_fixed
                                },
                                "tipi": tipi_operatorit
                            }
                        }

                        coordinates = utils.get_city_coordinates(company_address_slug_fixed)
                        if company_address_slug_fixed != "" and coordinates != None:
                            report["kompania"]["selia"]["kordinatat"] = {
                                'gjeresi': coordinates['lat'],
                                'gjatesi': coordinates['lon']
                            }

                        line_number = line_number + 1
                        collection.insert(report)

def convert_nr(number):
    if(number is None):
        return ""
    else:

        newNumber = [int(s) for s in number.split() if s.isdigit()]
        if len(newNumber) > 0:
            return int(newNumber[0])
        else:
            return ""


def convert_classification(number):
    if number != "":
        if number.startswith('0'):
            return str(int(number))
        else:
            return number
    else:
        return 0

def convert_date(date_str, year, qyteti):
    if date_str.startswith('nd') or date_str.startswith('a') or date_str == "":
        today = date.today()
        today = today.strftime(str("1.1."+str(year)))
        return datetime.strptime(today, '%d.%m.%Y')
    elif date_str.find(",") != -1:
        date_str = date_str.replace(',', '.')
        date_str = date_str[0: 10]
        return datetime.strptime(date_str, '%d.%m.%Y')
    elif date_str.find('/') != -1:
        date_str = date_str.replace('/', '.')
        date_str = date_str[0: 10]
        return datetime.strptime(date_str, '%d.%m.%Y')
    else:
        date_str = date_str[0: 10]

        if len(date_str[6:]) ==2:
            day = date_str[0:2]
            month =  date_str[3:5]
            datet = ""
        
            datet = date_str[6:]
            datet = str(20)+datet            
            final_date = str(day) +"."+str(month)+"." + datet
            return datetime.strptime(final_date, '%d.%m.%Y')
        return datetime.strptime(date_str, '%d.%m.%Y')


def convert_price(num):
    if isinstance(num, str):
        if num.startswith('A') or num.startswith('a') or 'p' in num or num == "":
            return 0
        elif ',' in num:
            return float(num.replace(',', ''))
        elif num == 'N/A':
            return 0
        else:
            num = num.decode('unicode_escape').encode('ascii', 'ignore')
            return float(num)
    else:
        return float(num)


def remove_quotes(name):
    '''
    if name[0] == '"':
        name = name[1:]

    if name[len(name)-1] == '"':
        name = name[0: (len(name)-1)]
    '''
    return name.replace('"', '')


def convert_buget_type(number):
    if (number.find('+') != -1) or (number.find(',') != -1):
        budget_array = []
        if number[:1] == '1':
            budget_array.append("Të hyra vetanake")
        if number[2:3] == '2':
            budget_array.append("Buxheti i Kosovës")
        if number[4:5] == '3':
            budget_array.append("Donacione")
        return budget_array

    value = number[:1]
    if value != "":
        num = int(value)
        if num == 1:
            return "Të hyra vetanake"
        elif num == 2:
            return "Buxheti i Kosovës"
        elif num == 3:
            return "Donacione"
    else:
        return "n/a"


def convert_procurement_type(num):
    if num != "":
        number = int(num)
        if number == 1:
            return "Furnizim"
        elif number == 2:
            return "Shërbime"
        elif number == 3:
            return "Shërbime keshillimi"
        elif number == 4:
            return "Konkurs projektimi"
        elif number == 5:
            return "Punë"
        elif number == 6:
            return "Punë me koncesion"
        elif number == 7:
            return "Prone e palujtshme"
    else:
        return "n/a"


def convert_procurement_value(num):
    if num != "":
        number = int(num)
        if number == 1:
            return "Vlerë e madhe"
        elif number == 2:
            return "Vlerë e mesme"
        elif number == 3:
            return "Vlerë e vogël"
        elif number == 4:
            return "Vlerë minimale"
    else:
        return "n/a"


def convert_procurement_procedure(num):
    if num != "":
        number = int(num)
        if number == 1:
            return "Procedura e hapur"
        elif number == 2:
            return "Procedura e kufizuar"
        elif number == 3:
            return "Konkurs projektimi"
        elif number == 4:
            return "Procedura e negociuar pas publikimit të njoftimit të kontratës"
        elif number == 5:
            return "Procedura e negociuar pa publikimit të njoftimit të kontratës"
        elif number == 6:
            return "Procedura e kuotimit të Çmimeve"
        elif number == 7:
            return "Procedura e vlerës minimale"
    else:
        return "n/a"


def convert_company_type(num):
    if num != "":
        if num == "Ferizaj":
            return "OE Vendor"
        elif num == 'I':
            return "OE Vendor"
        elif num != "Ferizaj":
            number = int(num)
            if number == 1:
                return "OE Vendor"
            elif number == 2:
                return "OE Jo vendor"
    else:
        return "n/a"


def convert_due_time(num):
    if num != "":
        number = int(num)
        if number == 1:
            return "Afati kohor normal"
        elif number == 2:
            return "Afati kohor i shkurtuar"
    else:
        return "n/a"


def convert_criteria_type(num):
    if num != "":
        number = int(num)
        if number == 1:
            return "Çmimi më i ulët"
        elif number == 2:
            return "Tenderi ekonomikisht më i favorshëm"
    else:
        return "n/a"

parse()