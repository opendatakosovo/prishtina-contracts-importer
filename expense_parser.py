import csv

from pymongo import MongoClient


# Connect to default local instance of mongo
client = MongoClient()

# Get database and collection
db = client.gjakova
collection = db.expenses


def parse():

    print "Importing expense data."

    with open('data/expense/2011.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            group = row[0]
            forecast = row[1]
            commune = row[2]
            type1 = row[3]
            type2 = row[4]
            type3 = row[5]
            total = remove_comma(row[8])
            date = row[9]

            doc = {
                "group": group,
                "forecast": forecast,
                "commune": commune,
                "type1": type1,
                "type2": type2,
                "type3": type3,
                "total": float(total),
                "date": date
            }

            collection.insert(doc)


def remove_comma(number):
    return number.replace(",", "")


parse()
