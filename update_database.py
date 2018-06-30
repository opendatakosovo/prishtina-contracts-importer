# -*- coding: utf-8 -*-
from pymongo import MongoClient

# Connect to default local instance of mongo
client = MongoClient()

# Get database and collection
db = client.kosovoprocurements
collection = db.procurements


def modify_company_slug_and_residence():
    if collection.find({'$or': [{"kompania.selia.emri": "", "kompania.selia.slug": ""}, {"kompania.selia.emri": " ", "kompania.selia.slug": ""}]}).count() > 0:
        collection.update({
            '$or': [{"kompania.selia.emri": "", "kompania.selia.slug": ""},
                    {"kompania.selia.emri": " ", "kompania.selia.slug": ""}]
            },{
                "$set": {
                    "kompania.selia.emri": "n/a",
                    "kompania.selia.slug": "n/a"}},
            multi=True)
        print 'Documents with empty values for company name and residence are modified with n/a value.'
    else:
        print "Company slug and residence : There's no document that needs to be updated."


def delete_documents_with_empty_values():
    if collection.find({"kompania.emri": "","kontrata.qmimi":0,"kontrata.vlera":0,"kontrata.qmimiAneks":0,"aktiviteti":"","procedura":"n/a","vlera":"n/a","tipi":"n/a"}).count() > 0:
        collection.remove({
            "kompania.emri": "",
            "kontrata.qmimi": 0,
            "kontrata.vlera": 0,
            "kontrata.qmimiAneks": 0,
            "aktiviteti": "",
            "procedura": "n/a",
            "vlera": "n/a",
            "tipi": "n/a"})
        print "Documents with empty values are deleted from the database."
    else:
        print "Documents with empty values: There's no document that needs to be deleted."


modify_company_slug_and_residence()
delete_documents_with_empty_values()