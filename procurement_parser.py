# -*- coding: utf-8 -*-

import csv
import os
import sys
import unicodedata

from datetime import datetime, date
from slugify import slugify
from pymongo import MongoClient
from utils import Utils
import re
import importlib

# Connect to default local instance of mongo
client = MongoClient()

# Get database and collection
db = client.opencontracts
collection = db.contracts
collectionDataset = db.datasets
utils = Utils()


def parse():
    print("------------------------------------")
    print("Importing procurements data.")
    for filename in os.listdir('data/procurements/new'):
        collectionDataset.insert({
            "datasetFilePath": filename,
            "folder": "new",
            "createdAt": datetime.now().isoformat(),
            "updatedAt": datetime.now().isoformat()
        })
        if(filename.endswith(".csv")):
            with open('data/procurements/new/' + filename, 'r') as csvfile:
                reader = csv.reader(csvfile, delimiter=',')
                line_number = 0
                installments = []
                annexes = []
                annex_amount_1 = 37
                annex_date_1 = 38
                annex_amount_2 = 0
                annex_date_2 = 0
                annex_amount_3 = 0
                annex_date_3 = 0
                annex_amount_4 = 0
                annex_date_4 = 0
                installment_amount_1 = 0
                installment_date_1 = 0
                installment_amount_2 = 0
                installment_date_2 = 0
                installment_amount_3 = 0
                installment_date_3 = 0
                installment_amount_4 = 0
                installment_date_4 = 0
                deduction_amount = 0
                last_installment_amount = 0
                last_installment_date = 0
                total_payed_price = 0
                directorate = 0
                name_of_procurement = 0
                company_headquarters = 0
                flag_status = 0
                for row in reader:
                    if filename == '2019.csv' or filename == '2017.csv':
                        annex_amount_1 = 37
                        annex_date_1 = 38
                        installment_date_1 = 39
                        installment_amount_1 = 40
                        installment_date_2 = 41
                        installment_amount_2 = 42
                        deduction_amount = 43
                        last_installment_date = 44
                        last_installment_amount = 45
                        total_payed_price = 46
                        directorate = 47
                        name_of_procurement = 48
                        company_headquarters = 49
                        flag_status = 50
                    if filename == '2018.csv':
                        annex_amount_1 = 37
                        annex_date_1 = 38
                        annex_amount_2 = 39
                        annex_date_2 = 40
                        annex_amount_3 = 41
                        annex_date_3 = 42
                        annex_amount_4 = 43
                        annex_date_4 = 44
                        installment_amount_1 = 46
                        installment_date_1 = 45
                        installment_amount_2 = 48
                        installment_date_2 = 47
                        installment_amount_3 = 50
                        installment_date_3 = 49
                        installment_amount_4 = 52
                        installment_date_4 = 51
                        deduction_amount = 53
                        last_installment_amount = 55
                        last_installment_date = 54
                        total_payed_price = 56
                        directorate = 57
                        name_of_procurement = 58
                        company_headquarters = 59
                        flag_status = 60
                    if line_number > 0:
                        year = int(filename.replace('.csv', ''))
                        planned = convert_planned_number(row[0])
                        budget_type = convert_buget_type(row[1])
                        procurmentNo = convert_nr(row[2])
                        type_of_procurement = convert_procurement_type(row[3])
                        value_of_procurement = convert_procurement_value(
                            row[4])
                        procurement_procedure = convert_procurement_procedure(
                            row[5])
                        classification = convert_classification(row[6])
                        activity_title_of_procurement = remove_quotes(row[7])
                        initiationDate = convert_date(row[8], year)
                        approvalDateOfFunds = convert_date(row[9], year)
                        torDate = convert_date(row[10], year)
                        publicationDate = convert_date(row[11], year)
                        complaintsToAuthority1 = convert_complaints(row[12])
                        complaintsToOshp1 = convert_complaints(row[13])
                        bidOpeningDate = convert_date(row[14], year)
                        noOfCompaniesWhoDownloadedTenderDoc = convert_nr(
                            row[15])
                        noOfCompaniesWhoSubmited = convert_nr(row[16])
                        if row[17].find("-") != -1:
                            startingAndEndingEvaluationDateArray = row[17].split(
                                "-")
                            startingOfEvaluationDate = convert_date(
                                startingAndEndingEvaluationDateArray[0], year)
                            endingOfEvaluationDate = convert_date(
                                startingAndEndingEvaluationDateArray[1], year)
                            startingAndEndingEvaluationDate = None
                        else:
                            startingOfEvaluationDate = None
                            endingOfEvaluationDate = None
                            startingAndEndingEvaluationDate = convert_date_range(
                                row[17], year)
                        noOfRefusedBids = convert_nr(row[18])
                        reapprovalDate = convert_date(row[19], year)
                        publicationDateOfGivenContract = convert_date(
                            row[20], year)
                        cancellationNoticeDate = convert_date(row[21], year)
                        standardDocuments = convert_date(row[22], year)
                        complaintsToAuthority2 = convert_complaints_second(
                            row[23])
                        complaintsToOshp2 = convert_complaints_second(row[24])
                        predictedValue = convert_price(row[25])
                        companyType = convert_company_type(row[26])
                        applicationDeadlineType = convert_due_time(row[27])
                        criteria = convert_criteria_type(row[28])
                        retender = convert_rentender(row[29])
                        status = convert_status(row[30])
                        companyName = remove_quotes(row[31])
                        signed_date = convert_date(row[32], year)
                        implementationDeadline = row[33]
                        closingDate = convert_date_range(row[34], year)
                        totalAmountOfContractsIncludingTaxes = convert_price(
                            row[35])
                        noOfPaymentInstallments = convert_nr(row[36])
                        totalValueOfAnnexContract1 = convert_price(row[annex_amount_1])
                        annexContractSigningDate1 = convert_date(row[annex_date_1], year)
                        annexes.append({
                            "totalValueOfAnnexContract1": totalValueOfAnnexContract1,
                            "annexContractSigningDate1": annexContractSigningDate1
                        })
                        # totalAmountOfAllAnnexContractsIncludingTaxes = convert_price(
                        #     row[39])
                        if filename == '2018.csv':
                            totalValueOfAnnexContract2 = convert_price(row[annex_amount_2])
                            annexContractSigningDate2 = convert_date(row[annex_date_2], year)
                            annexes.append({
                                "totalValueOfAnnexContract1": totalValueOfAnnexContract2,
                                "annexContractSigningDate1": annexContractSigningDate2
                            })
                            totalValueOfAnnexContract3 = convert_price(row[annex_amount_3])
                            annexContractSigningDate3 = convert_date(row[annex_date_3], year)
                            annexes.append({
                                "totalValueOfAnnexContract1": totalValueOfAnnexContract3,
                                "annexContractSigningDate1": annexContractSigningDate3
                            })
                            totalValueOfAnnexContract4 = convert_price(row[annex_amount_4])
                            annexContractSigningDate4 = convert_date(row[annex_date_4], year)
                            annexes.append({
                                "totalValueOfAnnexContract1": totalValueOfAnnexContract4,
                                "annexContractSigningDate1": annexContractSigningDate4
                            })
                            
                        installmentPayDate1 = convert_date(row[installment_date_1], year)
                        installmentAmount1 = convert_price(row[installment_amount_1])
                        installments.append({
                            "installmentPayDate1": installmentPayDate1,
                            "installmentAmount1": installmentAmount1
                        })
                        installmentPayDate2 = convert_date(row[installment_date_2], year)
                        installmentAmount2 = convert_price(row[installment_amount_2])
                        installments.append({
                            "installmentPayDate1": installmentPayDate2,
                            "installmentAmount1": installmentAmount2
                        })
                        if filename == '2018.csv':
                            installmentPayDate3 = convert_date(row[installment_date_3], year)
                            installmentAmount3 = convert_price(row[installment_amount_3])
                            installments.append({
                                "installmentPayDate1": installmentPayDate3,
                                "installmentAmount1": installmentAmount3
                            })
                            installmentPayDate4 = convert_date(row[installment_date_4], year)
                            installmentAmount4 = convert_price(row[installment_amount_4])
                            installments.append({
                                "installmentPayDate1": installmentPayDate4,
                                "installmentAmount1": installmentAmount4
                            })

                        discountAmountFromContract = convert_price(row[deduction_amount])
                        lastInstallmentPayDate = convert_date(row[last_installment_date], year)
                        lastInstallmentAmount = convert_price(row[last_installment_amount])
                        totalPayedPriceForContract = convert_price(row[total_payed_price])
                        directorates = row[directorate].strip()
                        nameOfProcurementOffical = row[name_of_procurement]
                        headquarters = row[company_headquarters]
                        flagStatus = row[flag_status]

                        report = {
                            "activityTitle": activity_title_of_procurement,
                            "activityTitleSlug": slugify(activity_title_of_procurement),
                            "procurementNo": procurmentNo,
                            "procurementType": type_of_procurement,
                            "procurementValue": value_of_procurement,
                            "procurementProcedure": procurement_procedure,
                            "fppClassification": classification,
                            "planned": planned,
                            "budget": budget_type,
                            "initiationDate": initiationDate,
                            "approvalDateOfFunds": approvalDateOfFunds,
                            "torDate": torDate,
                            "complaintsToAuthority1": complaintsToAuthority1,
                            "complaintsToOshp1": complaintsToOshp1,
                            "bidOpeningDate": bidOpeningDate,
                            "noOfCompaniesWhoDownloadedTenderDoc": noOfCompaniesWhoDownloadedTenderDoc,
                            "noOfCompaniesWhoSubmited": noOfCompaniesWhoSubmited,
                            "startingOfEvaluationDate": startingOfEvaluationDate,
                            "endingOfEvaluationDate": endingOfEvaluationDate,
                            "startingAndEndingEvaluationDate": startingAndEndingEvaluationDate,
                            "noOfRefusedBids": noOfRefusedBids,
                            "reapprovalDate": reapprovalDate,
                            "cancellationNoticeDate": cancellationNoticeDate,
                            "complaintsToAuthority2": complaintsToAuthority2,
                            "complaintsToOshp2": complaintsToOshp2,
                            "retender": retender,
                            "status": status,
                            "noOfPaymentInstallments": noOfPaymentInstallments,
                            "directorates": directorates,
                            "directoratesSlug": slugify(directorates),
                            "nameOfProcurementOffical": nameOfProcurementOffical,
                            "installments": installments,
                            "lastInstallmentPayDate":  lastInstallmentPayDate,
                            "lastInstallmentAmount": lastInstallmentAmount,
                            "year": year,
                            "flagStatus": convert_flagSatus(flagStatus),
                            "applicationDeadlineType": applicationDeadlineType,
                            "contract": {
                                "predictedValue": predictedValue,
                                "predictedValueSlug": mySlugify(predictedValue),
                                "totalAmountOfAllAnnexContractsIncludingTaxes": 0,
                                "totalAmountOfContractsIncludingTaxes": totalAmountOfContractsIncludingTaxes,
                                "totalAmountOfContractsIncludingTaxesSlug": mySlugify(totalAmountOfContractsIncludingTaxes),
                                "totalPayedPriceForContract": totalPayedPriceForContract,
                                "annexes": annexes,
                                "criteria": criteria,
                                "implementationDeadline": implementationDeadline,
                                "implementationDeadlineSlug": slugify(implementationDeadline),
                                "publicationDate": publicationDate,
                                "publicationDateOfGivenContract": publicationDateOfGivenContract,
                                "closingDate": closingDate,
                                "discountAmountFromContract": discountAmountFromContract,
                                "file": "",
                                "signingDate": signed_date
                            },
                            "company": {
                                "name": companyName,
                                "slug": slugify(companyName),
                                "headquarters": {
                                    "name": headquarters,
                                    "slug": slugify(headquarters)
                                },
                                "type": companyType,
                                "standardDocuments": standardDocuments
                            },
                            "imported": True,
                            "createdAt": datetime(int(year), 1, 1),
                            "documents": []
                        }
                        collection.insert(report)

                    line_number = line_number + 1
                    annexes = []
                    installments = []


def convert_nr(number):
    if(number is None):
        return ""
    else:
        newNumber = [int(s) for s in number.split() if s.isdigit()]
        if len(newNumber) > 0:
            return int(newNumber[0])
        else:
            return None


def convert_classification(number):
    if number != "":
        if number.startswith('0'):
            return int(number)
        else:
            return int(number)
    else:
        return None


def convert_date(date_str, year):
    date_str = date_str.strip()
    if date_str != "" and date_str != " " and date_str != "n/a" and date_str != "N/A" and date_str.find("€") == -1 and date_str != ".." and date_str != "0" and date_str != "Ankesë":
        if date_str.find(',') != -1:
            splitedDate = date_str.split(',')
            if len(splitedDate[0]) < 2 and len(splitedDate) > 3:
                date_str = "0%s.%s.%s" % (
                    splitedDate[0], splitedDate[1], splitedDate[2])
            elif len(splitedDate[1]) < 2 and len(splitedDate) > 3:
                date_str = "%s.0%s.%s" % (
                    splitedDate[0], splitedDate[1], splitedDate[2])
            elif len(splitedDate[2]) == 2:
                date_str = "%s.%s.%s" % (
                    splitedDate[0], splitedDate[1], splitedDate[2])
        elif date_str.find('.') != -1:
            splitedDate = date_str.split('.')
            if len(splitedDate[0]) < 2 and len(splitedDate) > 3:
                date_str = "0%s.%s.%s" % (
                    splitedDate[0], splitedDate[1], splitedDate[2])
            elif len(splitedDate[1]) < 2 and len(splitedDate) > 3:
                date_str = "%s.0%s.%s" % (
                    splitedDate[0], splitedDate[1], splitedDate[2])
        splitedDate2 = date_str.split('.')
        if len(splitedDate2) == 3 and len(splitedDate2[2]) == 4:
            date_str = "%s.%s.%s" % (
                splitedDate2[0], splitedDate2[1], splitedDate2[2][2:4])
        return datetime.strptime(date_str, '%d.%m.%y')
    elif date_str == "n/a" or date_str == "N/A" or date_str == "0":
        return None
    else:
        return None


def convert_price(num):
    num = num.strip()
    if num != "" and num != "#VALUE!" and num.find('-') == -1 and num != '0' and num != 'n/a' and num != 'N/A' and num != 'Qm.mujor' and num != 'NaN':
        numFormatted = re.sub('([a-zA-Z€])', '', num).strip()
        firstIndexOfFloatingPoint = len(numFormatted) - 3
        secondIndexOfFloatingPoint = len(numFormatted) - 2
        if numFormatted[firstIndexOfFloatingPoint] == '.':
            priceArray = numFormatted.split('.')
            if priceArray[0].find(',') != -1:
                priceArray[1] = priceArray[1] + \
                    "0" if len(priceArray[1]) == 1 else priceArray[1]
                return '{:,.0f}'.format(
                    float(priceArray[0].replace(",", "")))+"."+priceArray[1]
            elif priceArray[0].find('.') != -1:
                priceArray[1] = priceArray[1] + \
                    "0" if len(priceArray[1]) == 1 else priceArray[1]
                return '{:,.0f}'.format(float(priceArray[0].replace(",", "")))+"."+priceArray[1]
            else:
                return '{:,.0f}'.format(float(priceArray[0]))+"."+priceArray[1]
        elif numFormatted[secondIndexOfFloatingPoint] == '.':
            priceArray = numFormatted.split('.')
            if priceArray[0].find(',') != -1:
                priceArray[1] = priceArray[1] + \
                    "0" if len(priceArray[1]) == 1 else priceArray[1]
                return '{:,.0f}'.format(
                    float(priceArray[0].replace(",", "")))+"."+priceArray[1]
            elif priceArray[0].find('.') != -1:
                priceArray[1] = priceArray[1] + \
                    "0" if len(priceArray[1]) == 1 else priceArray[1]
                return '{:,.0f}'.format(float(priceArray[0].replace(",", "")))+"."+priceArray[1]
            else:
                priceArray[1] = priceArray[1] + \
                    "0" if len(priceArray[1]) == 1 else priceArray[1]
                return '{:,.0f}'.format(float(priceArray[0]))+"."+priceArray[1]
        elif numFormatted[firstIndexOfFloatingPoint] == ',':
            numArray = list(numFormatted)
            numArray[firstIndexOfFloatingPoint] = '.'
            for indx, num in enumerate(numArray):
                numArray[indx] = ',' if firstIndexOfFloatingPoint != indx and num == '.' else num
            numFormatted = ''.join(numArray)
            priceArray = numFormatted.split('.')
            if priceArray[0].find('.') != -1:
                priceArray[1] = priceArray[1] + \
                    "0" if len(priceArray[1]) == 1 else priceArray[1]
                return '{:,.0f}'.format(float(priceArray[0].replace(".", "")))+"."+priceArray[1]
            elif priceArray[0].find(',') != -1:
                priceArray[1] = priceArray[1] + \
                    "0" if len(priceArray[1]) == 1 else priceArray[1]
                return '{:,.0f}'.format(float(priceArray[0].replace(",", "")))+"."+priceArray[1]
            else:
                priceArray[1] = priceArray[1] + \
                    "0" if len(priceArray[1]) == 1 else priceArray[1]
                return '{:,.0f}'.format(float(priceArray[0]))+"."+priceArray[1]
        elif numFormatted[secondIndexOfFloatingPoint] == ',':
            numArray = list(numFormatted)
            numArray[secondIndexOfFloatingPoint] = '.'
            numFormatted = ''.join(numArray)
            priceArray = numFormatted.split('.')
            if priceArray[0].find(',') != -1:
                priceArray[1] = priceArray[1] + \
                    "0" if len(priceArray[1]) == 1 else priceArray[1]
                return '{:,.0f}'.format(
                    float(priceArray[0].replace(",", "")))+"."+priceArray[1]
            elif priceArray[0].find('.') != -1:
                priceArray[1] = priceArray[1] + \
                    "0" if len(priceArray[1]) == 1 else priceArray[1]
                return '{:,.0f}'.format(float(priceArray[0].replace(",", "")))+"."+priceArray[1]
            else:
                priceArray[1] = priceArray[1] + \
                    "0" if len(priceArray[1]) == 1 else priceArray[1]
                return '{:,.0f}'.format(float(priceArray[0]))+"."+priceArray[1]
        else:
            return '{:,.0f}'.format(float(numFormatted.replace(",", "")))+".00"
    else:
        return ''


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
            budget_array.append("Donacion")
        return budget_array

    value = number[:1]
    if value != "":
        num = value
        if num == 1:
            return "Të hyra vetanake"
        elif num == 2:
            return "Buxheti i Kosovës"
        elif num == 3:
            return "Donacion"
    else:
        return ""


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
        return ""


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
        return ""


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
        return ""


def convert_company_type(num):
    if num != '' and num != 'n/a':
        number = int(num)
        if number == 1:
            return "vendor"
        elif number == 2:
            return "jo vendor"
    elif num == 'n/a' or num == 'N/A':
        return "n/a"
    else:
        return ''


def convert_due_time(num):
    if num != "":
        number = int(num)
        if number == 1:
            return "Afati kohor normal"
        elif number == 2:
            return "Afati kohor i shkurtuar"
    else:
        return ""


def convert_criteria_type(num):
    if num != "":
        number = int(num)
        if number == 1:
            return "Çmimi më i ulët"
        elif number == 2:
            return "Tenderi ekonomikisht më i favorshëm"
        elif number == 3:
            return "Çmimi më i ulët me poentim"
    else:
        return ""


def convert_planned_number(num):
    if num != "":
        if num.find("1") != -1:
            return "po"
        elif num.find("2") != -1:
            return "jo"
    else:
        return ""


def convert_complaints(num):
    if num != "n/a" and num != "N/A" and num != "" and num != " ":
        complaintType = int(num)
        if complaintType == 1:
            return "negativ"
        elif complaintType == 2:
            return "pozitiv"
    elif num == "n/a" or num == "N/A":
        return "n/a"
    else:
        return ""


def convert_complaints_second(num):
    if num != "" and num != "n/a" and num != "N/A" and num != " ":
        complaintType = int(num)
        if complaintType == 0:
            return "nuk ka"
        elif complaintType == 1:
            return "negativ"
        elif complaintType == 2:
            return "pozitiv"
    elif num == "n/a" or num == "N/A":
        return "n/a"
    else:
        return ""


def convert_date_range(date_str, year):
    if date_str != "" and date_str != "n/a" and date_str.strip().lower() != "vazhdon" and date_str.strip().lower() != "vazhon" and date_str.strip().lower() != "vazdhon" and date_str.strip().lower() != "ne ankese"and date_str.strip().lower() != "e nderprere":
        if date_str.find('muaj') != -1 or date_str.find('dite') != -1 or date_str.find('ditë') != -1:
            return date_str
        else:
            if date_str.startswith('nd') or date_str.startswith('a') or date_str == "":
                today = date.today()
                today = today.strftime(str("1.1."+str(year)))
                return datetime.strptime(today, '%d.%m.%y')
            elif date_str.find(",") != -1:
                date_str = date_str.replace(',', '.')
                date_str = date_str[0: 10]
                return datetime.strptime(date_str, '%d.%m.%y'),
            elif date_str.find('/') != -1:
                date_str = date_str.replace('/', '.')
                date_str = date_str[0: 10]
                return datetime.strptime(date_str, '%d.%m.%y')
            else:
                ate_str = date_str[0: 10]
                if len(date_str[6:]) == 2:
                    day = date_str[0:2]
                    month = date_str[3:5]
                    datet = ""
                    datet = date_str[6:]
                    final_date = str(day) + "."+str(month)+"."+datet
                    return datetime.strptime(final_date, '%d.%m.%y')
    else:
        return None


def convert_rentender(string):
    lowerCaseString = string.lower()
    if lowerCaseString == "po" or lowerCaseString == "1":
        return "po"
    elif lowerCaseString == "jo" or lowerCaseString == "0":
        return "jo"
    elif lowerCaseString == "n/a" or lowerCaseString == "N/A":
        return "n/a"
    else:
        return ""


def convert_status(num):
    if num != "":
        number = int(num)
        if number == 1:
            return "publikuar"
        elif number == 2:
            return "vlerësim"
        elif number == 3:
            return "anuluar"
        elif number == 4:
            return "kontraktuar"
    else:
        return ""


def convert_flagSatus(num):
    if num != "":
        number = int(num)
        return number
    else:
        return 0


def mySlugify(num):
    if num != "":
        return re.sub('[,]', '', num)
    else:
        return ''


parse()
