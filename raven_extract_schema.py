#given a database_id, this extracts the document type and properties
#import sys
#import re
#import fileinput
import json
import csv
#import time
#from datetime import datetime
import requests
#import StringIO

#need to get a list of collections from the ravenAPI
#see ngp-qa-dev - dbid b50784d2
##Name: Collections
##
##Map: from doc in docs 
##select new { Collection = doc["@metadata"]["Raven-Entity-Name"] };
##
##Reduce: results.GroupBy(r => new {
##    r.Collection
##}).Select(g => new {g.Key.Collection});
##

##the above index provides the list of collections in the database
##hardcode here for now

collections =["Accounts", 
"Appointments",
"BillingSettings",
"CompanyHolidays",
"DispatchWindows",
"ElementIntegrationConfigs",
"InvoiceViews",
"OpenCreditViews",
"PaymentViews",
"QuickBooksIntegrationConfigs",
"QuickBooksOnlineConfigs",
"ResourceAvailabilities",
"SchedulingSettings",
"ServiceAgreements",
"ServiceHours",
"ServiceOfferings",
"ServiceTerritories",
"Teams",
"TechnicianRoutes",
"Technicians",
"UserProfiles",
"WorkOrderDetails",
"WorkOrderSchedulingConstraints"]

def is_dict(a):
    if type(a)==type({}):	
        #print('Structure is a dictionary.')
        return True
    else:
        #print('Structure is not a dictionary.')
        return False

def is_empty(any_structure):
    if any_structure:
        print('Structure is not empty.')
        return False
    else:
        print('Structure is empty.')
        return True

#function to call the API
def callAPI(url, payload, headers, multiplier):
    for iteration in range(0, multiplier):
        print("> " + url )
        r = requests.get(url, params = payload, headers=headers)
        print("< status_code=" + str(r.status_code))
        
    return;

#set up the url components
metadata = 0 #by default, don't extract metadata fields
server = "ngp-qa-db"
port = "8080"
url_base = "http://" + server + ":" + port + "/" 
api_call = "databases"
url = url_base + api_call 
params = {"pageSize" : "1"}

#pick a sample database
dbid  = "b50784d2-f3a6-4d42-adf0-a9ac06e32e2b"

url = url_base + api_call + "/" + dbid + "/indexes/dynamic/Accounts"
params = {"pageSize" : "1"}

#r = requests.get(url, params = params)

#for each doc_type in the collection, pull out
#one document instance for parsing out the JSON
for doc in collections:
    url = url_base + api_call + "/" + dbid + "/indexes/dynamic/" + doc
    r = requests.get(url, params = params)
    s = r.json()
    #this extracts the 1st level JSON property names from the document
    l1_fields = s['Results'][0].keys()
    #loop thru the level 1 fields to check for container fields
    for l1_field in l1_fields:
        #recurse level1
        if is_dict(s['Results'][0][l1_field]): #if the field is a parent
            #get the keys for the child members at level2
            l2_fields = s['Results'][0][l1_field].keys()

            #check each field in level 2
            for l2_field in l2_fields:
                if is_dict(s['Results'][0][l1_field][l2_field]): #if the field is a parent
                #get the keys for the child members at level

                    l3_fields = s['Results'][0][l1_field][l2_field].keys()
                    for l3_field in l3_fields:
                        if is_dict(s['Results'][0][l1_field][l2_field][l3_field]): #if the field is a parent
                            l4_fields = s['Results'][0][l1_field][l2_field][l3_field].keys()
                            for l4_field in l4_fields:
                                print(doc + "," + l1_field + "," + l2_field + "," + l3_field + "," + l4_field) #level4 print
                        else:
                            print(doc + "," + l1_field + "," + l2_field + "," + l3_field) #level3 print

                else:
                    if metadata == 1:
                        print(doc + "," + l1_field + "," + l2_field ) #level2 print
                    else:
                    
                        if l1_field <> "@metadata":
                            print(doc + "," + l1_field + "," + l2_field) #level2 print
            
        else: #no children below level 1
            if metadata == 1:
                print(doc + "," + l1_field) #leve1 print
            else:
                if l1_field <> "@metadata":
                    
                    print(doc + "," + l1_field) #leve1 print
            

stop()    

for line in tenantFile.xreadlines():
    l = [i.strip() for i in line.split(',')]
    lines.append(l)


    #payload = {"accountId" : "accounts/900140"}
    #headers = {"TenantID" : "mock-100k-49efb094-9857-4784-bab9-12c0673a3cdb"}

#close the input and output files
#outputFile.close()
