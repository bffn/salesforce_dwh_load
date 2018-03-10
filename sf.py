#!/usr/bin/python3
import sys
import json
import time
import re
import datetime
from simple_salesforce import Salesforce
csvfile = '/home/bffn/salesforse/csvdata.csv'
skip=True  #skip first row
try:
    cred_f=open('/home/bffn/salesforse/cred.csv','r')
except FileNotFoundError:
    sys.exit('File does not exist')
cred=[x.strip() for x in cred_f]
sf = Salesforce(username=cred[0], password=cred[1], security_token=cred[2]) #connect to SF
cred_f.close()
#########################
def data_parser(d_row, acc): 
    out_json={}
    data_list=[]
    for row in d_row.strip().split('||'):
        row_data={}
        for field in row.split(';'):
            p_f=field.split(':')
            row_data.update({p_f[0]: p_f[1]})
        data_list.append(row_data)
    out_json.update({'records': data_list, 'Acc': acc})
    return out_json

def bulk_del(d_ids, obj): #del data from SF
    res=[]
    for x in d_ids['records']:
        del_id={'Id': x['Id']}
        res.append(del_id)
    if res:
        if obj=='Account':
            sf.bulk.Account.delete(res)
        elif obj=='Opportunity':
            sf.bulk.Opportunity.delete(res)
        elif obj=='Contact':
            sf.bulk.Contact.delete(res)
        elif obj=='Contract':
            sf.bulk.Contract.delete(res)
        elif obj=='Sales_rep__c':
            sf.bulk.Sales_rep__c.delete(res)
        elif obj=='Sales_rep_with_opportunity__c':
            sf.bulk.Sales_rep_with_opportunity__c.delete(res)
        elif obj=='Staff_for_Projects__c':
            sf.bulk.Staff_for_Projects__c.delete(res)
        elif obj=='Responsible_presales_person__c':
            sf.bulk.Responsible_presales_person__c.delete(res)
        elif obj=='Task':
            sf.bulk.Task.delete(res)
#main code
bulk_del(json.loads(json.dumps(sf.query("SELECT Id from Sales_rep_with_opportunity__c"))), 'Sales_rep_with_opportunity__c')
bulk_del(json.loads(json.dumps(sf.query("SELECT Id from Responsible_presales_person__c"))), 'Responsible_presales_person__c')
bulk_del(json.loads(json.dumps(sf.query("SELECT Id from Contract"))), 'Contract')
bulk_del(json.loads(json.dumps(sf.query("SELECT Id from Contact"))), 'Contact')
bulk_del(json.loads(json.dumps(sf.query("SELECT Id from Opportunity"))), 'Opportunity')
bulk_del(json.loads(json.dumps(sf.query("SELECT Id from Account"))), 'Account')
bulk_del(json.loads(json.dumps(sf.query("SELECT Id from Sales_rep__c"))), 'Sales_rep__c')
bulk_del(json.loads(json.dumps(sf.query("SELECT Id from Staff_for_Projects__c"))), 'Staff_for_Projects__c')
bulk_del(json.loads(json.dumps(sf.query("SELECT Id from Task"))), 'Task')
print('Delete complete')
try:
    f=open(csvfile,'r')
except FileNotFoundError: 
    sys.exit('File does not exist')
acc_id_name=[]
acc_names=[] #for unique acc
opp_bulk=[]
opp_id_name=[]
contact_pre_bulk=[]
contact_bulk=[]
contract_pre_bulk=[]
contract_bulk=[]
sales_rep_pre_bulk=[]
sales_rep_unique=[]
s_r_id_names=[]
sales_rep_opp_pre_bulk=[]
sales_rep_opp_bulk=[]
staff_for_proj_pre_bulk=[]
staff_for_proj_unique=[]
s_f_p_id_names=[]
res_presal_per_pre_bulk=[]
res_presal_per__bulk=[]
activity_bulk=[]
for row in f:
    if skip:  #skip first row
        skip=False
        continue
    data=row.strip().split(',')
    acc_data={'Name': data[0], 'BillingCountry': data[1]} #get acc data
    if acc_data['Name'] not in acc_names: #check unique account name
        acc_names.append(data[0])
        acc_id_name_j={'Id':json.loads(json.dumps(sf.Account.create(acc_data)))['id'], 'Name': data[0]}
        acc_id_name.append(acc_id_name_j)
    opp_data={'Acc': data[0], 'Name':'Opportunity for '+data[0], 'Competence__c': data[2], 'Name': data[3], 'StageName': 'Closed Won', 'CloseDate': '2018-12-31'}  #get opp data
    #if data[4] != '': #add Project_of_Sale__c to opp to get contract id later; check if contract empty #FOR CONTRACTID IN OPPORTUNITY(DON'T DELETE!!!)
        #opp_data.update({'Contract_num': data_parser(data[4],data[0])['records'][0]['Project_of_Sale__c']}) #FOR CONTRACTID IN OPPORTUNITY(DON'T DELETE!!!)
    opp_bulk.append(opp_data)
    if data[4] != '': #check if contact empty
        contact_pre_bulk.append(data_parser(data[4], data[0])) #get contact data
    if data[5] != '': #check if contract empty
        contract_pre_bulk.append(data_parser(data[5],data[0]))
    if data[6] != '': #check if sales_rep empty
        sales_rep_pre_bulk.append(data_parser(data[6],data[3]))
    if data[7] != '': #check if staff_for proj_empty
        staff_for_proj_pre_bulk.append(data_parser(data[7],data[3]))
    if data[9] != '': #check if last_activity empty
        if data[8] != '': #check if last_activity_date empty
            activity=data[9].split('||')
            if re.match(r'(\d+/\d+/\d+)',activity[0]):
                last_act={'ActivityDate': datetime.datetime.strptime(data[8], '%m/%d/%Y').strftime('%Y-%m-%d'), 'Subject': activity[0].replace(re.search(r'(\d+/\d+/\d+)',activity[0]).group(),'').strip(), 'WhatId': acc_id_name_j['Id'], 'Priority': 'Normal', 'Status': 'Completed'}
                activity_bulk.append(last_act)
                for i in range(1,len(activity),1):
                    if re.match(r'(\d+/\d+/\d+)',activity[i]):
                         last_act={'ActivityDate': datetime.datetime.strptime(re.search(r'(\d+/\d+/\d+)',activity[i]).group(), '%m/%d/%Y').strftime('%Y-%m-%d'), 'Subject': activity[i].replace(re.search(r'(\d+/\d+/\d+)',activity[i]).group(),'').strip(), 'WhatId': acc_id_name_j['Id'], 'Priority': 'Normal', 'Status': 'Completed'}
                         activity_bulk.append(last_act)
            else:
                last_act={'ActivityDate': datetime.datetime.strptime(data[8], '%m/%d/%Y').strftime('%Y-%m-%d'), 'Subject': data[9], 'WhatId': acc_id_name_j['Id'], 'Priority': 'Normal', 'Status': 'Completed'}
                activity_bulk.append(last_act)
        else:
            activity=data[9].split('||')
            for row in activity:
                last_act={'ActivityDate': datetime.datetime.strptime(re.search(r'(\d+/\d+/\d+)',row).group(), '%m/%d/%Y').strftime('%Y-%m-%d'), 'Subject': row.replace(re.search(r'(\d+/\d+/\d+)',row).group(),'').strip(), 'WhatId': acc_id_name_j['Id'], 'Priority': 'Normal', 'Status': 'Completed'}
                activity_bulk.append(last_act)
    if data[11] != '': #check if next_activity empty
        if data[10] != '': #check if next_activity_date empty
            activity=data[11].split('||')
            if re.match(r'(\d+/\d+/\d+)',activity[0]):
                last_act={'ActivityDate': datetime.datetime.strptime(data[10], '%m/%d/%Y').strftime('%Y-%m-%d'), 'Subject': activity[0].replace(re.search(r'(\d+/\d+/\d+)',activity[0]).group(),'').strip(), 'WhatId': acc_id_name_j['Id'], 'Priority': 'Normal', 'Status': 'Not Started'}
                activity_bulk.append(last_act)
                for i in range(1,len(activity),1):
                    if re.match(r'(\d+/\d+/\d+)',activity[i]):
                         last_act={'ActivityDate': datetime.datetime.strptime(re.search(r'(\d+/\d+/\d+)',activity[i]).group(), '%m/%d/%Y').strftime('%Y-%m-%d'), 'Subject': activity[i].replace(re.search(r'(\d+/\d+/\d+)',activity[i]).group(),'').strip(), 'WhatId': acc_id_name_j['Id'], 'Priority': 'Normal', 'Status': 'Not Started'}
                         activity_bulk.append(last_act)
            else:
                last_act={'ActivityDate': datetime.datetime.strptime(data[10], '%m/%d/%Y').strftime('%Y-%m-%d'), 'Subject': data[11], 'WhatId': acc_id_name_j['Id'], 'Priority': 'Normal', 'Status': 'Not Started'}
                activity_bulk.append(last_act)
        else:
            activity=data[11].split('||')
            for row in activity:
                last_act={'ActivityDate': datetime.datetime.strptime(re.search(r'(\d+/\d+/\d+)',row).group(), '%m/%d/%Y').strftime('%Y-%m-%d'), 'Subject': row.replace(re.search(r'(\d+/\d+/\d+)',row).group(),'').strip(), 'WhatId': acc_id_name_j['Id'], 'Priority': 'Normal', 'Status': 'Not Started'}
                activity_bulk.append(last_act)
f.close()
for cont in contact_pre_bulk: #Add AccId to contact JSON
    for data in acc_id_name:
         if data['Name'] == cont['Acc']:
             acc_id=data['Id']
    for row in cont['records']:
        row.update({'AccountId': acc_id})
        contact_bulk.append(row)
sf.bulk.Contact.insert(contact_bulk)
for contr in contract_pre_bulk:
    for data in acc_id_name:
        if data['Name'] == contr['Acc']:
             contr['records'][0].update({'AccountId': data['Id']}) #Add AccId
             contr['records'][0].update({'CustomerSignedId': json.loads(json.dumps(sf.query("SELECT Id from Contact WHERE FirstName = " + "'" + row['FirstName'] + "'" + " and LastName = " + "'" + row['LastName'] + "'")))['records'][0]['Id']}) #Add Contact Id to contract
             del contr['records'][0]['FirstName']
             del contr['records'][0]['LastName']
             contract_bulk.append(contr['records'][0])
sf.bulk.Contract.insert(contract_bulk)
for opp in opp_bulk: #Add AccId to opportunity JSON
    for data in acc_id_name:
        if data['Name'] == opp['Acc']:
            acc_id=data['Id']
    opp.update({'AccountId': acc_id})
    del opp['Acc']
    opp_id_name_j={'Id':json.loads(json.dumps(sf.Opportunity.create(opp)))['id'], 'Name': opp['Name']}
    opp_id_name.append(opp_id_name_j)
    #opp.update({'ContractId': json.loads(json.dumps(sf.query("SELECT Id from Contract WHERE Project_of_Sale__c = " + opp['Contract_num'])))['records'][0]['Id']}) #FOR CONTRACTID IN OPPORTUNITY(DON'T DELETE!!!)
    #del opp['Contract_num'] #FOR CONTRACTID IN OPPORTUNITY
#START SECTION OF GETTING Sales rep DATA
for sales_reps in sales_rep_pre_bulk:
    sales_reps = sales_reps['records']
    for row in sales_reps:  #All this $#%@ for unique sales_rep
        if len(sales_rep_unique) == 0:
            sales_rep_unique.append(row)
            s_r_id_names_j={'Id':json.loads(json.dumps(sf.Sales_rep__c.create(row)))['id'], 'First_Name__c': row['First_Name__c'], 'Name': row['Name']}
            s_r_id_names.append(s_r_id_names_j)
            continue
        to_insert = True
        for sal_rep_to_ins in sales_rep_unique:
            if sal_rep_to_ins['Name'] == row['Name'] and sal_rep_to_ins['First_Name__c'] == row['First_Name__c']:
                to_insert=False
                break
        if (to_insert):
            sales_rep_unique.append(row)
            s_r_id_names_j={'Id':json.loads(json.dumps(sf.Sales_rep__c.create(row)))['id'], 'First_Name__c': row['First_Name__c'], 'Name': row['Name']}
            s_r_id_names.append(s_r_id_names_j)
#END SECTION OF GETTING Sales rep DATA
#START SECTION OF GETTING Sales rep with opportunity DATA
for sales_reps in sales_rep_pre_bulk:
    sales_reps_m = sales_reps['records']
    for sales_rep in sales_reps_m:
        sales_rep.update({'opp_name': sales_reps['Acc']})
        for s_r_id_names_row in s_r_id_names:
            if s_r_id_names_row['Name'] == sales_rep['Name'] and s_r_id_names_row['First_Name__c'] == sales_rep['First_Name__c']:
                sales_rep.update({'Sales_rep__c': s_r_id_names_row['Id']})
                sales_rep_opp_pre_bulk.append(sales_rep)
for sales_rep_opp_pre_bulk_row in sales_rep_opp_pre_bulk:
    for opp_id_name_row in opp_id_name:
        if sales_rep_opp_pre_bulk_row['opp_name'] == opp_id_name_row['Name']:
           sales_rep_opp_pre_bulk_row.update({'Opportunity__c': opp_id_name_row['Id']})
           del sales_rep_opp_pre_bulk_row['First_Name__c']
           del sales_rep_opp_pre_bulk_row['Name']
           del sales_rep_opp_pre_bulk_row['opp_name']
           sales_rep_opp_bulk.append(sales_rep_opp_pre_bulk_row)
           break
sf.bulk.Sales_rep_with_opportunity__c.insert(sales_rep_opp_bulk)
#END SECTION OF GETTING Sales rep with opportunity DATA
#START SECTION OF GETTING Staff for Projects DATA
for staff_for_projs in staff_for_proj_pre_bulk:
    staff_for_projs = staff_for_projs['records']
    for row in staff_for_projs:
        if len(staff_for_proj_unique) == 0:
            staff_for_proj_unique.append(row)
            s_f_p_id_names_j={'Id':json.loads(json.dumps(sf.Staff_for_Projects__c.create(row)))['id'], 'First_Name__c': row['First_Name__c'], 'Name': row['Name']}
            s_f_p_id_names.append(s_f_p_id_names_j)
            continue
        to_insert = True
        for staf_f_proj_to_ins in staff_for_proj_unique:
            if staf_f_proj_to_ins['Name'] == row['Name'] and staf_f_proj_to_ins['First_Name__c'] == row['First_Name__c']:
                to_insert=False
                break
        if (to_insert):
             staff_for_proj_unique.append(row)
             s_f_p_id_names_j={'Id':json.loads(json.dumps(sf.Staff_for_Projects__c.create(row)))['id'], 'First_Name__c': row['First_Name__c'], 'Name': row['Name']}
             s_f_p_id_names.append(s_f_p_id_names_j)
#END SECTION OF GETTING Staff for Projects DATA
#START SECTION OF GETTING Responsible presales person DATA
for staff_for_projs in staff_for_proj_pre_bulk:
    staff_for_projs_m = staff_for_projs['records']
    for staff_for_proj in staff_for_projs_m:
        staff_for_proj.update({'opp_name': staff_for_projs['Acc']})
        for s_f_p_id_names_row in s_f_p_id_names:
            if s_f_p_id_names_row['Name'] == staff_for_proj['Name'] and s_f_p_id_names_row['First_Name__c'] == staff_for_proj['First_Name__c']:
                staff_for_proj.update({'Staff_for_Projects__c': s_f_p_id_names_row['Id']})
                res_presal_per_pre_bulk.append(staff_for_proj)
for res_presal_per_pre_bulk_row in res_presal_per_pre_bulk:
    for opp_id_name_row in opp_id_name:
        if res_presal_per_pre_bulk_row['opp_name'] == opp_id_name_row['Name']:
            res_presal_per_pre_bulk_row.update({'Opportunity__c': opp_id_name_row['Id']})
            del res_presal_per_pre_bulk_row['First_Name__c']
            del res_presal_per_pre_bulk_row['Name']
            del res_presal_per_pre_bulk_row['opp_name']
            res_presal_per__bulk.append(res_presal_per_pre_bulk_row)
            break
sf.bulk.Responsible_presales_person__c.insert(res_presal_per__bulk)
#END SECTION OF GETTING Responsible presales person DATA
sf.bulk.Task.insert(activity_bulk)
