#!/usr/bin/python3
import sys
import json
import time
import re
import datetime
import pandas as pd
from csv import reader
from simple_salesforce import Salesforce

xlsxfile_raw = '/home/bffn/salesforce/data_raw.xlsx'
csvfile = '/home/bffn/salesforce/data.csv'
skip = True  # skip first row
try:
    cred_f = open('/home/bffn/salesforce/cred.csv', 'r')
except FileNotFoundError:
    sys.exit('File does not exist')
cred = [x.strip() for x in cred_f]
sf = Salesforce(username=cred[0], password=cred[1], security_token=cred[2])  # connect to SF
cred_f.close()


#########################
def download_doc():
    print('no_doc')


def data_parser(d_row, acc, obj_type):
    out_json = {}
    data_list = []
    for row in d_row.strip().split('||'):
        row = row.replace(re.search(r'(\s?:\s?)', row).group(), ':').strip()  # to remove whitespaces near :
        row_data = {}
        value = re.split("[a-zA-Z_]+:", row)
        key = re.findall("[a-zA-Z_]+:", row)
        value.remove('')
        for index, key_elem in enumerate(key):
            if key_elem.replace(':', '') == 'Title':
                value[index] = value[index].strip()[:128]
            if key_elem.replace(':', '') == 'Name':
                fio = value[index].split()
                for fio_index, fio_value in enumerate(fio):
                    if obj_type == 'contact':
                        if fio_index == 0:
                            row_data.update({'LastName': fio_value})
                        elif fio_index == 1:
                            row_data.update({'FirstName': fio_value})
                        elif fio_index == 2:
                            row_data.update({'Middle_Name__c': fio_value})
                    elif obj_type == 'sales_rep' or obj_type == 'staff_for_proj':
                        if fio_index == 0:
                            row_data.update({'Name': fio_value})
                        elif fio_index == 1:
                            row_data.update({'First_Name__c': fio_value})
                        elif fio_index == 2:
                            row_data.update({'Middle_Name__c': fio_value})
            else:
                row_data.update({key_elem.replace(':', ''): value[index].strip()})
        data_list.append(row_data)
    out_json.update({'records': data_list, 'Acc': acc})
    return out_json


def bulk_del(d_ids, obj):
    res = []
    for x in d_ids['records']:
        del_id = {'Id': x['Id']}
        res.append(del_id)
    if res:
        obj(res)


# main code
print("Script started at", time.strftime('%X'))
# download_doc()
# Start parse raw file in csvfile
try:
    data = pd.read_excel(xlsxfile_raw)
except FileNotFoundError:
    sys.exit('No raw file')
data.replace({'\n': ' '}, inplace=True, regex=True)
data.to_csv(csvfile, index=False, header=False, date_format='%m/%d/%Y')
# End parse raw file in csvfile
bulk_del(json.loads(json.dumps(sf.query("SELECT Id from Sales_rep_with_opportunity__c"))),
         sf.bulk.Sales_rep_with_opportunity__c.delete)
bulk_del(json.loads(json.dumps(sf.query("SELECT Id from Responsible_presales_person__c"))),
         sf.bulk.Responsible_presales_person__c.delete)
bulk_del(json.loads(json.dumps(sf.query("SELECT Id from Contract"))), sf.bulk.Contract.delete)
bulk_del(json.loads(json.dumps(sf.query("SELECT Id from Contact"))), sf.bulk.Contact.delete)
bulk_del(json.loads(json.dumps(sf.query("SELECT Id from Opportunity"))), sf.bulk.Opportunity.delete)
bulk_del(json.loads(json.dumps(sf.query("SELECT Id from Account"))), sf.bulk.Account.delete)
bulk_del(json.loads(json.dumps(sf.query("SELECT Id from Sales_rep__c"))), sf.bulk.Sales_rep__c.delete)
bulk_del(json.loads(json.dumps(sf.query("SELECT Id from Staff_for_Projects__c"))), sf.bulk.Staff_for_Projects__c.delete)
# bulk_del(json.loads(json.dumps(sf.query("SELECT Id from Task"))), 'Task') Task deleted with accounts
print('Delete complete')
current_date = datetime.datetime.now()
acc_id_name = []
acc_names = []
opp_bulk = []
opp_id_name = []
contact_pre_bulk = []
contact_bulk = []
contract_pre_bulk = []
contract_bulk = []
sales_rep_pre_bulk = []
sales_rep_unique = []
s_r_id_names = []
sales_rep_opp_pre_bulk = []
sales_rep_opp_bulk = []
staff_for_proj_pre_bulk = []
staff_for_proj_unique = []
s_f_p_id_names = []
res_presal_per_pre_bulk = []
res_presal_per__bulk = []
activity_bulk = []
opp_contact_bulk = []
opp_acc_id = []
contact_acc_id = []
opportunity_contact = []
f = open(csvfile, 'r')
for data in reader(f):
    if data[4] == 'Open':
        acc_active = 'Yes'
    else:
        acc_active = 'No'
    acc_data = {'Name': data[0], 'BillingCountry': data[2], 'Active__c': acc_active}  # get acc data
    if acc_data['Name'] not in acc_names:  # check unique account name
        acc_names.append(data[0])
        acc_id_name_j = {'Id': json.loads(json.dumps(sf.Account.create(acc_data)))['id'], 'Name': data[0]}
        acc_id_name.append(acc_id_name_j)
    opp_data = {'Acc': data[0], 'Competencies__c': data[3], 'Name': data[1], 'StageName': 'Prospecting',
                'CloseDate': str(datetime.date.today() + datetime.timedelta(92))}  # get opp data
    opp_bulk.append(opp_data)
    if data[6] != '' and data[6].find(':') != -1:  # check if contact empty
        contact_pre_bulk.append(data_parser(data[6], data[0], 'contact'))  # get contact data
    if data[12] != '' and data[12].find(':') != -1:  # check if contract empty
        contract_pre_bulk.append(data_parser(data[12], data[0], 'contract'))
    if data[5] != '' and data[5].find(':') != -1:  # check if sales_rep empty
        sales_rep_pre_bulk.append(data_parser(data[5], data[1], 'sales_rep'))
    if data[11] != '' and data[11].find(':') != -1:  # check if staff_for_proj_empty
        staff_for_proj_pre_bulk.append(data_parser(data[11], data[1], 'staff_for_proj'))
    if data[8] != '':  # check if last_activity empty
        if data[7] != '':  # check if last_activity_date empty
            activity = data[8].split('||')
            if re.match(r'(\d+/\d+/\d+)', activity[0]):
                last_act = {'ActivityDate': datetime.datetime.strptime(data[7], '%m/%d/%Y').strftime('%Y-%m-%d'),
                            'Subject': activity[0].replace(re.search(r'(\d+/\d+/\d+)', activity[0]).group(),
                                                           '').strip(), 'Priority': 'Normal', 'Status': 'Completed',
                            'Opportunity': data[1]}
                activity_bulk.append(last_act)
                for i in range(1, len(activity), 1):
                    if re.match(r'(\d+/\d+/\d+)', activity[i].strip()):
                        last_act = {
                            'ActivityDate': datetime.datetime.strptime(re.search(r'(\d+/\d+/\d+)', activity[i]).group(),
                                                                       '%m/%d/%Y').strftime('%Y-%m-%d'),
                            'Subject': activity[i].replace(re.search(r'(\d+/\d+/\d+)', activity[i]).group(),
                                                           '').strip(), 'Priority': 'Normal', 'Status': 'Completed',
                            'Opportunity': data[1]}
                        activity_bulk.append(last_act)
            else:
                last_act = {'ActivityDate': datetime.datetime.strptime(data[7], '%m/%d/%Y').strftime('%Y-%m-%d'),
                            'Subject': data[8], 'Priority': 'Normal', 'Status': 'Completed', 'Opportunity': data[1]}
                activity_bulk.append(last_act)
        else:
            activity = data[8].split('||')
            for row in activity:
                last_act = {'ActivityDate': datetime.datetime.strptime(re.search(r'(\d+/\d+/\d+)', row).group(),
                                                                       '%m/%d/%Y').strftime('%Y-%m-%d'),
                            'Subject': row.replace(re.search(r'(\d+/\d+/\d+)', row).group(), '').strip(),
                            'Priority': 'Normal', 'Status': 'Completed', 'Opportunity': data[1]}
                activity_bulk.append(last_act)
    if data[9] == 'TBD':
        cur_date_p_month = current_date + datetime.timedelta(30)
        activity_bulk.append(
            {'ActivityDate': cur_date_p_month.strftime('%Y-%m-%d'), 'Subject': 'TBD', 'Priority': 'Normal',
             'Status': 'Not Started', 'Opportunity': data[1]})
    else:
        if data[10] != '' and data[10] != 'Не требуется':  # check if next_activity empty or не требуется
            if data[9] != '' and data[9] != '-':  # check if next_activity_date empty
                activity = data[10].split('||')
                if re.match(r'(\d+/\d+/\d+)', activity[0]):
                    last_act = {'ActivityDate': datetime.datetime.strptime(data[9], '%m/%d/%Y').strftime('%Y-%m-%d'),
                                'Subject': activity[0].replace(re.search(r'(\d+/\d+/\d+)', activity[0]).group(),
                                                               '').strip(), 'Priority': 'Normal',
                                'Status': 'Not Started', 'Opportunity': data[1]}
                    activity_bulk.append(last_act)
                    for i in range(1, len(activity), 1):
                        if re.match(r'(\d+/\d+/\d+)', activity[i].strip()):
                            last_act = {'ActivityDate': datetime.datetime.strptime(
                                re.search(r'(\d+/\d+/\d+)', activity[i]).group(), '%m/%d/%Y').strftime('%Y-%m-%d'),
                                        'Subject': activity[i].replace(re.search(r'(\d+/\d+/\d+)', activity[i]).group(),
                                                                       '').strip(), 'Priority': 'Normal',
                                        'Status': 'Not Started', 'Opportunity': data[1]}
                            activity_bulk.append(last_act)
                else:
                    last_act = {'ActivityDate': datetime.datetime.strptime(data[9], '%m/%d/%Y').strftime('%Y-%m-%d'),
                                'Subject': data[10], 'Priority': 'Normal', 'Status': 'Not Started',
                                'Opportunity': data[1]}
                    activity_bulk.append(last_act)
            else:
                activity = data[10].split('||')
                for row in activity:
                    last_act = {'ActivityDate': datetime.datetime.strptime(re.search(r'(\d+/\d+/\d+)', row).group(),
                                                                           '%m/%d/%Y').strftime('%Y-%m-%d'),
                                'Subject': row.replace(re.search(r'(\d+/\d+/\d+)', row).group(), '').strip(),
                                'Priority': 'Normal', 'Status': 'Not Started', 'Opportunity': data[1]}
                    activity_bulk.append(last_act)
f.close()
for opp in opp_bulk:  # Add AccId to opportunity JSON
    for data in acc_id_name:
        if data['Name'] == opp['Acc']:
            acc_id = data['Id']
    opp.update({'AccountId': acc_id})
    del opp['Acc']
    opp_id_name_j = {'Id': json.loads(json.dumps(sf.Opportunity.create(opp)))['id'], 'Name': opp['Name']}
    opp_acc_id.append({'OppId': opp_id_name_j['Id'], 'AccountId': acc_id})
    opp_id_name.append(opp_id_name_j)
for cont in contact_pre_bulk:  # Add AccId to contact JSON
    for data in acc_id_name:
        if data['Name'] == cont['Acc']:
            acc_id = data['Id']
    for row in cont['records']:
        row.update({'AccountId': acc_id})
        contact_acc_id.append(
            {'ContactId': json.loads(json.dumps(sf.Contact.create(row)))['id'], 'AccId': row['AccountId']})
for contr in contract_pre_bulk:
    for data in acc_id_name:
        if data['Name'] == contr['Acc']:
            contr['records'][0].update({'AccountId': data['Id']})  # Add AccId
            contract_bulk.append(contr['records'][0])
sf.bulk.Contract.insert(contract_bulk)
# START SECTION OF GETTING Sales rep DATA
for sales_reps in sales_rep_pre_bulk:
    sales_reps = sales_reps['records']
    for row in sales_reps:  # All this for unique sales_rep
        if len(sales_rep_unique) == 0:
            sales_rep_unique.append(row)
            s_r_id_names_j = {'Id': json.loads(json.dumps(sf.Sales_rep__c.create(row)))['id'],
                              'First_Name__c': row['First_Name__c'], 'Name': row['Name']}
            s_r_id_names.append(s_r_id_names_j)
            continue
        to_insert = True
        for sal_rep_to_ins in sales_rep_unique:
            if sal_rep_to_ins['Name'] == row['Name'] and sal_rep_to_ins['First_Name__c'] == row['First_Name__c']:
                to_insert = False
                break
        if (to_insert):
            sales_rep_unique.append(row)
            s_r_id_names_j = {'Id': json.loads(json.dumps(sf.Sales_rep__c.create(row)))['id'],
                              'First_Name__c': row['First_Name__c'], 'Name': row['Name']}
            s_r_id_names.append(s_r_id_names_j)
# END SECTION OF GETTING Sales rep DATA
# START SECTION OF GETTING Sales rep with opportunity DATA
for sales_reps in sales_rep_pre_bulk:
    sales_reps_m = sales_reps['records']
    for sales_rep in sales_reps_m:
        sales_rep.update({'opp_name': sales_reps['Acc']})
        for s_r_id_names_row in s_r_id_names:
            if s_r_id_names_row['Name'] == sales_rep['Name'] and s_r_id_names_row['First_Name__c'] == sales_rep[
                'First_Name__c']:
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
# END SECTION OF GETTING Sales rep with opportunity DATA
# START SECTION OF GETTING Staff for Projects DATA
for staff_for_projs in staff_for_proj_pre_bulk:
    staff_for_projs = staff_for_projs['records']
    for row in staff_for_projs:
        if len(staff_for_proj_unique) == 0:
            staff_for_proj_unique.append(row)
            s_f_p_id_names_j = {'Id': json.loads(json.dumps(sf.Staff_for_Projects__c.create(row)))['id'],
                                'First_Name__c': row['First_Name__c'], 'Name': row['Name']}
            s_f_p_id_names.append(s_f_p_id_names_j)
            continue
        to_insert = True
        for staf_f_proj_to_ins in staff_for_proj_unique:
            if staf_f_proj_to_ins['Name'] == row['Name'] and staf_f_proj_to_ins['First_Name__c'] == row[
                'First_Name__c']:
                to_insert = False
                break
        if (to_insert):
            staff_for_proj_unique.append(row)
            s_f_p_id_names_j = {'Id': json.loads(json.dumps(sf.Staff_for_Projects__c.create(row)))['id'],
                                'First_Name__c': row['First_Name__c'], 'Name': row['Name']}
            s_f_p_id_names.append(s_f_p_id_names_j)
# END SECTION OF GETTING Staff for Projects DATA
# START SECTION OF GETTING Responsible presales person DATA
for staff_for_projs in staff_for_proj_pre_bulk:
    staff_for_projs_m = staff_for_projs['records']
    for staff_for_proj in staff_for_projs_m:
        staff_for_proj.update({'opp_name': staff_for_projs['Acc']})
        for s_f_p_id_names_row in s_f_p_id_names:
            if s_f_p_id_names_row['Name'] == staff_for_proj['Name'] and s_f_p_id_names_row['First_Name__c'] == \
                    staff_for_proj['First_Name__c']:
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
# END SECTION OF GETTING Responsible presales person DATA
for row in activity_bulk:  # for Tasks
    for opp_id in opp_id_name:
        if row['Opportunity'] == opp_id['Name']:
            row.update({'WhatId': opp_id['Id']})
            del row['Opportunity']
            break
for opp_acc_row in opp_acc_id:  # for OpportunityContact
    for cont_acc_row in contact_acc_id:
        if opp_acc_row['AccountId'] == cont_acc_row['AccId']:
            opportunity_contact.append({'OpportunityId': opp_acc_row['OppId'], 'ContactId': cont_acc_row['ContactId']})
sf.bulk.Task.insert(activity_bulk)
sf.bulk.OpportunityContactRole.insert(opportunity_contact)
print("Reload complete")
print("Script finished at", time.strftime('%X'))
