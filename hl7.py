import time
import sys

import pandas as pd
import phonenumbers

from datetime import datetime
from conf import config

def get_date_components(date):
    if pd.isnull(date):
        raise ValueError(f'Invalid date: {date}')

    m, d, y = date.month, date.day, date.year

    return str(m).zfill(2), str(d).zfill(2), str(y)

def generate_field_list(seq, size):
    fields = ['' for _ in range(size)]
    for i, val in seq.items():
        fields[i] = val

    return fields

def msh(message_time, msg_control_id):
    seq = {
        0: 'MSH', # Segment name
        1: '^~\&', # Encoding characters
        2: 'MEDENT', # Sending application
        3: config['MSH']['client_code'], # Sending facility
        4: config['MSH']['application'], # Receiving application
        5: config['MSH']['application'], # Receiving facility
        6: message_time, # Date/Time of message
        8: 'DFT^P03', # Message Type
        9: msg_control_id, # Message control ID
        10: 'P', # Processing ID
        11: '2.3', # Version ID
        14: 'AL', # Accept Ack. type
        15: 'AL', # Application Ack. type
    }

    return '|'.join(generate_field_list(seq, 17))

def evn(message_time):
    fields = [
        'EVN',
        'DFT^P03', # Event type code
        message_time # Date/Time of event
    ]

    return '|'.join(fields)

def pid(patient):
    m, d, y = get_date_components(patient['Date of Birth (mm/dd/yyyy)'])

    for component in ['Address Line 1', 'City', 'State', 'Zip Code']:
        if not patient[component]:
            raise ValueError(f'{component} empty')

    address = '^'.join([
        patient['Address Line 1'],
        "",
        patient['City'],
        patient['State'],
        str(patient['Zip Code'])
    ])

    try:
        phone = phonenumbers.parse(str(patient['Phone']), 'US')
    except:
        raise ValueError(f'Invalid phone number: {patient["Phone"]}')

    phone = str(phone.national_number)
    phone1, phone2, phone3 = phone[0:3], phone[3:6], phone[6:]

    seq = {
        0: 'PID',
        1: '1', # Patient ID
        5: f'{patient["Last Name"]}^{patient["First Name"]}', # Patient Name,
        7: f'{y}{m}{d}', # Date of Birth,
        8: 'M' if patient['Gender'].lower() == 'male' else 'F', # Sex
        11: address, # Patient address
        13: f'({phone1}){phone2}-{phone3}' # patient phone
    }

    return '|'.join(generate_field_list(seq, 15))

def pv1():
    fields = ['PV1', '1'] + ['' for _ in range(7)]
    return '|'.join(fields)

def ft1(patient):
    m, d, y = get_date_components(patient['Start Time'])
    description = f'Covid-19 Pfizer Vaccine {"1st" if patient["Procedure Code"] == "91300" else "2nd"} Dose'

    seq = {
        0: 'FT1',
        1: '1', # Set ID,
        4: f'{y}{m}{d}', # Transaction date
        6: 'CG', # Transaction type,
        7: f'{patient["Procedure Code"]}^{description}', # Transaction code
        16: config['FT1']['patient_location'], # Assigned patient location
        19: config['FT1']['diagnosis_code'], # Diagnosis Code
        20: config['FT1']['performed_by_code'], # Performed by Code
        21: config['FT1']['performed_by_code'], # Ordered by Code
    }

    return '|'.join(generate_field_list(seq, 27))

def gt1(patient):
    fields = ['GT1', '', ''] + [f'{patient["Last Name"]}^{patient["First Name"]}'] + ['', '', '', '', '']
    return '|'.join(fields)

def in1(patient, insurance):
    if not pd.isnull(patient['Subscriber DOB']):
        m, d, y = get_date_components(patient['Subscriber DOB'])
    else:
        m, d, y = get_date_components(patient['Date of Birth (mm/dd/yyyy)'])

    if patient['Insurance Plan ID'] not in set(insurance['Num']):
        raise ValueError(f'Invalid Insurance Plan ID: {patient["Insurance Plan ID"]}')

    if patient['Primary Insurance Name'].upper() not in set(insurance['Name']):
        raise ValueError(f'Invalid Primary Insurance Name: {patient["Primary Insurance Name"]}')

    if not str(patient['Primary Insurance ID #']):
        raise ValueError(f'Invalid Primary Insurance ID #: {patient["Primary Insurance ID #"]}')

    seq = {
        0: 'IN1',
        1: '1', # Set ID - Insurance
        2: str(patient['Insurance Plan ID']), # Insurance Plan ID
        3: str(patient['Insurance Plan ID']), # Insurance Company ID
        4: patient['Primary Insurance Name'], # Insurance Company Name
        16: f'{patient["Last Name"]}^{patient["First Name"]}', # Name of insured
        17: patient['Subscriber Relation to Patient'] if not pd.isnull(patient['Subscriber Relation to Patient']) else 'self', # Insured's Relation to Pat
        18: f'{y}{m}{d}', # Insured DOB
        36: str(patient['Primary Insurance ID #'])
    }

    return '|'.join(generate_field_list(seq, 37))

def generate_message(patient, message_time, message_control_id, insurance):
    segments = [
        msh(message_time, message_control_id),
        evn(message_time),
        pid(patient),
        pv1(),
        ft1(patient),
        gt1(patient),
        in1(patient, insurance)
    ]

    return '\n'.join(segments)

if __name__ == '__main__':

    if len(sys.argv) < 4:
        print('Usage: python3 hl7.py <input_records> <insurance_ids> <output_dir>')
        exit(1)
        
    data = pd.read_excel(
        sys.argv[1],
        skiprows=[0],
        converters={
            'Start Time': lambda x: pd.to_datetime(x, errors='coerce'),
            'Date of Birth (mm/dd/yyyy)': lambda x: pd.to_datetime(x, errors='coerce'),
            'Subscriber DOB': lambda x: pd.to_datetime(x, errors='coerce')
        }
    )

    insurance = pd.read_excel(sys.argv[2])

    for col in data.columns:
        if data[col].dtype == str:
            data[col] = data[col].str.strip()

    messages = {}
    errors = {}

    for i, patient in data.iterrows():
        message_time = datetime.now().strftime("%Y%m%d%H%M")
        msg_control_id = str(time.time_ns())

        message = ""
        try:
            message = generate_message(patient, message_time, msg_control_id, insurance)
        except ValueError as error:
            errors[i] = str(error)
            continue

        messages[msg_control_id] = message

    if errors:
        with open('errors.txt', 'w') as error_file:
            errors = [f'{id + 2}\t{error}\n' for id, error in errors.items()]
            error_file.write('Line #\tError Message\n')
            error_file.writelines(errors)

        print(f'{len(errors)} errors found. Check errors.txt for details.')

    else:
        for id, message in messages.items():
            with open(f'{sys.argv[3]}/{id}.hl7', 'w') as output_file:
                output_file.write(message)
