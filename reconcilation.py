import contextlib
from sqlalchemy import create_engine
import os
import time
import pandas as pd
import logging
from mailer import send_mail


@contextlib.contextmanager
def connect(connection_string):
    """
    Creates a connection to database, returns connection to calling program and finally closes connection

    Parameters
    ----------
    connection_string : STRING
        String that is used for making connection to database

    Returns
    -------
    connection object
    """
    try:
        engine = create_engine(connection_string, encoding='utf8', echo=False)
        connection = engine.connect()
    except Exception as e:
        print("Error:", e)

    try:
        yield connection
    finally:
        engine.dispose()


def process_and_insert_ncell_file(file_data):
    file_data.rename(columns={'Vendor Trace Id': 'VENDOR_TRACE_ID', 'Mobile Number': 'SERVICE_ATTRIBUTE',
                              'Vendor Trans Id': 'VENDOR_TRANS_ID', 'Amount': 'AMOUNT',
                              'Vendor Code': 'VENDOR_CODE',
                              'Vendor Description': 'VENDOR_DESCRIPTION', 'Transaction Date': 'TXN_DATE'},
                     inplace='TRUE')
    file_data['TXN_DATE'] = pd.to_datetime(file_data['TXN_DATE'], dayfirst=True)

    with connect(connection_credential) as connection:
        file_data[['VENDOR_TRANS_ID', 'SERVICE_ATTRIBUTE', 'VENDOR_TRACE_ID', 'AMOUNT', 'VENDOR_CODE',
                   'VENDOR_DESCRIPTION', 'TXN_DATE', 'FILE_ID']].to_sql('NCELL_TOPUP_RECORD', con=connection,
                                                                        if_exists='append', index=False)

def process_and_insert_ntc_file(file_data):
    file_data.rename(columns={'TransactionId': 'VENDOR_TRANS_ID', 'PhoneId': 'SERVICE_ATTRIBUTE',
                              'Amount': 'AMOUNT', 'StartDate': 'TXN_DATE'}, inplace='TRUE')
    file_data['TXN_DATE'] = pd.to_datetime(file_data['TXN_DATE'])

    with connect(connection_credential) as connection:
        file_data[['FILE_ID', 'VENDOR_TRANS_ID', 'SERVICE_ATTRIBUTE', 'AMOUNT',
                   'TXN_DATE']].to_sql('NTC_TOPUP_RECORD', con=connection, if_exists='append', index=False)


class ReconFile:
    def __init__(self, file_name):
        self.file_name = file_name

        if file_name.startswith('NTC'):
            self.vendor = 'NTC'

        elif file_name.startswith('NCELL'):
            self.vendor = 'NCELL'

    def is_loaded(self):
        return self.file_name in os.listdir(working_dir + '/SUCCESS_PLACE/' + self.vendor)

    def is_loaded_in_db(self):
        return self.get_file_id() is not None

    def move_to_loaded(self):
        os.rename(working_dir + '/PROCESS_PLACE/' + self.vendor + '/' + self.file_name,
                  working_dir + '/SUCCESS_PLACE/' + self.vendor + '/' + self.file_name)

    def move_to_failed(self):
        os.rename(working_dir + '/PROCESS_PLACE/' + self.vendor + '/' + self.file_name,
                  working_dir + '/FAILED_PLACE/' + self.vendor + '/' + self.file_name + str(time.time()))

    def move_to_processing(self):
        os.rename(working_dir + '/UPLOAD_PLACE/' + self.file_name,
                  working_dir + '/PROCESS_PLACE/' + self.vendor + '/' + self.file_name)

    def load_to_recon_config(self):
        with connect(connection_credential) as connection_object:
            connection_object.execute("INSERT INTO RECONCILIATION_CONFIG(FILENAME, STATUS, TYPE) values"
                                      "('{}','PROCESSING','{}')".format(self.file_name, self.vendor))

    def get_file_id(self):
        with connect(connection_credential) as connection_object:
            db_load_result = connection_object.execute(
                'SELECT ID from RECONCILIATION_CONFIG where filename="{}"'.format(self.file_name)).fetchone()
            return db_load_result[0] if db_load_result is not None else None

    def load_to_recon_table(self):
        file_data = pd.read_csv(working_dir + '/PROCESS_PLACE/' + self.vendor + '/' + self.file_name,
                                engine='python', index_col=None, dtype=str)
        file_data['FILE_ID'] = self.get_file_id()

        if self.vendor == 'NTC':
            process_and_insert_ntc_file(file_data)
        elif self.vendor == 'NCELL':
            process_and_insert_ncell_file(file_data)

    def update_status_in_recon_config(self):
        with connect(connection_credential) as db_connection:
            db_connection.execute("UPDATE RECONCILIATION_CONFIG SET STATUS = 'SUCCESS' where "
                                  "FILENAME ='{}'".format(self.file_name))


working_dir = 'C:/Users/rabindra/Desktop/DBATask/Reconcilation/DataFiles'
connection_credential = 'mysql+pymysql://recon_user:recon_password@192.168.56.136/CARDSERVER_SETTLEMENT_JOB'
file_list = os.listdir(working_dir + '/UPLOAD_PLACE')

for file in file_list:
    try:
        print(file)
        file_object = ReconFile(file)

        # It already present then it will throw error due to same name and stay in processing
        # It will be processed again after 5 minutes and this time it executes
        file_object.move_to_processing()

        if file_object.is_loaded() or file_object.is_loaded_in_db():
            print('Already Loaded')
            file_object.move_to_failed()
            send_mail(file_object.file_name, load_status='FAILED')
            continue

        file_object.load_to_recon_config()
        file_object.load_to_recon_table()
        file_object.update_status_in_recon_config()
        file_object.move_to_loaded()
        send_mail(file_object.file_name, load_status='SUCCESS')
        print('Loaded Sucessfully')
    except Exception as e:
        print("Error:", e)