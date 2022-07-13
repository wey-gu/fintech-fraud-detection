import csv
import os

import pandas as pd

from faker import Faker
from pydbgen import pydbgen
from random import randint


CLIENT_COUNT = 100000
ACCOUNT_COUNT = 10000
LOCATION_COUNT = 200000
RELATIVE_COUNT = 100000
PHONE_COUNT = 1000
IP_COUNT = 1000
COMPANY_COUNT = 200
VOUCHER_COUNT = 200
CONT_COUNT = 100
DEVICE_COUNT = 500
TRANSACTION_COUNT = 100000
WORK_FOR_COUNT = 100
IS_RELATIVE_TO_COUNT = 100
WITH_REL_PHONE_COUNT = 100
WITH_REGISTER_PHONE_COUNT = 100
VOUCHER_CLIENT_COUNT = 100
CONT_VOUCHER_COUNT = 100
LOG_IN_LOC_COUNT = 100
LOG_IN_IP_COUNT = 100
LOG_IN_DEV_COUNT = 100
OWN_COUNT = 100
REGISTER_LOC_COUNT = 100
REGISTER_IP_COUNT = 100
REGISTER_DEV_COUNT = 100
TRANSACTION_DEV_COUNT = 100
TRANSACTION_IP_COUNT = 100
TRANSACTION_LOC_COUNT = 100
DEBIT_SRC_COUNT = 100
DEBIT_DEST_COUNT = 100
TRANSFER_SRC_COUNT = 100
TRANSFER_DEST_COUNT = 100
PAYMENT_SRC_COUNT = 100
PAYMENT_DEST_COUNT = 100

WRITE_BATCH = 1000


def csv_writer(file_path, row_count, row_generator):
    with open(file_path, mode="w") as file:
        writer = csv.writer(
            file, delimiter=",", quotechar="'", quoting=csv.QUOTE_MINIMAL
        )
        csv_buffer = list()
        for row in range(row_count):
            csv_buffer.append(row_generator())
            if len(csv_buffer) > WRITE_BATCH:
                writer.writerows(csv_buffer)
                del csv_buffer[:]
        if csv_buffer:
            writer.writerows(csv_buffer)
            del csv_buffer[:]


def csv_writer_ng(file_path, row_count, row_generator, index=False, index_prefix=""):
    with open(file_path, mode='w') as file:
        if index:
            cursor = 0
        writer = csv.writer(
            file, delimiter=',', quotechar="'", quoting=csv.QUOTE_MINIMAL)
        csv_buffer = list()
        for row in range(row_count):
            if index:
                csv_buffer.append((f"{index_prefix}{cursor}",) + row_generator())
                cursor += 1
            else:
                csv_buffer.append(row_generator())
            if len(csv_buffer) > WRITE_BATCH:
                writer.writerows(csv_buffer)
                del csv_buffer[:]
        if csv_buffer:
            writer.writerows(csv_buffer)
            del csv_buffer[:]


generator = pydbgen.pydb()
faker = Faker()

if not os.path.exists("./data"):
    os.makedirs("./data")

# client
# client (client_id STRING NOT NULL, client_name STRING)

client_df = generator.gen_dataframe(num=CLIENT_COUNT, fields=["name"])
client_pd = pd.DataFrame(client_df)
client_pd = client_pd.set_index("client_" + client_pd.index.astype(str))
client_pd.to_csv("data/client.csv", header=False, quoting=csv.QUOTE_MINIMAL)

# account
# account (acc_id STRING NOT NULL)
account_df = generator.gen_dataframe(num=ACCOUNT_COUNT)
account_pd = pd.DataFrame(account_df)
account_pd = account_pd.set_index("acc_" + account_pd.index.astype(str))
account_pd.to_csv("data/account.csv", header=False, quoting=csv.QUOTE_MINIMAL)

# location
# location (loc_id STRING NOT NULL, longitude float, latitude float)
location_df = generator.gen_dataframe(
    num=LOCATION_COUNT, fields=["longitude", "latitude"]
)
location_pd = pd.DataFrame(location_df)
location_pd = location_pd.set_index("loc_" + location_pd.index.astype(str))
location_pd.to_csv("data/location.csv", header=False, quoting=csv.QUOTE_MINIMAL)

# ip
# ip (ip_add STRING NOT NULL)
ip_df = generator.gen_dataframe(num=IP_COUNT)
ip_pd = pd.DataFrame(ip_df)
ip_pd = ip_pd.set_index("ip_" + ip_pd.index.astype(str))
ip_pd.to_csv("data/ip.csv", header=False, quoting=csv.QUOTE_MINIMAL)

# company
# company (com_id STRING NOT NULL, com_name STRING NOT NULL)

company_df = generator.gen_dataframe(num=COMPANY_COUNT, fields=["company"])
company_pd = pd.DataFrame(company_df)
company_pd = company_pd.set_index("com_" + company_pd.index.astype(str))
company_pd.to_csv("data/company.csv", header=False, quoting=csv.QUOTE_MINIMAL)

# relative
# relative (rel_id STRING NOT NULL, rel_name STRING)
relative_df = generator.gen_dataframe(num=RELATIVE_COUNT, fields=["name"])
relative_pd = pd.DataFrame(relative_df)
relative_pd = relative_pd.set_index("rel_" + relative_pd.index.astype(str))
relative_pd.to_csv("data/relative.csv", header=False, quoting=csv.QUOTE_MINIMAL)

# phone
# phone (phone_num STRING NOT NULL)
phone_df = generator.gen_dataframe(num=PHONE_COUNT, fields=["phone"])
phone_pd = pd.DataFrame(phone_df)
phone_pd = phone_pd.set_index("phone_" + phone_pd.index.astype(str))
phone_pd.to_csv("data/phone.csv", header=False, quoting=csv.QUOTE_MINIMAL)

# voucher
# voucher (voucher_id STRING NOT NULL, opt_kind STRING, loan_date date, end_date date, loan_sum float)
voucher_df = generator.gen_dataframe(num=VOUCHER_COUNT, fields=["date", "date", "age"])
voucher_pd = pd.DataFrame(voucher_df)
voucher_pd = voucher_pd.set_index("voucher_" + voucher_pd.index.astype(str))
voucher_pd.to_csv("data/voucher.csv", header=False, quoting=csv.QUOTE_MINIMAL)

# cont
# cont (cont_id STRING NOT NULL)
cont_df = generator.gen_dataframe(num=CONT_COUNT)
cont_pd = pd.DataFrame(cont_df)
cont_pd = cont_pd.set_index("cont_" + cont_pd.index.astype(str))
cont_pd.to_csv("data/cont.csv", header=False, quoting=csv.QUOTE_MINIMAL)

# device
# device (dev_id STRING NOT NULL)
device_df = generator.gen_dataframe(num=DEVICE_COUNT)
device_pd = pd.DataFrame(device_df)
device_pd = device_pd.set_index("dev_" + device_pd.index.astype(str))
device_pd.to_csv("data/device.csv", header=False, quoting=csv.QUOTE_MINIMAL)

# transaction
# transaction (transaction_id STRING NOT NULL , src_name STRING NOT NULL, dest_name STRING NOT NULL, amount double NOT NULL , is_fraud bool NOT NULL)
def trasnaction_generator():
    """
    (transaction_id STRING NOT NULL , src_name STRING NOT NULL, dest_name STRING NOT NULL, amount double NOT NULL , is_fraud bool NOT NULL)
    """
    amount = faker.random_int(min=10000, max=5000000)
    is_fraud = faker.boolean()
    return (
        faker.name(),
        faker.name(),
        amount,
        is_fraud)

csv_writer_ng(
    'data/trasnaction.csv',
    TRANSACTION_COUNT,
    trasnaction_generator,
    index=True,
    index_prefix="transaction_")

# work_for
def work_for_generator():
    """
    work_for (client_id, com_id)
    """
    return (
        "client_" + str(randint(0, CLIENT_COUNT - 1)),
        "com_" + str(randint(0, COMPANY_COUNT - 1)),
    )


csv_writer("data/work_for.csv", WORK_FOR_COUNT, work_for_generator)

# is_relative_to (client_id, rel_id)


def is_relative_to_generator():
    """
    is_relative_to (client_id, rel_id)
    """
    return (
        "client_" + str(randint(0, CLIENT_COUNT - 1)),
        "rel_" + str(randint(0, RELATIVE_COUNT - 1)),
    )


csv_writer("data/is_relative_to.csv", IS_RELATIVE_TO_COUNT, is_relative_to_generator)

# with_rel_phone (rel_id, phone_num)


def with_rel_phone_generator():
    """
    with_rel_phone (rel_id, phone_id)
    """
    return (
        "rel_" + str(randint(0, RELATIVE_COUNT - 1)),
        "phone_" + str(randint(0, PHONE_COUNT - 1)),
    )


csv_writer("data/with_rel_phone.csv", WITH_REL_PHONE_COUNT, with_rel_phone_generator)

# with_register_phone (client_id, phone_num)


def with_register_phone_generator():
    """
    with_register_phone (client_id, phone_id)
    """
    return (
        "client_" + str(randint(0, CLIENT_COUNT - 1)),
        "phone_" + str(randint(0, PHONE_COUNT - 1)),
    )


csv_writer(
    "data/with_register_phone.csv",
    WITH_REGISTER_PHONE_COUNT,
    with_register_phone_generator,
)

# voucher_client (client_id, voucher_id)


def voucher_client_generator():
    """
    voucher_client (client_id, voucher_id)
    """
    return (
        "client_" + str(randint(0, CLIENT_COUNT - 1)),
        "voucher_" + str(randint(0, VOUCHER_COUNT - 1)),
    )


csv_writer("data/voucher_client.csv", VOUCHER_CLIENT_COUNT, voucher_client_generator)

# cont_voucher (voucher_id, cont_id)


def cont_voucher_generator():
    """
    cont_voucher (voucher_id, cont_id)
    """
    return (
        "voucher_" + str(randint(0, VOUCHER_COUNT - 1)),
        "cont_" + str(randint(0, CONT_COUNT - 1)),
    )


csv_writer("data/cont_voucher.csv", CONT_VOUCHER_COUNT, cont_voucher_generator)

# log_in_loc (client_id, loc_id)


def log_in_loc_generator():
    """
    log_in_loc (client_id, loc_id)
    """
    return (
        "client_" + str(randint(0, CLIENT_COUNT - 1)),
        "loc_" + str(randint(0, LOCATION_COUNT - 1)),
    )


csv_writer("data/log_in_loc.csv", LOG_IN_LOC_COUNT, log_in_loc_generator)

# log_in_ip (client_id, ip_add)


def log_in_ip_generator():
    """
    log_in_ip (client_id, ip_add)
    """
    return (
        "client_" + str(randint(0, CLIENT_COUNT - 1)),
        "ip_" + str(randint(0, IP_COUNT - 1)),
    )


csv_writer("data/log_in_ip.csv", LOG_IN_IP_COUNT, log_in_ip_generator)

# log_in_dev (client_id, dev_id)


def log_in_dev_generator():
    """
    log_in_dev (client_id, dev_id)
    """
    return (
        "client_" + str(randint(0, CLIENT_COUNT - 1)),
        "dev_" + str(randint(0, DEVICE_COUNT - 1)),
    )


csv_writer("data/log_in_dev.csv", LOG_IN_DEV_COUNT, log_in_dev_generator)

# own (client_id, acc_id)


def own_generator():
    """
    own (client_id, acc_id)
    """
    return (
        "client_" + str(randint(0, CLIENT_COUNT - 1)),
        "acc_" + str(randint(0, ACCOUNT_COUNT - 1)),
    )


csv_writer("data/own.csv", OWN_COUNT, own_generator)

# register_loc (acc_id, loc_id)


def register_loc_generator():
    """
    register_loc (acc_id, loc_id)
    """
    return (
        "acc_" + str(randint(0, ACCOUNT_COUNT - 1)),
        "loc_" + str(randint(0, LOCATION_COUNT - 1)),
    )


csv_writer("data/register_loc.csv", REGISTER_LOC_COUNT, register_loc_generator)

# register_ip (acc_id, ip_add)


def register_ip_generator():
    """
    register_ip (acc_id, ip_add)
    """
    return (
        "acc_" + str(randint(0, ACCOUNT_COUNT - 1)),
        "ip_" + str(randint(0, IP_COUNT - 1)),
    )


csv_writer("data/register_ip.csv", REGISTER_IP_COUNT, register_ip_generator)

# register_dev (acc_id, dev_id)


def register_dev_generator():
    """
    register_dev (acc_id, dev_id)
    """
    return (
        "acc_" + str(randint(0, ACCOUNT_COUNT - 1)),
        "dev_" + str(randint(0, DEVICE_COUNT - 1)),
    )


csv_writer("data/register_dev.csv", REGISTER_DEV_COUNT, register_dev_generator)

# transaction_dev (transaction_id, dev_id)


def transaction_dev_generator():
    """
    transaction_dev (transaction_id, dev_id)
    """
    return (
        "transaction_" + str(randint(0, TRANSACTION_COUNT - 1)),
        "dev_" + str(randint(0, DEVICE_COUNT - 1)),
    )


csv_writer("data/transaction_dev.csv", TRANSACTION_DEV_COUNT, transaction_dev_generator)

# transaction_ip (transaction_id, ip_add)


def transaction_ip_generator():
    """
    transaction_ip (transaction_id, ip_add)
    """
    return (
        "transaction_" + str(randint(0, TRANSACTION_COUNT - 1)),
        "ip_" + str(randint(0, IP_COUNT - 1)),
    )


csv_writer("data/transaction_ip.csv", TRANSACTION_IP_COUNT, transaction_ip_generator)

# transaction_loc (transaction_id, loc_id )


def transaction_loc_generator():
    """
    transaction_loc (transaction_id, loc_id )
    """
    return (
        "transaction_" + str(randint(0, TRANSACTION_COUNT - 1)),
        "loc_" + str(randint(0, LOCATION_COUNT - 1)),
    )


csv_writer("data/transaction_loc.csv", TRANSACTION_LOC_COUNT, transaction_loc_generator)

# debit_src (acc_id, transaction_id)


def debit_src_generator():
    """
    debit_src (acc_id, transaction_id)
    """
    return (
        "acc_" + str(randint(0, ACCOUNT_COUNT - 1)),
        "transaction_" + str(randint(0, TRANSACTION_COUNT - 1)),
    )


csv_writer("data/debit_src.csv", DEBIT_SRC_COUNT, debit_src_generator)

# debit_dest (transaction_id, acc_id)


def debit_dest_generator():
    """
    debit_dest (transaction_id, acc_id)
    """
    return (
        "transaction_" + str(randint(0, TRANSACTION_COUNT - 1)),
        "acc_" + str(randint(0, ACCOUNT_COUNT - 1)),
    )


csv_writer("data/debit_dest.csv", DEBIT_DEST_COUNT, debit_dest_generator)

# transfer_src (acc_id, transaction_id)


def transfer_src_generator():
    """
    transfer_src (acc_id, transaction_id)
    """
    return (
        "acc_" + str(randint(0, ACCOUNT_COUNT - 1)),
        "transaction_" + str(randint(0, TRANSACTION_COUNT - 1)),
    )


csv_writer("data/transfer_src.csv", TRANSFER_SRC_COUNT, transfer_src_generator)

# transfer_dest (transaction_id, acc_id)


def transfer_dest_generator():
    """
    transfer_dest (transaction_id, acc_id)
    """
    return (
        "transaction_" + str(randint(0, TRANSACTION_COUNT - 1)),
        "acc_" + str(randint(0, ACCOUNT_COUNT - 1)),
    )


csv_writer("data/transfer_dest.csv", TRANSFER_DEST_COUNT, transfer_dest_generator)

# payment_src (acc_id, transaction_id)


def payment_src_generator():
    """
    payment_src (acc_id, transaction_id)
    """
    return (
        "acc_" + str(randint(0, ACCOUNT_COUNT - 1)),
        "transaction_" + str(randint(0, TRANSACTION_COUNT - 1)),
    )


csv_writer("data/payment_src.csv", PAYMENT_SRC_COUNT, payment_src_generator)

# payment_dest (transaction_id, acc_id)


def payment_dest_generator():
    """
    payment_dest (transaction_id, acc_id)
    """
    return (
        "transaction_" + str(randint(0, TRANSACTION_COUNT - 1)),
        "acc_" + str(randint(0, ACCOUNT_COUNT - 1)),
    )


csv_writer("data/payment_dest.csv", PAYMENT_DEST_COUNT, payment_dest_generator)
