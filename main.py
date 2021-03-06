import requests
import datetime
import psycopg2
import csv
import argparse
import os
from dotenv import load_dotenv, find_dotenv

parser = argparse.ArgumentParser(description="Fetch Bitcoin Transactions")
parser.add_argument('--log', default=False, action=argparse.BooleanOptionalAction, help="Print logs? If this flag is not used, no info. about txs will be printed.")
parser.add_argument("--m_txs", dest="m_txs", type=int, nargs=1, help="Fetch M transactions.")
parser.add_argument('--n_days', default=1, help="Fetch past N days' transactions. Will be overrided by --m_txs. (default: 1)")
parser.add_argument("--l_in_size", dest="l_in_size", type=int, nargs=1, help="Limit size of SENDERS handled for faster fetching, but skipping some txs.")
parser.add_argument("--l_out_size", dest="l_out_size", type=int, nargs=1, help="Limit size of RECEIVERS handled for faster fetching, but skipping some txs.")
parser.add_argument("--file_name", dest="file_name", type=str, nargs=1, help="Name of the output file. Omit .csv in terminal. (default: transactions.csv)")
args = parser.parse_args()

print(f"log = {args.log}")
if args.m_txs:
    print(f"m_txs = {args.m_txs[0]}")
print(f"n_days = {args.n_days}")
if args.l_in_size:
    print(f"l_in_size = {args.l_in_size[0]}")
if args.l_out_size:
    print(f"l_out_size = {args.l_out_size[0]}")
if args.file_name:
    print(f"file_name = {args.file_name[0]}")

"""
    loading environment variables
"""
load_dotenv(find_dotenv())
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DIR_PATH = os.getenv("DIR_PATH")
if (None in [DB_NAME, DB_HOST, DB_PORT, DIR_PATH]):
    print("!! environment not setup properly !!\n!! please check it manually !!\n!! then retry !!")
    quit()

"""
    database connection
"""
CONN = psycopg2.connect(database=DB_NAME,
                        host=DB_HOST, port=DB_PORT)
print("Opened database successfully")
CURSOR = CONN.cursor()

def shutdown_gracefully(store_config, particular_day=None, block_index=None, transaction_index=None, fetch_start_day=None):
    if store_config:
        CURSOR.execute(f"select particular_day_seconds from history limit 1;")
        stored_day = CURSOR.fetchone()[0]
        if fetch_start_day == "NULL":
            CURSOR.execute(f"UPDATE history SET (particular_day_seconds, block_index, transaction_index, starting_date) = ({particular_day}, {block_index}, {transaction_index}, {fetch_start_day}) where particular_day_seconds = {stored_day};")
        else:
            CURSOR.execute(f"UPDATE history SET (particular_day_seconds, block_index, transaction_index, starting_date) = ({particular_day}, {block_index}, {transaction_index}, \'{fetch_start_day}\') where particular_day_seconds = {stored_day};")
    CURSOR.close()
    CONN.commit()
    CONN.close()

"""
    DATABASE Functions
"""
try:
    def existsMAIN_HASH(s_main_address, table_name = "main_hash"):
        CURSOR.execute(f"select exists(select 1 from {table_name} where main_address=\'{s_main_address}\');")
        result = CURSOR.fetchone()[0]
        return result

    def appendMAIN_HASH(main_address, secondary_address_to_append, table_name = "main_hash"):
        CURSOR.execute("UPDATE "+table_name+" SET secondary_addresses = array_append(secondary_addresses,'" + secondary_address_to_append + "') WHERE main_address = '" + main_address + "';")
        CONN.commit()
        

    def insertMAIN_HASH(main_address, secondary_address, table_name = "main_hash"):
        CURSOR.execute("""INSERT INTO main_hash VALUES (%s, ARRAY[%s]);""", (main_address, secondary_address))
        CONN.commit()

        
    def existsSECONDARY_HASH(s_secondary_address, table_name = "secondary_hash"):
        CURSOR.execute(f"select exists(select 1 from {table_name} where secondary_address=\'{s_secondary_address}\');")
        result = CURSOR.fetchone()[0]
        return result

    def selectSECONDARY_HASH(secondary_address, table_name = "secondary_hash"):
        CURSOR.execute("SELECT main_address FROM " + table_name + "  WHERE secondary_address = '" + secondary_address + "';")
        result = CURSOR.fetchone()[0]
        return result


    def insertSECONDARY_HASH(secondary_address, main_address, table_name = "secondary_hash"):
        CURSOR.execute("INSERT INTO " + table_name + " VALUES (\'" + secondary_address + "\', \'" + main_address + "\');")
        CONN.commit()

        

    global st_day
    st_day = 0
    global st_block
    st_block = 0
    global st_transaction
    st_transaction = 0
    global st_fetching_started_day
    st_fetching_started_day = "NULL"
    


    def handle_input(inputs):

        case_1 = False
        case_2 = False
        case_3 = False

        temp_input_list = []
        for input_i in range(len(inputs)):
            if "addr" in inputs[input_i]["prev_out"]:
                if inputs[input_i]["prev_out"]["addr"] not in temp_input_list:
                    temp_input_list.append(inputs[input_i]["prev_out"]["addr"]) 
                # print(temp_input_list)
                if (case_1 == False and existsMAIN_HASH(inputs[input_i]["prev_out"]["addr"])):
                    # print("case 1")
                    case_1 = True
                    for temp in temp_input_list:
                        if not existsSECONDARY_HASH(temp) and temp != inputs[input_i]["prev_out"]["addr"]:
                            appendMAIN_HASH(inputs[input_i]["prev_out"]["addr"], temp)
                            insertSECONDARY_HASH(temp, inputs[input_i]["prev_out"]["addr"])
                    for input in inputs[input_i+1:]:
                        if not existsSECONDARY_HASH(input["prev_out"]["addr"]) and input["prev_out"]["addr"] != inputs[input_i]["prev_out"]["addr"]: 
                            appendMAIN_HASH(inputs[input_i]["prev_out"]["addr"], input["prev_out"]["addr"])
                            insertSECONDARY_HASH(input["prev_out"]["addr"], inputs[input_i]["prev_out"]["addr"])
                    # break
                    r = inputs[input_i]["prev_out"]["addr"]
                    # print(f"returning {r}")
                    return r
                elif (case_1 == False and existsSECONDARY_HASH(inputs[input_i]["prev_out"]["addr"])):
                    case_2 = True
                elif (case_1 == False and case_2 == False):
                    case_3 = True

        if (case_1 == False and case_2 == True):
            # print("case 2")
            for temp_index in range(len(temp_input_list)):
                if existsSECONDARY_HASH(temp_input_list[temp_index]):
                    main_table_key = selectSECONDARY_HASH(temp_input_list[temp_index]) ## select main_hash address from second_hash where secondary_address = temp_input_list[temp_index]
                    for temp in temp_input_list:
                        if temp != temp_input_list[temp_index] and not existsSECONDARY_HASH(temp):
                            if not existsMAIN_HASH(main_table_key):
                                insertMAIN_HASH(main_table_key, None)
                            appendMAIN_HASH(main_table_key, temp)
                            insertSECONDARY_HASH(temp, temp_input_list[temp_index])
                    break
            # print(f"returning {second_hash[temp_input_list[0]]}")
            return selectSECONDARY_HASH(temp_input_list[0])
        elif (case_1 == False and case_2 == False and case_3 == True):
            # one will be the main, add them accordinly to both tables
            # print("case 3")
            main_key = temp_input_list[0]
            insertMAIN_HASH(main_key, None) # insert new row in main_hash
            # print(f"len = {len(temp_input_list)}")
            for temp_index in range(1, len(temp_input_list)):
                # print(f"3>> {temp_index}")
                insertSECONDARY_HASH(temp_input_list[temp_index], main_key)
                appendMAIN_HASH(main_key, temp_input_list[temp_index])
            # print(f"returning {main_key}")
            return main_key

        # r = next(iter(main_hash))
        # print(f"returning {r}")
        # return r


    def handle_output(outputs, input, each_tx_hash):
        final_outputs = []
        # print(f"length of outputs = {len(outputs)}")
        limit_output = False
        if args.l_out_size:
            limit_output = len(outputs) > args.l_out_size[0]
        if limit_output:
            return None

        for output in outputs:
            # compare output with input
            # compare secondary_hash's main_address with input
            # if not equal > then 
            # check if its in final_output already
                # > true? -> add value
                # > else -> append new value
            if "addr" not in output:
                continue
                
            if output["addr"] == input:
                continue
            

            if existsSECONDARY_HASH(output["addr"]): 
                addr = selectSECONDARY_HASH(output["addr"])
                if addr == input:
                    continue
            
                not_in = True
                found_index = 0
                for f_index in range(len(final_outputs)):
                    if final_outputs[f_index]["addr"] == addr:
                        not_in = False
                        found_index = f_index
                        break
                if not_in:
                    # append
                    final_outputs.append({"value": str(output["value"]/100000000), "addr": str(addr)})
                else:
                    final_outputs[found_index]["value"] = str(float(final_outputs[found_index]["value"]) + output["value"]/100000000)
            else:
                addr = output["addr"]
                if addr == input:
                    continue

                if not existsMAIN_HASH(addr):
                    insertMAIN_HASH(addr, None) # insert new row in main_hash
                    
                not_in = True
                found_index = 0
                for f_index in range(len(final_outputs)):
                    if final_outputs[f_index]["addr"] == addr:
                        not_in = False
                        found_index = f_index
                        break
                if not_in:
                    #append
                    final_outputs.append({"value": str(output["value"]/100000000), "addr": str(addr)})
                else:
                    final_outputs[found_index]["value"] = str(float(final_outputs[found_index]["value"]) + output["value"]/100000000)

        return (final_outputs)


    def fetch_config():
        CURSOR.execute(f"select * from history limit 1;")
        result = CURSOR.fetchone()
        return result

    pastNDays = args.n_days

    today = datetime.datetime.now().date()

    sum_hash = 0
    sum_transactions = 0

    i_counter = 0
    csv_counter = 0

    starting_day, starting_block, starting_transaction, fetching_started_on_day = fetch_config()
    st_fetching_started_day = fetching_started_on_day
    if fetching_started_on_day != None:
        diff = today - fetching_started_on_day
        rvalue_day_inc = diff.days
    else:
        rvalue_day_inc = 0
    print(f"starting_day = {starting_day}")
    print(f"starting_block = {starting_block}")
    print(f"starting_transaction = {starting_transaction}")    
    print(f"fetching_started_on_day = {fetching_started_on_day}")

    print("fetchin data")
    for day in range(starting_day + rvalue_day_inc, pastNDays + rvalue_day_inc):
        st_day = day - rvalue_day_inc
        particular_day = int((datetime.datetime(
            today.year, today.month, today.day).timestamp() - (86400.0 * day)) * 1000)
        url = "https://blockchain.info/blocks/" + \
            str(particular_day) + "?format=json"
        blocks = (requests.get(url)).json()
        sum_hash += len(blocks)

        for block_i in range(starting_block, len(blocks)):
            st_block = block_i
            block = blocks[block_i]
            b_log = block["hash"]
            if args.log: print(f"block =  {b_log}")  # log
            single_block = "https://blockchain.info/rawblock/" + str(block["hash"])
            single_block_res = (requests.get(single_block)).json()
            sum_transactions += (len(single_block_res["tx"]))
            
            for each_transaction_i in range(starting_transaction + 1, len(single_block_res["tx"])):
                st_transaction = each_transaction_i
                each_transaction = single_block_res["tx"][each_transaction_i]
                if args.log: print("tx: " + str(each_transaction["hash"]))  # log
                # parse through each transaction as we already have the transaction data fetched
                
                inputs = []
                outputs = []
                global simplified_inputs
                simplified_inputs = None
                limit_input = True
                if args.l_in_size:
                    limit_input = (len(each_transaction["inputs"]) <= args.l_in_size[0])
                if len(each_transaction["inputs"]) > 0 and limit_input:
                    to_insert_input = False
                    to_insert_output = False
                    for input in each_transaction["inputs"]:
                        if "addr" in input["prev_out"]:
                            to_insert_input = True
                            inputs.append({"value": (
                                input["prev_out"]["value"] / 100000000), "addr": input["prev_out"]["addr"]})
                            simplified_inputs = handle_input(
                                each_transaction["inputs"])

                    if simplified_inputs != None:
                        to_insert_output = True
                        # print(each_transaction["hash"])
                        outputs = handle_output(each_transaction["out"], simplified_inputs, each_transaction["hash"])
                        length_of_each_transaction_output = len(each_transaction["out"])
                        # print(f"len(actual_transaction > output) = {length_of_each_transaction_output})  VS  len(final_output) = {len(outputs)}")
                        if outputs != None:
                            if length_of_each_transaction_output < len(outputs):
                                print("\n\nNOTICE THIS\n=========\n")
                                print(f"len(actual_transaction > output) = {length_of_each_transaction_output})  VS  len(final_output) = {len(outputs)}")

                   
                    if to_insert_input and to_insert_output and outputs != None:
                        # dict = {"hash": each_transaction["hash"], "time": each_transaction["time"], "sender": simplified_inputs, "receiver": output["addr"], "value": output["value"]}
                        # dict = {"hash": each_transaction["hash"],"time": each_transaction["time"], "sender":inputs, "receivers":outputs}
                        if args.file_name:
                            file_name = args.file_name[0]
                        else:
                            file_name = "transactions"
                        for output in outputs:
                            if simplified_inputs != output["addr"]:
                                with open(DIR_PATH + file_name + ".csv", "a") as f:
                                    writer = csv.writer(f)
                                    writer.writerow([each_transaction["hash"], each_transaction["time"],
                                                    simplified_inputs, output["addr"], output["value"]])
                                f.close()
                                csv_counter += 1 
                        i_counter += 1
                        if args.log: print(f"# {i_counter}")
                        if args.m_txs:
                            if csv_counter >= args.m_txs[0]:
                                # closing steps
                                print(str(args.m_txs[0]) + " data fetched")
                                if st_fetching_started_day == None:
                                    st_fetching_started_day = today = datetime.datetime.now().date()
                                shutdown_gracefully(True, st_day, st_block, st_transaction, st_fetching_started_day)
                                print("done storing data")
                                print(sum_hash)
                                quit()
                            
                
            
        print(f"\n================\nDay {day} Complete!\n================\n")


    print("done fetching data")
    shutdown_gracefully(True, 0, 0, 0, "NULL")
    print("done storing data")
    print(sum_hash)
except SystemExit:
    print()
except KeyboardInterrupt:
    print("\n=================\nEXCEPTION CAUGHT! " )

    print(f"EXCEPT @ starting_day = {st_day}")
    print(f"EXCEPT @ starting_block = {st_block}")
    print(f"EXCEPT @ starting_transaction = {st_transaction}")

    if st_day != 0 or st_block != 0 or st_transaction != 0:
        if st_fetching_started_day == None:
            st_fetching_started_day = today = datetime.datetime.now().date()
        shutdown_gracefully(True, st_day, st_block, st_transaction, st_fetching_started_day)




"""
time
sender
receiver
amount
"""
