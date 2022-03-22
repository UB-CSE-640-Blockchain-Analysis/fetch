import requests
import json
import datetime


global main_hash
main_hash = {}
global second_hash
second_hash = {}


def handle_input(inputs):
    # print("\n\n\n\n")
    # print(inputs)

    # print("\n\n\n\n")

    case_1 = False
    case_2 = False
    case_3 = False

    temp_input_list = []
    for input_i in range(len(inputs)):
        if "addr" in inputs[input_i]["prev_out"]:
            if inputs[input_i]["prev_out"]["addr"] not in temp_input_list:
                temp_input_list.append(inputs[input_i]["prev_out"]["addr"])
            # print(temp_input_list)
            if (case_1 == False and inputs[input_i]["prev_out"]["addr"] in main_hash):
                # print("case 1")
                case_1 = True
                for temp in temp_input_list:
                    if temp not in second_hash and temp != inputs[input_i]["prev_out"]["addr"]:
                        (main_hash[inputs[input_i]["prev_out"]["addr"]]).append(
                            temp)
                        second_hash[temp] = inputs[input_i]["prev_out"]["addr"]
                for input in inputs[input_i+1:]:
                    if input["prev_out"]["addr"] not in second_hash and input["prev_out"]["addr"] != inputs[input_i]["prev_out"]["addr"]:
                        (main_hash[inputs[input_i]["prev_out"]["addr"]]).append(
                            input["prev_out"]["addr"])
                        second_hash[input["prev_out"]["addr"]
                                    ] = inputs[input_i]["prev_out"]["addr"]
                break
            elif (case_1 == False and inputs[input_i]["prev_out"]["addr"] in second_hash):
                case_2 = True
            elif (case_1 == False and case_2 == False):
                case_3 = True

    if (case_1 == False and case_2 == True):
        # print("case 2")
        for temp_index in range(len(temp_input_list)):
            if temp_input_list[temp_index] in second_hash:
                main_table_key = second_hash[temp_input_list[temp_index]]
                for temp in temp_input_list:
                    if temp != temp_input_list[temp_index] and temp not in second_hash:
                        if main_table_key not in main_hash:
                            main_hash[main_table_key] = []
                        (main_hash[main_table_key]).append(temp)
                        second_hash[temp] = temp_input_list[temp_index]
                break
    elif (case_1 == False and case_2 == False and case_3 == True):
        # one will be the main, add them accordinly to both tables
        # print("case 3")
        main_key = temp_input_list[0]
        main_hash[main_key] = []
        for temp_index in range(1, len(temp_input_list)):
            second_hash[temp_input_list[temp_index]] = main_key
            (main_hash[main_key]).append(temp_input_list[temp_index])

    return next(iter(main_hash))


def handle_output(outputs, input):
    final_outputs = []
    for output in outputs:
        if "addr" in output:
            if output["addr"] in second_hash:
                addr = second_hash[output["addr"]]
                if addr != input:
                    if len(final_outputs) > 0:  # TODO verify correctness
                        for el_index in range(len(final_outputs)):
                            if final_outputs[el_index]["addr"] == addr:
                                final_outputs[el_index]["value"] = int(
                                    final_outputs[el_index]["value"]) + int(output["value"])
                                break
                            else:
                                final_outputs.append(
                                    {"value": str(output["value"]), "addr": str(addr)})
                    else:
                        final_outputs.append(
                            {"value": str(output["value"]), "addr": str(addr)})
            else:
                final_outputs.append(
                    {"value": str(int(output["value"])/100000000), "addr": str(output["addr"])})

    return (final_outputs)


pastNDays = 1

today = datetime.datetime.now().date()

sum_hash = 0
sum_transactions = 0
for day in range(pastNDays):
    particular_day = int((datetime.datetime(
        today.year, today.month, today.day).timestamp() - (86400.0 * day)) * 1000)
    url = "https://blockchain.info/blocks/" + \
        str(particular_day) + "?format=json"
    blocks = (requests.get(url)).json()
    sum_hash += len(blocks)
    for block in blocks:
        # print(block["hash"])  # log
        single_block = "https://blockchain.info/rawblock/" + str(block["hash"])
        single_block_res = (requests.get(single_block)).json()
        sum_transactions += (len(single_block_res["tx"]))
        for each_transaction in single_block_res["tx"]:
            print("tx: " + str(each_transaction["hash"]))  # log
            # parse through each transaction as we already have the transaction data fetched
            inputs = []
            outputs = []
            global simplified_inputs
            simplified_inputs = None
            if len(each_transaction["inputs"]) > 0:
                to_insert_input = False
                to_insert_output = False
                for input in each_transaction["inputs"]:
                    if "addr" in input["prev_out"]:
                        to_insert_input = True
                        inputs.append({"value": (
                            input["prev_out"]["value"] / 100000000), "addr": input["prev_out"]["addr"]})
                        # print(each_transaction)
                        # exit()
                        simplified_inputs = handle_input(
                            each_transaction["inputs"])
                        # print(
                        #     f"\n\nmain_hash = {main_hash}\n\nsecond_hash = {second_hash}\n\n")
                        # quit()
                        # print("\n\n\n\n")
                        # print(each_transaction["inputs"])
                        # print("\n\n\n\n")

                if simplified_inputs != None:
                    to_insert_output = True
                    outputs = handle_output(
                        each_transaction["out"], simplified_inputs)

                # for out in each_transaction["out"]:
                #     if "addr" in out:
                #         to_insert_output = True
                #         outputs.append(
                #             {"value": (out["value"] / 100000000), "addr": out["addr"]})
                if to_insert_input and to_insert_output:
                    for output in outputs:
                        dict = {"hash": each_transaction["hash"], "time": each_transaction["time"],
                                "sender": simplified_inputs, "receiver": output["addr"], "value": output["value"]}

                    # dict = {"hash": each_transaction["hash"],"time": each_transaction["time"], "sender":inputs, "receivers":outputs}
                    # if each_transaction["hash"] == "293a5e6c0eea9ed493c5982f638b15b9d669d1c5304424024f7c1a04f605b429":

                        print(json.dumps(dict))
                        print("\n--------\n")
                    # TODO append to the main dataset

print(sum_hash)

# print(datetime.datetime(2022, 3, 1).timestamp())

# url = "https://blockchain.info/blocks/1646110800000?format=json"
# res = (requests.get(url)).json()
# print(len(res))

# print(res)
"""
time
sender
receiver
amount
"""