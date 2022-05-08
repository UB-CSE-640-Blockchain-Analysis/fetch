# Fetch Transactions

Scripts to fetch *confirmed* Bitcoin transactions and store them in a csv file.

<br />

## Dependencies:
- Python 3.x (used 3.9.7)
- psycopg2 (postgres interface)
- csv
- requests
- datetime
- argparse
- os
- dotenv

<br />

## Setup Steps:

**Step 1**:
- Open Postgres app and create a database server.
- We recommend database on hard disk drive instead of SSD.<br />So that you do not overuse your SSD's read-write cycles.

<img src="https://github.com/UB-CSE-640-Blockchain-Analysis/fetch/blob/assests/step1.png?raw=true" width="512"/>

<img src="https://github.com/UB-CSE-640-Blockchain-Analysis/fetch/blob/assests/step2.png?raw=true" width="512"/>

<br />

**Step 2**:
- Clone repository locally.

**Step 3**:
- Install dependencies using `pip` command.

**Step 3**:
- Run `python utils/setup.py` from root of the repository.
- Enter following neccessary information:
  - *Absolute path* to the destination where transactions will be stored.
  - Name of the Postgres database you want. (default: cse640)
  - Database access URL. (default: 127.0.0.1)
  - Database access port number. (default: 5432)
- This will setup the Postgres server and appropriately create a .env file.

<br />

## Usage:
- Always from root.
- Make sure that the postgres server is running.
- Run `python main.py --help` to get more information about how to execute the script.
- In general, use one of the alternatives from the following command list.
```python
# Fetch 50,000 transactions from previous 1 day with console logs. 
# [slow]
python main.py --log --m_txs=50000

# Fetch 50,000 transactions from previous 1 day with console logs. 
# [faster, by limiting number of inputs and outputs, thus reducing pre-processing]
python main.py --log --l_in_size=500 --l_out_size=1200 --m_txs=50000

# To reset the history table used for pausing and resume fetching feature.
# That is, main.py will starting fetching from first confirmed transactions.
python utils/reset-history.py
```
- List of all command line arguments that you can use:
  - `--log` &emsp;&emsp;(no logs will be printed without this flag)
  - `--m_txs` &emsp;&emsp;(Fetch `m` confirmed transactions.)
  - `--n_days` &emsp;&emsp;(Fetch past `n` days' transactions. Will be overrided by `--m_txs`. (default: 1))
  - `--file_name` &emsp;&emsp;(Name of the output file where transactions will be stored. Omit .csv in terminal. (default: transactions.csv))
  - `--l_in_size` &emsp;&emsp;(Limit size of `SENDERS` handled for faster fetching, but skips some transactions.)
  - `--l_out_size` &emsp;&emsp;(Limit size of `RECEIVERS` handled for faster fetching, but skips some transactions.)