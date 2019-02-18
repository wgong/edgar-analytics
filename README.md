# Table of Contents
1. [Introduction](README.md#introduction)
2. [Implementation details](README.md#implementation-details)
3. [How to run](README.md#how-to-run)
4. [Test case](README.md#test-case)

# Introduction

Many investors, researchers, journalists and others use the Securities and Exchange Commission's Electronic Data Gathering, Analysis and Retrieval (EDGAR) system to retrieve financial documents, whether they are doing a deep dive into a particular company's financials or learning new information that a company has revealed through their filings. 

The SEC maintains EDGAR weblogs showing which IP addresses have accessed which documents for what company, and at what day and time this occurred.

Imagine the SEC has asked you to take the data and produce a dashboard that would provide a real-time view into how users are accessing EDGAR, including how long they stay and the number of documents they access during the visit.

While the SEC usually makes its EDGAR weblogs publicly available after a six month delay, imagine that for this challenge, the government entity has promised it would stream the data into your program in real-time and with no delay.

Your job as a data engineer is to build a pipeline to ingest that stream of data and calculate how long a particular user spends on EDGAR during a visit and how many documents that user requests during the session. 

# Implementation details

Data processing is implemented in python function `process_data` of `src/sessionization.py`. 

The basic data structure is a dictionary which stores requests of an active sessions:
* key: ip address
* value: list of requests (timestamp in sec, date, time). 

```
# logic to process each request
loop over dict.keys
  if ip eq new_ip
    if session expired
      write out last session
      reset dict to new_ip
    else
      append request info (ts, date, time)
  else
    if session expired
      write out last session
      remove from dict    
```

# How to run
see `run.sh` shell script, e.g.

```
$ python ./src/sessionization.py -i ./input/log.csv -p ./input/inactivity_period.txt -o ./output/sessionization.txt
```

# Test case
We downloaded a log file from https://www.sec.gov/dera/data/edgar-log-file-data-set.html and took the first 10k lines and created a new test case found at `insight_testsuite/tests/test_2`.

run this test case as follows:
```
$ cd insight_testsuite
$ ./run_tests.sh
Processed 11 lines in 0.003 sec
[PASS]: test_1 sessionization.txt
Processed 10000 lines in 0.458 sec
[PASS]: test_2 sessionization.txt
[Wed Feb 13 20:16:35 EST 2019] 2 of 2 tests passed

```

# Credit

this repo is based on https://github.com/InsightDataScience/edgar-analytics.git
