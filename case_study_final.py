# -*- coding: utf-8 -*-
"""
Created on Tue Jun 16 20:49:31 2020

@author: nenad
"""
import pandas as pd
import numpy as np
from json import JSONDecoder
from functools import partial
from datetime import datetime

# function parsing data in chunks in JSON multiple objects file
def json_parse(fileobj, decoder=JSONDecoder(), buffersize=2048):
    buffer = ""
    for chunk in iter(partial(fileobj.read, buffersize), ""):
        buffer += chunk
        while buffer:
            try:
                result, index = decoder.raw_decode(buffer.strip(" \n"))
                yield result
                buffer = buffer[index:].lstrip()
            except ValueError:
                break  # it breaks the while loop so we move on the next chunk read from the file. The break is only triggered if there is no JSON object to decode in the current buffer.


# arranging, splitting data in two dataframes
with open("data", "r") as infh:
    dataA = []
    dataB = []
    for data in json_parse(infh):
        if "mandrill_events" in data:
            dataA.append(data)
        else:
            dataB.append(data)


# Data Frame with complete(all) data for productA
dfA = pd.DataFrame(dataA)

# Data Frame with complete(all) data for productB
dfB = pd.DataFrame(dataB)

# ProductA table
# ===================================================================================

# json_normalize for flattening multiple JSON objects for each cell of df
# dfAloc0 = dfA.mandrill_events.loc[0]
# dfAtest0 = pd.json_normalize(dfAloc0)

# dfAloc1 = dfA.mandrill_events.loc[1]
# dfAtest1 = pd.json_normalize(dfAloc1)


# dfAloc2 = dfA.mandrill_events.loc[2]
# dfAtest2 = pd.json_normalize(dfAloc2)

# dfAloc3 = dfA.mandrill_events.loc[3]
# dfAtest3 = pd.json_normalize(dfAloc3)

# finding a shape of dfA
lendfA = dfA.shape[0]
dfAtest = {}

for x in range(lendfA):
    dftest = dfA.mandrill_events.loc[x]
    dfAtest[x] = pd.json_normalize(dftest)

# print(dfAtest)

final_dfA = dfAtest[0].iloc[0:0, :].copy()
# print("df_zero shape: ", df_zero.shape)
# print("df_zero columnsape: ", df_zero.columns)
# print(df_zero.empty)
# final_dfA = x for x in dftest
for x in dfAtest:
    final_dfA = final_dfA.append(dfAtest[x])


# appending results and creating final dataframe for productA
# final_dfA = dfAtest0.append([dfAtest1, dfAtest2, dfAtest3])

# Creating Date & Time Columns
final_dfA["createdatdate"] = [
    datetime.utcfromtimestamp(d).strftime("%Y%m%d") for d in final_dfA["ts"]
]
final_dfA["createdattime"] = [
    datetime.utcfromtimestamp(d).strftime("%H%M%S") for d in final_dfA["ts"]
]

# Renaming Columns
final_dfA = final_dfA.rename(columns={"_id": "id", "msg.email": "email"})

# COMPLITED DataFrame A
productA = final_dfA[["id", "email", "event", "createdatdate", "createdattime"]]

# Droping Duplicate Rows
productA.drop_duplicates(inplace=True)

# Writting DataFrame productA to .CSV file (doublequotes=True by default)
productA.to_csv("productA.csv", index=False)

# ProductB table
# ==============================================================================

# creating df that will contain all needed event data
df_event = pd.DataFrame({"event": []})

df_event["event"] = dfB["eventName"].dropna()
df_event["workflow_id"] = [x["workflowId"] for x in dfB["dataFields"].dropna()]
df_event["id"] = [x["messageId"] for x in dfB["dataFields"].dropna()]
df_event["workflow_name"] = [x["workflowName"] for x in dfB["dataFields"].dropna()]
df_event["campaign_name"] = [x["campaignName"] for x in dfB["dataFields"].dropna()]
df_event["createdAt"] = [x["createdAt"] for x in dfB["dataFields"].dropna()]
df_event["tag"] = [x["labels"] for x in dfB["dataFields"].dropna()]
df_event["email"] = [x for x, y in zip(dfB["email"], dfB["workflowId"]) if np.isnan(y)]

# Creating date and time columns and formatting
date_event = pd.to_datetime(df_event["createdAt"], unit="ns")
date_event1 = date_event.dt.tz_localize(None)
df_event["createdattime"] = date_event1.dt.strftime("%H%M%S")
df_event["createdatdate"] = date_event1.dt.strftime("%Y%m%d")

# create df that will contain test data
df_test = dfB[dfB.test.notnull()].copy()

# Renaming columns in dataframe test
df_test = df_test.rename(columns={"workflowId": "workflow_id", "test": "custom"})

# merging columns
df_merge = df_event.merge(df_test, on=["workflow_id", "email"], how="left")

# Changing values emailOpen -> Open ...
df_merge.event.replace(
    {"emailOpen": "open", "emailClick": "click", "emailUnsubscribe": "unsub"},
    inplace=True,
)

# Replaceing in labels [] with None
x = [y == [] for y in df_merge["tag"]]
df_merge.loc[x, "tag"] = None


# Final Data Frame of ProductB
productB = df_merge[
    [
        "id",
        "email",
        "event",
        "workflow_id",
        "workflow_name",
        "campaign_name",
        "tag",
        "custom",
        "createdatdate",
        "createdattime",
    ]
]

# Droping Duplicate Row
productB.drop_duplicates(inplace=True)

# Writting DataFrame productB to .CSV file (doublequotes=True by default)
productB.to_csv("productB.csv", index=False)

# =================================================================================================
