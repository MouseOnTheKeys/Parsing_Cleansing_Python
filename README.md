# Parsing_Cleansing_Python

ETL preparation 
Data parsing and clensing with Python

# Data parsing and cleansing (python)

**Our task is:** ​ to map Product “A” and Product “B” events from this sample file to target csv
files.
Please download the files needed to solve the task from here: ​data​, ​productA​ and ​productB​.
The data is a sample file in which we receive the records of both Product “A” and “B”.
Luckily, we are given example json records on how both Product “A” (productA) and Product
“B” (productB) look like.
Have a look on these two files first to have an impression of what schemas we are dealing
with. ​ **Hint** ​: we can use ​jsonlint.com​ an online tool that helps us formatting json records in
human readable format easily. We would like to deliver the following content in csv files:
</br>
product “A”: ​"id","email","event","createdatdate","createdattime"
Product “B”: ​"id","email","event","workflow_id","workflow_name","campaign_name","tag",
"custom","createdatdate","createdattime"
</br>
using the mapping:
</br>
Product “A”:
**Desired output | Input**
id | \_id
email | msg->email
event | has the value either open, click, or unsub.
createdatdate | ts: date, without separators i.e. 2020-06-07 -> 20200607
createdattime | ts: time, without separators i.e.: 15:06:16 -> 150616

Product “B”:
**Desired output | Input**
id | dataFields.messageId
email | email
event | eventName. has the value: emailOpen -> open, emailClick -> click, emailUnsubscribe -> unsub.
workflow_id | dataFields.workflowId. could be NULL, or an id if the event is attached to a workflow
createdattime | ts: time, without separators i.e.: 15:06:16 -> 150616
workflow_name | dataFields.workflowName. could be NULL, or an id if the event is attached to a workflow
campaign_name | dataFields.campaignName.
tag | label. if [] -> NULL,otherwise label
custom | test -> custom
createdatdate | ts: date, withoutseparators i.e. 2020-06--> 20200607
createdattime | ts: time, without separators i.e.: 15:06: -> 150616

**Note:** ​For Product “B” we have two examples. The first line shows an example when an
event was not attached to a workflow (​workflow_id​ ​and​ ​workflow_name​ ​are null). The
second line displays an example when an event was attached to a workflow (​workflow_id
and ​workflow_name ​are not null). In this case ​ **optionally** ​ we might have also received an
extra json object​ with a new field of ​test​ - in this case we would like to link the extra object
to this event object based on ​workflow_id ​and​ email​.
we would like to link on ​workflowId ​and​ email​:
{"test":"value1",​ **"workflowId":71370** ​,​ **"email":"a1@b.com"** ​}{​ **"email":"a1@b.com"** ​,"dataFields":{..,​ **"workflowId":71370** ​,..}
=> and we would like to generate the end result csv as discussed before (​test​ -> ​custom​):
"id","email","event","workflow_id","workflow_name","campaign_name","tag",
"custom","createdatdate","createdattime"

**Help:** ​ we don’t know how the records are being stored in data, it could be we have multiple
json records within a line.
**Help2** ​: choosing a pattern to identify Product “A” events, "mandrill_events" could be a good
idea. Respectively, for Product “B” "dataFields" could be a good choice, but we are free to
choose different ones.
**Help3** ​: we might have multiple event objects arriving for one ​extra json object​, therefore it
seems to make sense to represent the extra objects only once.
**Help4** ​: it makes sense to get rid of the duplicate records before saving any of the output files
**Environment** ​: python3 and pandas for data transformations (mandatory), on top of this
additional libraries are free to use
**Deliverables** ​: the python source code: the application needs to read in the file data, and
needs to generate the output files productA.csv and productB.csv with quotes enabled.
Please also include a requirements.txt plain/text file containing the used python libraries with
their version numbers.

# Data lookup (linux)

```
A) Can you please tell us, how many times the ​id​ ​ be4e071c11594bb0b4ee3c444fd08b
for Product “B” occurs in the input file and in your output file? What is your observation? How
can you explain?
B) Can you show us the distinct number of occurrences of the events we receive in the
output of Product “A” with a solution? What is your observation on the output of this solution?
How can you explain?
Please use a shell script, or, more preferably tool(s) from GNU core utils to perform this task.
Environment ​: shell, or GNU core utils (simplicity preferred)
```

**Deliverables** ​: code for the chosen solution that provides us the expected output, and the
reasoning of what we observed in a README plain/text file.
