#!/usr/bin/env python
# coding: utf-8

# # Telephone Directory CRUD OperationCRUD Operation using TelePhone Dirctory

# In[1]:


get_ipython().system('pip install dnspython')
get_ipython().system('pip install pymongo[srv]')


# In[11]:


import pymongo

client = pymongo.MongoClient("mongodb+srv://Venkatesh:Allmightypush@cluster0.ts01o5x.mongodb.net/?retryWrites=true&w=majority")
db = client.Mongodb_Task
record = db.Telephone_Directory

# insert_one


data = { "Name":"ElonMusk", "Phone_number": 9876543210, "Place": "USA" }


x = record.insert_one(data)


# In[ ]:


#Insert_many

directory  = [
    
           {"Name":"Venkatesh", "Phone_number": 8667866339, "Place": "Coimbatore"},
           {"Name":"Nanda", "Phone_number": 8807007907, "Place": "Chennai"},
           {"Name": "Appa", "Phone_number": 9092284378, "Place": "Bangalore"},
           {"Name":"Amma", "Phone_number": 9092375534, "Place": "Mysore"},
           {"Name":"Venkatesh2", "Phone_number": 9514010889, "Place": "Pune"}
    
]


insertion = record.insert_many(directory)

print(insertion.inserted_ids)


# In[25]:


# Retriving the database

for datas in record.find({},{'_id': False}):
    print(datas)


# In[27]:


# Retriving the Specific data

for datas in record.find({'Name': 'Venkatesh' },{'_id': False}):
    print(datas)


# In[44]:


# Retriving using multiple condition

query = {'$or':[{'Place':'Coimbatore'},{'Place':'Bangalore'}]}

for datas in record.find(query,{'_id':False}):
    print(datas)


# In[37]:


# Updating files

# update_one

my_query = {'Name':'Appa','Name': 'Amma'}

new_values = {'$set':{'Name':'Viswanathan','Name': 'Ruckmani'}}

record.update_one(my_query,new_values)

for datas in record.find({},{'_id':False}):
    print(datas)


# In[39]:


#Delete_one

my_query = {'Place': 'USA'}

#record.delete_one(my_query)

for datas in record.find({},{'_id':False}):
    print(datas)


# In[43]:


# Delete_many

my_query = {'Name':{'$regex':'^E'}}
#record.delete_many(my_query)

for datas in record.find({},{'_id': False}):
    print(datas)

