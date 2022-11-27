#!/usr/bin/env python
# coding: utf-8

# # Student Database (MongoDB)

# #### Installing the Pymongo

# In[ ]:


get_ipython().system('pip install dnspython')
get_ipython().system('pip install pymongo[srv]')


# #### Importing the necessary libraries 

# In[1]:


import pymongo
import pandas as pd

# Connecting the Mongodb atlas 
client = pymongo.MongoClient("mongodb+srv://Venkatesh:Allmightypush@cluster0.ts01o5x.mongodb.net/?retryWrites=true&w=majority")

# assigning variable for the database

db = client.Mongodb_Task 

# assigning variable for the Collection

collection = db.Student_Database


# #### Getting the list of database

# In[28]:


client.list_database_names()


# #### Getting the list of collections

# In[27]:


db.list_collection_names()


# #### Adding the Student dataset to the collection

# In[20]:


import json

file = open('students.json', 'r')
            

for i in file:    
    x = json.loads(i)
    collection.insert_one(x)


# #### This is also one of the method to adding the student dataset to the collection

# In[2]:


import pandas as pd 

df = pd.read_json('Students.json', lines = True)
df

data = df.to_dict(orient = 'records')
data


# #### Retriving the all the data's in the collections

# In[29]:


for i in collection.find():
    print(i)


# # Queries need to answer:
# 
# ### 1. Find the student name who scored maximum scores in all (exam, quiz and homework)?
# ### 2. Find students who scored below average in the exam and pass mark is 40%?
# ### 3. Find students who scored below pass mark and assigned them as fail, and above pass mark as pass in all the categories.
# ### 4. Find the total and average of the exam, quiz and homework and store them in a separate collection.
# ### 5. Create a new collection which consists of students who scored below average and above 40% in all the categories.
# ### 6. Create a new collection which consists of students who scored below the fail mark in all the categories.
# ### 7. Create a new collection which consists of students who scored above pass mark in all the categories

# ## 1. Find the student name who scored maximum scores in all (exam, quiz and homework)?

# In[2]:


from pprint import pprint


# In[3]:


stage1 = {'$unwind':'$scores'}

stage2 = {'$group':{'_id': '$scores.type','Maximum_Score':{'$max':'$scores.score'}}}


loop = collection.aggregate([stage1,stage2])

for i in loop:
    x = collection.aggregate([
        {'$unwind':'$scores'}, # decontructing a array field -scores
        {'$match' : {'$and' : [{'scores.type': i['_id']},{'scores.score' : i['Maximum_Score']}]}},
        {'$project': {'_id':0}}])
    print(list(x))


# ## 2. Find students who scored below average in the exam and pass mark is 40%?

# In[4]:


stage1 = {'$unwind':'$scores'}

stage2 = {'$match':{'scores.type':'exam'}}

stage3 = {'$group':{'_id':'$scores.type', 'Average':{'$avg':'$scores.score'}}}

stage4 = {'$project':{'_id':0,'name':1}}

    
exam_average = collection.aggregate([stage1,stage2,stage3])

exam_average = list(exam_average)[0]['Average']
exam_average


# In[5]:


y = collection.aggregate([{'$unwind':'$scores'},
                        {'$match': {'$and': [{'scores.type':'exam'},{'scores.score':{'$gte':40,'$lt': exam_average}}]}},
                        {'$project' : {'name':1,'scores.type':1 }} ])
list(y)


# # 3. Find students who scored below pass mark and assigned them as fail, and above pass mark as pass in all the categories

# ## Student who scored below 40%

# In[100]:


# failed students in exam

stage1 = {'$unwind':'$scores'}

stage2 = {'$match': {'$and': [ {'scores.type':'exam'}, {'scores.score':{'$lt':40} } ] } }

failed_student_exm = collection.aggregate([stage1,stage2])

failed_students_exam = list(failed_student_exm)


for i in failed_students_exam:
    collection.update_one({'_id':i['_id']},
                    {'$push': {'Suject_Results' : {'exam':'Fail'}}})




# In[101]:


# failed students in quiz

stage11 = {'$unwind':'$scores'}

stage22 = {'$match': {'$and': [ {'scores.type':'quiz'}, {'scores.score':{'$lt':40} } ] } }

failed_student_qiz = collection.aggregate([stage11,stage22])

failed_students_quiz = list(failed_student_qiz)



for i in failed_students_quiz:
    collection.update_one({'_id':i['_id']},
                    {'$push': {'Suject_Results' : {'quiz':'Fail'}}})


# In[102]:


# failed students in homework

stage11 = {'$unwind':'$scores'}

stage22 = {'$match': {'$and': [ {'scores.type':'homework'}, {'scores.score':{'$lt':40} } ] } }

failed_student_hmwrk = collection.aggregate([stage11,stage22])

failed_students_home_work = list(failed_student_hmwrk)



for i in failed_students_home_work:
    collection.update_one({'_id':i['_id']},
                    {'$push': {'Suject_Results' : {'homework':'Fail'}}})


# ## students who passed in each category

# In[116]:


# students who passed in exam

stage1 = {'$unwind':'$scores'}

stage2 = {'$match': {'$and': [ {'scores.type':'exam'}, {'scores.score':{'$gt':40} } ] } }

passed_student_exm = collection.aggregate([stage1,stage2])

passed_students_exam = list(passed_student_exm)



for i in passed_students_exam:
    collection.update_one({'_id':i['_id']},
                    {'$push': {'Suject_Results' : {'exam':'pass'}}})


# In[117]:


# students who passed in quiz

stage1 = {'$unwind':'$scores'}

stage2 = {'$match': {'$and': [ {'scores.type':'quiz'}, {'scores.score':{'$gt':40} } ] } }

passed_student_qiz = collection.aggregate([stage1,stage2])

passed_students_quiz = list(passed_student_qiz)




for i in passed_students_quiz:
    collection.update_one({'_id':i['_id']},
                    {'$push': {'Suject_Results' : {'quiz':'Pass'}}})


# In[118]:


# students who passed in homework

stage1 = {'$unwind':'$scores'}

stage2 = {'$match': {'$and': [ {'scores.type':'homework'}, {'scores.score':{'$gt':40} } ] } }

passed_student_hmwrk = collection.aggregate([stage1,stage2])

passed_students_home_work = list(passed_student_hmwrk)

passed_students_home_work


for i in passed_students_home_work:
    collection.update_one({'_id':i['_id']},
                    {'$push': {'Suject_Results' : {'homework':'Pass'}}})


# # 4. Find the total and average of the exam, quiz and homework and store them in a separate collection

# In[18]:



stage1 = {'$unwind': '$scores'}

stage2 = {'$group': {'_id':'$name', 'total_marks': {'$sum': '$scores.score'} ,'avg_of_all' : {'$avg': '$scores.score'}}}

stage3 = {'$sort': {'_id':1} }

#stage3 = {'$project': {'_id':0}}

#stage4 = {'$out': 'Average_of_all_type'}

averafe_of_all_the_type = collection.aggregate([stage1,stage2,stage3])

list(averafe_of_all_the_type)


# In[191]:


db.list_collection_names()


# # 5)      Create a new collection which consists of students who scored below average and above 40% in all the categories.

# In[54]:


stage1 = {'$unwind':'$scores'}

#stage2 = {'$match':{'scores.type':'exam'}}

stage2 = {'$group':{'_id':'$scores.type', 'Average':{'$avg':'$scores.score'}}}

stage4 = {'$project':{'_id':0,'name':1}}
    
exam = collection.aggregate([stage1,stage2])

exam_average = list(exam)
exam_average


# In[55]:


quiz_avg = exam_average[0]['Average']
exam_avg = exam_average[1]['Average']
homework_avg = exam_average[2]['Average']

print(quiz_avg,exam_avg,homework_avg)


# In[112]:



for i in exam_average:  
    collection.aggregate([{'$unwind':'$scores'},
                        {'$match': {'$and': [{'scores.type':i['_id']},{'scores.score':{'$gt':40 ,'$lt': i['Average']}}]}},
                         {'$project':{'_id':0}},
                        {'$out':'Avg_of_all_exam_type'}
                           ])
    
    
#db.list_collection_names()


# ##  6. Create a new collection which consists of students who scored below the fail mark in all the categories

# In[ ]:




