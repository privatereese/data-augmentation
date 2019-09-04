import sqlite3
import sys
from shutil import copyfile


taskset_id = []
taskset_list = []
tasks = []
single_tasks = []
job_ID = 0
task_ID = 0
two_tasks = []
jobs_to_rewrite=[]
taskset_counter = 0
taskset_case = []
job_values = []
jobey_values = []


def read_tasks_from_db():

    db.execute("SELECT count(Set_ID) FROM TaskSet")
    taskset_counter = db.fetchall()[0][0]
    print("Task_ID = ",taskset_counter)
    db.execute("SELECT count(Set_ID) FROM Job")
    job_ID = db.fetchall()[0][0]
    print("Job_ID = ",job_ID)

    db.execute("SELECT * FROM TaskSet WHERE Successful=0 AND TASK2_ID=-1")
    single_tasks = db.fetchall()
    db.execute("SELECT * FROM TaskSet WHERE TASK3_ID=-1 AND TASK2_ID!=-1")
    two_tasks = db.fetchall()
    print(len(two_tasks),"Lenght of two Tasks")
    print(len(single_tasks),"Lenght of single Tasks")
#    sys.exit()
    counterA = 0
    for two_row in two_tasks:
        #Clear taskset_case
        if(counterA%1000 == 0):
            print(counterA/1000," Tausend DONE")
        counterA += 1
        taskset_case = []
        job_values = []
#        print("Reihe aus Zweitertasks",two_row)
        i = 0
        for single_row in single_tasks:
            #get only 49 of the single tasks
            taskset_case.append((str(two_row[0]),str(two_row[2])))
            taskset_case.append((str(two_row[0]),str(two_row[3])))
            taskset_case.append((str(single_row[0]),str(single_row[2])))
#            print(taskset_case)

            jobey_values = []
            for case in taskset_case:
                db.execute('SELECT * FROM Job WHERE Set_ID=? AND TASK_ID=?',case)
                job_values.append(db.fetchall())
#            print(job_values)
#            input()
            for setty in job_values:
#                print("Das ist Setty",setty)
                for settey in setty:
#                    print("Vor Bearbeitung:",settey)
                #Neuen Counter in die Jobstabellen eintragen
                    test = (taskset_counter,) + (settey[1],) + (job_ID,) + settey[3:]
#                    print(test)
                    jobey_values.append(test)
                    job_ID += 1
            
            
#            input()
            #for rauwa in jobey_values:
            #    print("Reihe:",rauwa)


#            print("Einzelreihe:",single_row)
#            print("Zweierreihe:",two_row)
            zweierreihe = (taskset_counter, ) + (0,) + two_row[2:4] + (single_row[2],) + two_row[5:]
#            print("Zweierreihe(veraendert):",zweierreihe)
#            print("Entresultat:",jobey_values)
            db2.execute('INSERT INTO TaskSet VALUES (?,?,?,?,?,?)',zweierreihe)
            db2.executemany('INSERT INTO Job VALUES (?,?,?,?,?,?)', jobey_values)
            taskset_counter += 1
#            print(jobey_values)
#            input()
            i += 1
            if(i==50):
                break



def write_taskset_and_job_to_db():
    global job_ID
    global taskset_counter
    for success, taskset in taskset_list:
        taskset_values = [taskset_counter, 1] if success else [taskset_counter, 0]
        for task in taskset:
            task_id = tasks.index(get_task_hash(task))
            taskset_values.append(task_id)
            for number,job in task['jobs'].items():
                try:
                    job_values = (taskset_counter, task_id, job_ID, job[0], job[1], job[2])
                    db.execute('INSERT INTO Job VALUES (?,?,?,?,?,?)', job_values)
                    job_ID += 1
                except IndexError as e:
                    print('there was an error: ',job,'\n and the task: ',task)
                    
        taskset_values = taskset_values + [-1]*(6-len(taskset_values))
        db.execute('INSERT INTO TaskSet VALUES (?,?,?,?,?,?)', taskset_values)
        taskset_counter += 1



name = sys.argv[1]
database = sqlite3.connect(name)
db = database.cursor()

copyfile(name,name + "_changed")

database2 = sqlite3.connect(name + "_changed")
db2 = database2.cursor()

#print(db.fetchall())

read_tasks_from_db()

database2.commit()
#write_tasks_to_db()

#write_taskset_and_job_to_db()

#database.commit()
database2.close()
db.close()
