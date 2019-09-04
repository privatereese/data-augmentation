'''
Imports of  Sqlite3 tools
            System tools
            copyfile tools
'''
import sqlite3
import sys
from shutil import copyfile

'''
Parameter declaration
'''
single_tasks = []
two_tasks = []
job_counter = 0
taskset_counter = 0
taskset_case = []
job_values = []
new_job_values = []


def database_operations():

    
    db.execute("SELECT count(Set_ID) FROM TaskSet")
    taskset_counter = db.fetchall()[0][0]
    print("Task_Counter = ",taskset_counter)
    

    db.execute("SELECT count(Set_ID) FROM Job")
    job_counter = db.fetchall()[0][0]
    print("Job_Counter = ",job_ID)
    
    
    db.execute("SELECT * FROM TaskSet WHERE Successful=0 AND TASK2_ID=-1")
    single_tasks = db.fetchall()
    

    db.execute("SELECT * FROM TaskSet WHERE TASK3_ID=-1 AND TASK2_ID!=-1")
    two_tasks = db.fetchall()
    
    
    print(len(two_tasks),"Lenght of two Tasks")
    print(len(single_tasks),"Lenght of single Tasks")

    for two_row in two_tasks:
        
        '''
        Clearing chosen jobs and the jobs table
        as well as the new_jobs table
        '''
        taskset_case = []
        job_values = []
        new_job_values = []
        
        
        premature_break_counter = 0
        
        for single_row in single_tasks:

            #get only 49 of the single tasks
            job_values = []
            taskset_case = []
            taskset_case.append((str(two_row[0]),str(two_row[2])))
            taskset_case.append((str(two_row[0]),str(two_row[3])))
            taskset_case.append((str(single_row[0]),str(single_row[2])))

            for case in taskset_case:
                
                db.execute('SELECT * FROM Job WHERE Set_ID=? AND TASK_ID=?',case)
                job_values.append(db.fetchall())

            for set_of_single_jobs in job_values:
                
                for single_jobs in set_of_single_jobs:
                    
                    new_single_job = (taskset_counter,) + (single_jobs[1],) + (job_counter,) + single_jobs[3:]
                    new_job_values.append(new_single_job)
                    
                    job_counter += 1
            
            
            new_taskset_row = (taskset_counter, ) + (0,) + two_row[2:4] + (single_row[2],) + two_row[5:]

            final_tasksets.append(new_taskset_row)
            final_jobs.append(new_job_values)

            taskset_counter += 1
            
            
            premature_break_counter += 1
            '''
            Break after *#premature_break_counter* Tasks are added to complete 3 tasksets
            '''
            if(premature_break_counter==50):
                break

    for row in final_tasksets:
        db2.execute('INSERT INTO TaskSet VALUES (?,?,?,?,?,?)',row)
    for row in final_jobs:
        db2.executemany('INSERT INTO Job VALUES (?,?,?,?,?,?)', row)


def main():
    '''
    Getting Database from first argument
    '''
    database = sqlite3.connect(name + '.db')
    db = database.cursor()
    
    '''
    Copying Database from old to new with _changed
    '''
    copyfile(name,name + "_changed.db")
    
    '''
    Getting the new database to write to
    '''
    database2 = sqlite3.connect(name + "_changed.db")
    db2 = database2.cursor()
    
    
    database_operations()
    
    
    '''
    Commiting the second and written database
    '''
    database2.commit()
    

    '''
    Closing both databases
    '''
    database2.close()
    db.close()
    
    
    
    
if __name__ == '__main__':
    name = sys.argv[1]
    main()
