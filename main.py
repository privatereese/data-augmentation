import sqlite3
import sys

taskset_id = []
taskset_list = []
tasks = []
single_tasks = []
job_ID = 0




def read_tasks_from_db():

    db.execute("SELECT * FROM TaskSet WHERE Successful=0 AND TASK2_ID=-1")
    rows = db.fetchall()
    for row in rows:
        print(row)
    db.execute("SELECT * FROM TaskSet WHERE TASK3_ID=-1 AND TASK2_ID!=-1")
    rows2 = db.fetchall()
    for row1 in rows2:
        print(row1)


PKGTOINT = {'hey' : 1,
            'pi' : 2,
            'tumatmul' : 3,
            'cond_mod' : 4#,
            #'cond_42' : 5
            }

def get_task_hash(task):
    # returns a string containing 52 digits per task
    #print(task)
    hash_value = ''
    hash_value += str(PKGTOINT[task['pkg']])
    hash_value += str(task['priority']).zfill(3) #fine
    hash_value += str(task['deadline']).zfill(5)
    hash_value += str(task['period']).zfill(5)
    hash_value += str(task['criticaltime']).zfill(5)
    hash_value += str(task['numberofjobs']).zfill(3)
    hash_value += str(task['offset']).zfill(5)
    hash_value += task['quota'][:-1].zfill(3)
    hash_value += str(task['caps']).zfill(3)
    hash_value += str(task['cores']).zfill(2)
    hash_value += str(task['coreoffset']).zfill(2)
    hash_value += str(task['config']['arg1']).zfill(15)# todo, can be much bigger
    
    return hash_value


def write_taskset_and_job_to_db():
    global job_ID
    taskset_counter = 0
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


read_tasks_from_db()

#write_tasks_to_db()

#write_taskset_and_job_to_db()

#database.commit()
db.close()

