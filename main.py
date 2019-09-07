'''
Imports of  Sqlite3 tools
            System tools
            copyfile tools
'''
import sqlite3
import sys
from shutil import copyfile
from tqdm import tqdm

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
final_tasksets = []
final_jobs = []


def database_operations():
    db.execute("SELECT count(Set_ID) FROM TaskSet")
    taskset_counter = db.fetchall()[0][0]
    print("Task_Counter = ", taskset_counter)

    db.execute("SELECT count(Set_ID) FROM Job")
    job_counter = db.fetchall()[0][0]
    print("Job_Counter = ", job_counter)

    db.execute("SELECT * FROM TaskSet WHERE Successful=0 AND TASK2_ID=-1")
    single_tasks = db.fetchall()

    db.execute("SELECT count(*) FROM TaskSet WHERE Successful=1 AND TASK2_ID!=-1 AND TASK3_ID!=-1")
    successful_tasks3 = db.fetchall()[0][0]
    db.execute("SELECT count(*) FROM TaskSet WHERE Successful=0 AND TASK2_ID!=-1 AND TASK3_ID!=-1")
    unsuccessful_tasks3 = db.fetchall()[0][0]

    db.execute("SELECT * FROM TaskSet WHERE TASK3_ID=-1 AND TASK2_ID!=-1")
    two_tasks = db.fetchall()

    print(len(two_tasks), "Length of two Tasks")
    print(len(single_tasks), "Length of single Tasks")
    print(successful_tasks3, "Number of successful Tasks in Level 3")
    print(unsuccessful_tasks3, "Number of unsuccessful Tasks in Level 3")
    print("Your added failed 3Task-Tasksets will be", (len(two_tasks) - 1) * (len(single_tasks) - 1))
    print("It is calculated from unsuccessful 1Task-Tasksets times every 2Task-Taskset")
    print("Please give a number between one and ", len(single_tasks) - 1)
    print("Your best guess would be:", (successful_tasks3 - unsuccessful_tasks3) / (len(two_tasks) - 1))
    premature_break_condition = input("Please select your value and press enter:")
    print("Chosen value:", premature_break_condition)

    with tqdm(total=len(two_tasks + single_tasks)) as pbar:
        for two_row in two_tasks:

            premature_break_counter = 0

            for single_row in single_tasks:

                '''
                Clearing chosen jobs and the jobs table
                as well as the new_jobs table
                '''
                taskset_case = []
                job_values = []
                new_job_values = []
                # get only 49 of the single tasks
                taskset_case.append((str(two_row[0]), str(two_row[2])))
                taskset_case.append((str(two_row[0]), str(two_row[3])))
                taskset_case.append((str(single_row[0]), str(single_row[2])))

                for case in taskset_case:
                    db.execute('SELECT * FROM Job WHERE Set_ID=? AND TASK_ID=?', case)
                    job_values.append(db.fetchall())

                for set_of_single_jobs in job_values:

                    for single_jobs in set_of_single_jobs:
                        new_single_job = (taskset_counter, single_jobs[1], job_counter, single_jobs[3], single_jobs[4], single_jobs[5])
                        new_job_values.append(new_single_job)

                        job_counter += 1

                new_taskset_row = (taskset_counter, 0, two_row[2], two_row[3], single_row[2], two_row[5])

                final_tasksets.append(new_taskset_row)
                final_jobs.append(new_job_values)

                taskset_counter += 1

                premature_break_counter += 1
                '''
                Break after *#premature_break_counter* Tasks are added to complete 3 tasksets
                '''
                if premature_break_counter == premature_break_condition:
                    break

            pbar.update(1)

    for row in final_tasksets:
        db2.execute('INSERT INTO TaskSet VALUES (?,?,?,?,?,?)', row)
    for row in final_jobs:
        db2.executemany('INSERT INTO Job VALUES (?,?,?,?,?,?)', row)


def main():
    global db
    global db2
    '''
    Getting Database from first argument
    '''
    database = sqlite3.connect(name + '.db')
    db = database.cursor()

    '''
    Copying Database from old to new with _changed
    '''
    copyfile(name + '.db', name + '_changed.db')

    '''
    Getting the new database to write to
    '''
    database2 = sqlite3.connect(name + '_changed.db')
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
    print("Database written to:", name, "_changed.db")
    print("Database:", name, "db not changed, will be closed again")
    db.close()


if __name__ == '__main__':
    global name
    name = sys.argv[1]
    main()
