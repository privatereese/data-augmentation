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

    print("Starting to analyze the given Database", name, '.db')

    #Getting Counter to continue increasing the TaskSet_ID for new TaskSets
    db.execute("SELECT count(Set_ID) FROM TaskSet")
    taskset_counter = db.fetchall()[0][0]
    print("Task_Counter = ", taskset_counter)

    #Getting Counter for Jobs to continue increasing the Job_ID for new JobLists
    db.execute("SELECT count(Set_ID) FROM Job")
    job_counter = db.fetchall()[0][0]
    print("Job_Counter = ", job_counter)

    #Getting the set of unsuccessful tasksets with only one single task
    db.execute("SELECT * FROM TaskSet WHERE Successful=0 AND TASK2_ID=-1")
    single_tasks = db.fetchall()

    #Getting the set of *all* tasksets with only two tasks
    db.execute("SELECT * FROM TaskSet WHERE TASK3_ID=-1 AND TASK2_ID!=-1")
    two_tasks = db.fetchall()

    #Getting the count of successfully running tasksets with only two tasks
    db.execute("SELECT count(*) FROM TaskSet WHERE Successful=1 AND TASK2_ID!=-1 AND TASK3_ID!=-1")
    successful_tasks3 = db.fetchall()[0][0]

    #Getting the count of unsuccessful tasksets with only one task
    db.execute("SELECT count(*) FROM TaskSet WHERE Successful=0 AND TASK2_ID!=-1 AND TASK3_ID!=-1")
    unsuccessful_tasks3 = db.fetchall()[0][0]


    '''
    Printing the values we got and calculating the amount of bad_tasksets one needs 
    to generate to get a 50:50 set of successful and unsuccessful tasksets.
    If the Number is negative, there are more unsuccessful datasets than successful ones.
    Please consider using a higher number than suggested, based on the idea of having more tasksets
    rather than having less unsuccessful ones.
    '''
    print(len(two_tasks), "Length of two Tasks")
    print(len(single_tasks), "Length of single Tasks")
    print(successful_tasks3, "Number of successful Tasks in Level 3")
    print(unsuccessful_tasks3, "Number of unsuccessful Tasks in Level 3")
    print("Your added failed 3Task-Tasksets will be", (len(two_tasks) - 1) * (len(single_tasks) - 1))
    print("It is calculated from unsuccessful 1Task-Tasksets times every 2Task-Taskset")
    print("Please give a number between one and ", len(single_tasks) - 1)
    print("Your best guess would be equal to or higher than(please use whole numbers):", (successful_tasks3 - unsuccessful_tasks3) / (len(two_tasks) - 1) + 1)
    premature_break_condition = input("Please select your value and press enter:")
    #Casting the break condition to int, because it cannot be compared to the counter otherwise
    premature_break_condition = int(premature_break_condition)
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

                db2.execute('INSERT INTO TaskSet VALUES (?,?,?,?,?,?)', new_taskset_row)
                db2.executemany('INSERT INTO Job VALUES (?,?,?,?,?,?)', new_job_values)

                taskset_counter += 1

                premature_break_counter += 1
                '''
                Break after *#premature_break_counter* Tasks are added to complete 3 tasksets
                '''
                if premature_break_counter == premature_break_condition:
                    break

            pbar.update(1)


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
