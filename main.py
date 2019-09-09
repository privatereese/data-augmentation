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
functionname = None
databasename = None

def data_augmentation(db_input,db_output):

    print("Starting to analyze the given Database", databasename, '.db')

    #Getting Counter to continue increasing the TaskSet_ID for new TaskSets
    db_input.execute("SELECT count(Set_ID) FROM TaskSet")
    taskset_counter = db_input.fetchall()[0][0]
    print("Task_Counter = ", taskset_counter)

    #Getting Counter for Jobs to continue increasing the Job_ID for new JobLists
    db_input.execute("SELECT count(Set_ID) FROM Job")
    job_counter = db_input.fetchall()[0][0]
    print("Job_Counter = ", job_counter)

    #Getting the set of unsuccessful tasksets with only one single task
    db_input.execute("SELECT * FROM TaskSet WHERE Successful=0 AND TASK2_ID=-1")
    single_tasks = db_input.fetchall()

    #Getting the set of *all* tasksets with only two tasks
    db_input.execute("SELECT * FROM TaskSet WHERE TASK3_ID=-1 AND TASK2_ID!=-1")
    two_tasks = db_input.fetchall()

    #Getting the count of successfully running tasksets with only two tasks
    db_input.execute("SELECT count(*) FROM TaskSet WHERE Successful=1 AND TASK2_ID!=-1 AND TASK3_ID!=-1")
    successful_tasks3 = db_input.fetchall()[0][0]

    #Getting the count of unsuccessful tasksets with only one task
    db_input.execute("SELECT count(*) FROM TaskSet WHERE Successful=0 AND TASK2_ID!=-1 AND TASK3_ID!=-1")
    unsuccessful_tasks3 = db_input.fetchall()[0][0]


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
                    db_input.execute('SELECT * FROM Job WHERE Set_ID=? AND TASK_ID=?', case)
                    job_values.append(db_input.fetchall())

                for set_of_single_jobs in job_values:

                    for single_jobs in set_of_single_jobs:
                        new_single_job = (taskset_counter, single_jobs[1], job_counter, single_jobs[3], single_jobs[4], single_jobs[5])
                        new_job_values.append(new_single_job)

                        job_counter += 1

                new_taskset_row = (taskset_counter, 0, two_row[2], two_row[3], single_row[2], two_row[5])

                db_output.execute('INSERT INTO TaskSet VALUES (?,?,?,?,?,?)', new_taskset_row)
                db_output.executemany('INSERT INTO Job VALUES (?,?,?,?,?,?)', new_job_values)

                taskset_counter += 1

                premature_break_counter += 1
                '''
                Break after *#premature_break_counter* Tasks are added to complete 3 tasksets
                '''
                if premature_break_counter == premature_break_condition:
                    break

            pbar.update(1)

def runtimemath(db_input,db_output):



    tasks = generate_dict(db_input)

    db_input.execute("SELECT * FROM Task;")
    task_table = db_input.fetchall()

    create_new_table(db_output)

    new_task_table = []

    for row in task_table:
        new_task_table.append(row + tuple(tasks[row[0]]))

    db_output.executemany('INSERT INTO Task VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', new_task_table)


def create_new_table(database):


    database.execute("DROP TABLE Task")
    database.execute(
        '''
        CREATE TABLE IF NOT EXISTS Task
        (Task_ID INTEGER, 
         Priority INT, 
         Deadline INT, 
         Quota INT,
         CAPS INT,
         PKG STRING, 
         Arg INT, 
         CORES INT,
         COREOFFSET INT,
         CRITICALTIME INT,
         Period INT, 
         Number_of_Jobs INT,
         OFFSET INT,
         MIN_RUNTIME INT,
         MAX_RUNTIME INT,
         AVG_RUNTIME INT,
         PRIMARY KEY (Task_ID)
         )
         '''
    )

def generate_dict(cursor):

    tasks = {}
    for i in range(0,401):
        tasks[i] = [-1,-1,-1]
    for index,command_option in enumerate(("min","max","avg"),0):
        sqlstring = "SELECT Task_ID, CASE when End_Date < Start_Date Then {command}(((4294967 - Start_Date) + End_Date)) when End_Date > Start_Date then {command}((End_Date - Start_Date)) END Runtimes FROM Job WHERE Exit_Value='EXIT' GROUP BY Task_ID;".format(command=command_option)
        cursor.execute(sqlstring)
        dataList = cursor.fetchall()
        for task in dataList:
            tasks[task[0]][index]=task[1]

    return tasks

def main():

    '''
    Getting Database from first argument
    '''
    database = sqlite3.connect(databasename + '.db')
    db = database.cursor()

    '''
            Copying Database from old to new with _changed
            '''
    copyfile(databasename + '.db', databasename + functionname + '.db')

    '''
    Getting the new database to write to
    '''
    database2 = sqlite3.connect(databasename + functionname + '.db')
    db2 = database2.cursor()

    if functionname == "augmentation":

        data_augmentation(db,db2)

    if functionname == "runtime":

        runtimemath(db,db2)

    if functionname == "both":

        data_augmentation(db,db2)
        runtimemath(db2,db2)

    else:
        print("Please choose either augmentation or runtime as second parameter at script startup")
        print("If you want to do both, please consider writing both as parameter")
        print("This will execute the augmentation first and the runtime afterwards")

    '''
    Commiting the second and written database
    '''
    database2.commit()

    '''
    Closing both databases
    '''
    database2.close()
    print("Database written to:", databasename, functionname, ".db")
    print("Database:", databasename, "db not changed, will be closed again")
    database.close()


if __name__ == '__main__':

    databasename = sys.argv[1]
    functionname = sys.argv[2]
#    print(generate_dict(sqlite3.connect(databasename + '.db').cursor()))

    main()