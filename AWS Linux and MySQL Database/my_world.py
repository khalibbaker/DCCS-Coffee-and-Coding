# Import Libraries
# Needed (in Anaconda Prompt: $ pip install pymysql)
import pymysql.cursors
import sys

def welcome_message():
    print('****************************************')
    print('\Welcome to the Khalibs World Database')
    print('Database Engine: MySQL')
    print('$ AMAZON RDS CLOUD SERVER $\n')
    print('****************************************')

def dispay_options(connection):
    # Create a cursor for writing SQL queries to the database
    cursor = connection.cursor()

    # Get the names of all the tables to display to user
    cursor.execute('show tables')
    # Set results of query to a variable
    result = cursor.fetchall()

    # Initialize variables for later
    user_options = {}
    option_index = 0

    # Stored the table names into a dictionary, keys will be used for referencing
    # a selection to that table
    for r in result:
        user_options[option_index] = r['Tables_in_my_world']
        option_index += 1
    # Message
    print('\n\nTables in Khalibs World')
    # Display options to the user
    for option in user_options:
        print('[%i]\t%s' % (option, user_options[option]))
    print('[%i]\texit\n' % (len(user_options)))

    # Get selection from the user
    user_selection = int(input('Select table to modify (enter number): '))

    # Allow for exit of program
    if user_selection == 10:
        print('Committing changes...\nDone!')
        return 'exit'
    # Check for out of bound range
    while user_selection not in range(0, 11):
        user_selection = int(input('Select table to modify (enter number): '))
        # Construct a sql query to send to the add_data function, this sql query serves
        # as the first step in adding data (first step = get primary key count)

        # Allow for exit of program
        if user_selection == 10:
            print('Committing changes...\nDONE')
            return 'exit'

    print('Executing query...\nSending data...')

    # Return the user selection, which is name of the table
    return str(user_options[user_selection])

def add_data(table, connection):
    # Construct a sql query to send to the add_data function, this sql query serves
    # as the first step in adding data (first step = get primary key count)
    init_query = 'SELECT count(*) FROM ' + table

    # Create a cursor for writing SQL queries to the database
    cursor = connection.cursor()

    # Query the next iteration of the primary key (or ID column)
    cursor.execute(init_query)
    # Set results of query to a variable
    result = cursor.fetchone()
    # Set next iteration to a variable
    curr_pkey = int(list(result.values())[0])

    # Get column (field) names from the table that user requested
    cursor.execute('show fields from ' + table)
    # Set results of query to a variable
    result = cursor.fetchall()
    # Get names of columns (fields) and store into list
    column_names = []
    for r in result:
        column_names.append(r['Field'])

    # Start constucting parts of the INSERT query
    column_INSERT = '('
    for column in column_names:
        column_INSERT += column
        column_INSERT += ', '
    column_INSERT = column_INSERT[:-2]
    column_INSERT += ')'

    # Get values from user and store into dictionary
    column_ENTRIES = {}
    for entry in range(0, len(column_names)):
        if entry == 0:
            column_ENTRIES[column_names[entry]] = curr_pkey
        else:
            column_ENTRIES[column_names[entry]] = input(str(column_names[entry]) + ': ')
    # Convert values to list
    ENTRIES_LIST = list(column_ENTRIES.values())

    # Constuct the arguments parameters of INSERT query
    arg_INSERT = '('
    for arg in column_names:
        arg_INSERT += '%s, '
    arg_INSERT = arg_INSERT[:-2]
    arg_INSERT += ')'

    # Construct query to add the data inputed, and sent it to the database
    sql_query = 'INSERT INTO ' + str(table) + ' ' + str(column_INSERT) + ' VALUES ' + str(arg_INSERT)
    # Execute query
    cursor.execute(sql_query, (ENTRIES_LIST))
    # Commit data to the database
    connection.commit()



class RUN_SCRIPT:
    # Initialize method for the class
    def __init__(self):
        try:
            self.root = input('Master Username: ')
            self.password = input('Master Password: ')
            self.connection = pymysql.connect(host='127.0.0.1',
                                 user=self.root,
                                 password=self.password,
                                 db='my_world',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
        # If error from connecting to database, print
        except:
            print('ERROR: Check password, DNS endpoint, or DB Name')
            sys.exit()

    def execute(self):
        table = dispay_options(self.connection)
        if table == 'exit':
            sys.exit()
        add_data(table, self.connection)

if __name__ == '__main__':

    # Print Welcome Message
    welcome_message()

    # Set class variables
    run = RUN_SCRIPT()
    go = True

    # Execute the script
    while go:
        run.execute()
