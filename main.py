import psycopg2, json, os
from dotenv import dotenv_values

env_vars = dotenv_values('.env')

alias_1=env_vars['ALIAS_1']
host_1=env_vars['HOST_1']
port_1=env_vars['PORT_1']
username_1=env_vars['USERNAME_1']
password_1=env_vars['PASSWORD_1']
database_1=env_vars['DATABASE_1']

alias_2=env_vars['ALIAS_2']
host_2=env_vars['HOST_2']
port_2=env_vars['PORT_2']
username_2=env_vars['USERNAME_2']
password_2=env_vars['PASSWORD_2']
database_2=env_vars['DATABASE_2']

out_dir=env_vars['OUT_DIR']

if not os.path.exists(out_dir):
    os.makedirs(out_dir)

def get_column_data_for_database(database, user, password, host, port):
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port, connection_factory=None)
    cursor = conn.cursor()

    # Retrieve all table names
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' order by table_name")
    table_names = [row[0] for row in cursor.fetchall()]

    # Retrieve column names and data types for each table
    column_data = {}
    for table in table_names:
        column_data[table] = get_column_names_and_data_types(cursor, table)

    cursor.close()
    conn.close()

    return column_data

def get_column_names_and_data_types(cursor, table):
    cursor.execute(f"SELECT column_name, udt_name, character_maximum_length, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_name = '{table}'")
    columns = cursor.fetchall()

    column_data = []
    for column in columns:
        column_name = column[0]
        data_type = column[1]
        max_length = column[2]
        numeric_precision = column[3]
        numeric_scale = column[4]

        if max_length is not None:
            data_type += f"({max_length})"
        elif numeric_precision is not None and numeric_scale is not None:
            data_type += f"({numeric_precision},{numeric_scale})"

        column_data.append((column_name, data_type))

    return column_data


def get_diff_between_dictionaries(dict1, dict2):
    diff = {}

    for table, columns1 in dict1.items():
        if table not in dict2:
            diff[table] = {'added': columns1, 'removed': [], 'modified': []}
        else:
            columns2 = dict2[table]
            added_columns = [column for column in columns1 if column not in columns2]
            removed_columns = [column for column in columns2 if column not in columns1]
            modified_columns = []

            for column1 in columns1:
                if column1 in columns2:
                    index = columns2.index(column1)
                    if column1[1] != columns2[index][1]:  # Compare data types
                        modified_columns.append((column1, columns2[index]))


            if added_columns or removed_columns or modified_columns:
                diff[table] = {'added': added_columns, 'removed': removed_columns, 'modified': modified_columns}

    return diff

def generate_update_queries(diff):
    update_queries = []

    for table, changes in diff.items():
        added_columns = changes['added']
        removed_columns = changes['removed']
        modified_columns = changes['modified']

        # Generate SQL queries for added columns
        for column in added_columns:
            update_query = f"ALTER TABLE {table} ADD COLUMN {column[0]} {column[1]};"
            update_queries.append(update_query)

        # Generate SQL queries for removed columns
        for column in removed_columns:
            update_query = f"ALTER TABLE {table} DROP COLUMN {column};"
            update_queries.append(update_query)

        # Generate SQL queries for modified columns
        for old_column, new_column in modified_columns:
            update_query = f"ALTER TABLE {table} ALTER COLUMN {old_column[0]} TYPE {new_column[1]};"
            update_queries.append(update_query)

    return update_queries


def write_dictionary_to_json(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)

def write_queries_to_file(queries, output_file):
    with open(output_file, 'w') as file:
        for query in queries:
            file.write(query + "\n")

def generate_report(diff, queries, output_file):
    with open(output_file, 'w') as file:
        file.write("# Database Changes Report\n\n")
        file.write("This report summarizes the changes between two databases.\n\n")
        file.write("## Connection Details\n\n")
        file.write("| Alias | Database | Host | Port | Username |\n")
        file.write("| --- | --- | --- | --- | --- |\n")
        file.write(f"| {alias_1} | 1 | {host_1} | {port_1} | {username_1} |\n")
        file.write(f"| {alias_2} | 2 | {host_2} | {port_2} | {username_2} |\n")
        file.write("\n")

        file.write("The following changes have been detected in the databases:\n\n")

        for table, changes in diff.items():
            added_columns = changes['added']
            removed_columns = changes['removed']
            modified_columns = changes['modified']

            file.write(f"## Table: {table}\n\n")

            if added_columns:
                file.write("### Missing Columns\n\n")
                file.write("| Alias | Column Name | Data Type |\n")
                file.write("| --- | --- | --- |\n")
                for column in added_columns:
                    file.write(f"| {alias_2} | {column[0]} | {column[1]} |\n")
                file.write("\n")

            if removed_columns:
                file.write("### Additional Columns\n\n")
                file.write("| Alias | Column Name |\n")
                file.write("| --- | --- |\n")
                for column in removed_columns:
                    file.write(f"| {alias_1} | {column} |\n")
                file.write("\n")

            if modified_columns:
                file.write("### Modified Columns (Data Type Changed)\n\n")
                file.write("| Alias | Old Column Name | New Column Name | New Data Type |\n")
                file.write("| --- | --- | --- | --- |\n")
                for old_column, new_column in modified_columns:
                    file.write(f"| {alias_1} | {old_column[0]} | {new_column[0]} | {new_column[1]} |\n")
                file.write("\n")

        file.write("## Generated SQL Queries\n\n")
        file.write("````sql\n")
        for query in queries:
            file.write(f"{query}\n")
        file.write("````\n")



    # Obtain two result dictionaries
dict1 = get_column_data_for_database(database_1, username_1, password_1, host_1, port_1)
dict2 = get_column_data_for_database(database_2, username_2, password_2, host_2, port_2)

# Write dict1 to JSON
write_dictionary_to_json(dict1,  f'{out_dir}/dict1.json')

# Write dict2 to JSON
write_dictionary_to_json(dict2,  f'{out_dir}/dict2.json')


# Usage
diff = get_diff_between_dictionaries(dict1, dict2)

queries = generate_update_queries(diff)

generate_report(diff, queries, f'{out_dir}/diff.md')

for table, changes in diff.items():
    print(f"Table: {table}")
    added_columns = changes['added']
    removed_columns = changes['removed']
    modified_columns = changes['modified']

    if added_columns:
        print("Added columns:")
        for column in added_columns:
            print(f" - {column}")

    if removed_columns:
        print("Removed columns:")
        for column in removed_columns:
            print(f" - {column}")

    if modified_columns:
        print("Modified columns:")
        for old_column, new_column in modified_columns:
            print(f" - {old_column} (modified to {new_column})")

    print()
