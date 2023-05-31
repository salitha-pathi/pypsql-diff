import psycopg2, json, os, difflib
from dotenv import dotenv_values

env_vars = dotenv_values('.env')

alias_1 = env_vars['ALIAS_1']
host_1 = env_vars['HOST_1']
port_1 = env_vars['PORT_1']
username_1 = env_vars['USERNAME_1']
password_1 = env_vars['PASSWORD_1']
database_1 = env_vars['DATABASE_1']

alias_2 = env_vars['ALIAS_2']
host_2 = env_vars['HOST_2']
port_2 = env_vars['PORT_2']
username_2 = env_vars['USERNAME_2']
password_2 = env_vars['PASSWORD_2']
database_2 = env_vars['DATABASE_2']

out_dir = env_vars['OUT_DIR']

if not os.path.exists(out_dir):
    os.makedirs(out_dir)


def get_column_data_for_database(database, user, password, host, port):
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port,
                            connection_factory=None)
    cursor = conn.cursor()

    # Retrieve all table names
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' order by table_name")
    table_names = [row[0] for row in cursor.fetchall()]

    # Retrieve column names and data types for each table
    sequence_data = get_all_sequences(cursor)
    routines_data = get_all_routines(cursor)
    column_data = {}
    for table in table_names:
        column_data[table] = get_column_names_and_data_types(cursor, table)

    cursor.close()
    conn.close()

    return column_data, sequence_data, routines_data


def get_column_names_and_data_types(cursor, table):
    cursor.execute(
        f"SELECT column_name, udt_name, character_maximum_length, numeric_precision, numeric_scale,is_nullable, column_default FROM information_schema.columns WHERE table_name = '{table}'")
    columns = cursor.fetchall()

    column_data = []
    for column in columns:
        column_name = column[0]
        data_type = column[1]
        max_length = column[2]
        numeric_precision = column[3]
        numeric_scale = column[4]
        is_nullable = column[5]
        column_default = column[6]

        if max_length is not None:
            data_type += f"({max_length})"
        elif numeric_precision is not None and numeric_scale is not None:
            data_type += f"({numeric_precision},{numeric_scale})"

        column_data.append((column_name, data_type, column_default, is_nullable))

    return column_data


def get_all_sequences(cursor):
    cursor.execute(
        "SELECT sequence_name, data_type FROM information_schema.sequences WHERE sequence_schema NOT LIKE 'pg_%'")
    sequences = cursor.fetchall()

    sequences_data = []
    for seq in sequences:
        sequence_name = seq[0]
        data_type = seq[1]
        sequences_data.append([sequence_name, data_type])
    return sequences_data


def get_all_routines(cursor):
    cursor.execute(
        "SELECT routine_name, routine_definition FROM information_schema.routines WHERE routine_schema NOT LIKE 'pg_%' AND routine_type = 'FUNCTION'")
    routines = cursor.fetchall()

    routines_data = []
    for routine in routines:
        routine_name = routine[0]
        if routine[1]:
            routine_definition = ''.join(routine[1].split('\n'))
        else:
            routine_definition = ''

        routines_data.append([routine_name, routine_definition])

    return routines_data


def get_diff_between_dictionaries(dict1, dict2):
    diff = {}

    for table, columns1 in dict1.items():
        if table not in dict2:
            diff[table] = {'missing': columns1, 'additional': [], 'modified': []}
        else:
            columns2 = dict2[table]
            missing_columns = [column for column in columns1 if column not in columns2]
            additional_columns = [column for column in columns2 if column not in columns1]
            modified_columns = []

            for column1 in columns1:
                if column1 in columns2:
                    index = columns2.index(column1)
                    if column1[1] != columns2[index][1]:  # Compare data types
                        modified_columns.append((column1, columns2[index]))

            if missing_columns or additional_columns or modified_columns:
                diff[table] = {'missing': missing_columns, 'additional': additional_columns,
                               'modified': modified_columns}

    return diff


def compare_2d_arrays(arr1, arr2):
    dict1 = dict(arr1)
    dict2 = dict(arr2)

    missing_keys = []
    added_keys = []
    modified_keys = []

    for key in dict1:
        if key in dict2:
            if dict1[key] != dict2[key]:
                modified_keys.append([key, dict1[key], dict2[key]])
        else:
            missing_keys.append([key, dict1[key]])

    for key in dict2:
        if key not in dict1:
            added_keys.append([key, dict2[key]])

    return {'missing': missing_keys, 'additional': added_keys, 'modified': modified_keys}


def remove_whitespace_lines(text):
    lines = text.split('\n')
    filtered_lines = [line for line in lines if line.strip()]
    return '\n'.join(filtered_lines)


def get_code_diff(code1, code2):
    lines1 = remove_whitespace_lines(code1).splitlines(keepends=True)
    lines2 = remove_whitespace_lines(code2).splitlines(keepends=True)

    differ = difflib.Differ()
    diff = differ.compare(lines1, lines2)
    return ''.join(diff)


def generate_update_queries(diff):
    update_queries = []

    for table, changes in diff.items():
        missing_columns = changes['missing']
        additional_columns = changes['additional']
        modified_columns = changes['modified']

        # Generate SQL queries for added columns
        for column in missing_columns:
            column_name, data_type, default_value, is_nullable = column
            default_clause = f" DEFAULT {default_value}" if default_value is not None else ""
            nullable_clause = " NULL" if is_nullable == 'YES' else "NOT NULL"
            query = f" ALTER TABLE {table} ADD COLUMN {column_name} {data_type}{default_clause}{nullable_clause};"
            update_queries.append(query)

        # Generate SQL queries for removed columns
        for column in additional_columns:
            update_query = f"ALTER TABLE {table} DROP COLUMN {column};"
            update_queries.append(update_query)

        # Generate SQL queries for modified columns
        for old_column, new_column in modified_columns:
            old_column_name, old_data_type, old_default_value = old_column
            new_column_name, new_data_type, new_default_value, new_is_nullable = new_column
            default_clause = f"  DEFAULT {new_default_value}" if new_default_value is not None else ""
            nullable_clause = " NULL" if new_is_nullable == 'YES' else "NOT NULL"
            query = f"ALTER TABLE {table} ALTER COLUMN {old_column_name} SET DATA TYPE {new_data_type} USING {old_column_name}::{new_data_type}{default_clause}{nullable_clause};"
            update_queries.append(update_query)

    return update_queries


def write_dictionary_to_json(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)


def write_queries_to_file(queries, output_file):
    with open(output_file, 'w') as file:
        for query in queries:
            file.write(query + "\n")


def generate_report(column_diff, routines_diff, sequences_diff, queries, output_file):
    with open(output_file, 'w') as file:
        file.write("# Database Diff Report\n\n")
        file.write("This report summarizes the changes between two databases.\n\n")
        file.write("### Connection Details\n\n")
        file.write("| Alias | Database | Host | Port | Username |\n")
        file.write("| --- | --- | --- | --- | --- |\n")
        file.write(f"| {alias_1} | {database_1} | {host_1} | {port_1} | {username_1} |\n")
        file.write(f"| {alias_2} | {database_2} | {host_2} | {port_2} | {username_2} |\n")
        file.write("\n")

        file.write("## Schema changes\n")
        file.write("The following changes have been detected in the database schema:\n\n")

        for table, changes in column_diff.items():
            missing_columns = changes['missing']
            additional_columns = changes['additional']
            modified_columns = changes['modified']

            file.write(f"## Table: {table}\n\n")

            if missing_columns:
                file.write("### Missing Columns\n\n")
                file.write("| Alias | Column Name | Data Type |\n")
                file.write("| --- | --- | --- |\n")
                for column in missing_columns:
                    file.write(f"| {alias_2} | {column[0]} | {column[1]} |\n")
                file.write("\n")

            if additional_columns:
                file.write("### Additional Columns\n\n")
                file.write("| Alias | Column Name |\n")
                file.write("| --- | --- |\n")
                for column in additional_columns:
                    file.write(f"| {alias_1} | {column} |\n")
                file.write("\n")

            if modified_columns:
                file.write("### Modified Columns (Data Type Changed)\n\n")
                file.write("| Alias | Column Name | Old Data Type | New Data Type |\n")
                file.write("| --- | --- | --- | --- |\n")
                for old_column, new_column in modified_columns:
                    file.write(f"| {alias_1} | {old_column[0]} | {new_column[0]} | {new_column[1]} |\n")
                file.write("\n")

        file.write("## Generated SQL Queries to Update Columns\n\n")
        file.write("````sql\n")
        for query in queries:
            file.write(f"{query}\n")
        file.write("````\n\n")

        file.write("## Sequences Changes\n")
        file.write("The following changes have been detected in functions/routines.\n\n")

        if sequences_diff['missing']:
            file.write("### Missing Sequences\n\n")
            file.write("| Name | Return Type |\n")
            file.write("| --- | --- |\n")
            for seq in sequences_diff['missing']:
                file.write(f"| {seq[0]} | seq[0] |\n")
            file.write("\n")

        if sequences_diff['additional']:
            file.write("### Additional Sequences\n\n")
            file.write("| Name | Return Type |\n")
            file.write("| --- | --- |\n")
            for seq in sequences_diff['additional']:
                file.write(f"| {seq[0]} | seq[0] |\n")
            file.write("\n")

        if sequences_diff['modified']:
            file.write("### Modified Sequences\n\n")
            file.write("| Name | Return Type |\n")
            file.write("| --- | --- |\n")
            for seq in sequences_diff['modified']:
                file.write(f"| {seq[0]} | seq[0] |\n")
            file.write("\n")

        file.write("## Functions/Routines Changes\n")
        file.write("The following changes have been detected in functions/routines.\n\n")

        if routines_diff['missing']:
            file.write("### Missing Routines\n\n")
            for route in routines_diff['missing']:
                file.write(f"#### {route[0]}\n")
                file.write(f"````\n{route[1]}\n````\n\n")
            file.write("\n")

        if routines_diff['additional']:
            file.write("### Additional Routines\n\n")
            for route in routines_diff['added']:
                file.write(f"#### {route[0]}\n")
                file.write(f"````\n{route[1]}\n````\n\n")
            file.write("\n")

        if routines_diff['modified']:
            file.write("### Modified Routines\n\n")
            for route in routines_diff['modified']:
                file.write(f"#### {route[0]}\n")
                diff_text = get_code_diff(route[1], route[2])
                file.write(f"````diff\n{diff_text}\n````\n")
            file.write("\n")


# Obtain two result dictionaries


columns_1, sequences_1, routines_1 = get_column_data_for_database(database_1, username_1, password_1, host_1, port_1)
columns_2, sequences_2, routines_2 = get_column_data_for_database(database_2, username_2, password_2, host_2, port_2)

seq_diff = compare_2d_arrays(sequences_1, sequences_2)
routes_diff = compare_2d_arrays(routines_1, routines_2)

write_dictionary_to_json(columns_1, f'{out_dir}/schema-{alias_1}-{database_1}.json')
write_dictionary_to_json(columns_2, f'{out_dir}/schema-{alias_2}-{database_2}.json')

columns_diff = get_diff_between_dictionaries(columns_1, columns_2)
write_dictionary_to_json(columns_diff, f'{out_dir}/diff.json')

queries = generate_update_queries(columns_diff)

generate_report(columns_diff, routes_diff, seq_diff, queries,
                f'{out_dir}/diff-{alias_1}-{database_1}-VS-{alias_2}-{database_2}.md')

for table, changes in columns_diff.items():
    print(f"Table: {table}")
    added_columns = changes['missing']
    removed_columns = changes['additional']
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
