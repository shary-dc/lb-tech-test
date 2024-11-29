import psycopg2

db_config = {
    "user": "input_value_here",
    "password": "input_value_here",
    "host": "input_value_here",
    "port": "input_value_here",
    "database": "input_value_herez"
}

try:
    conn = psycopg2.connect(
        user=db_config["user"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"],
        database=db_config["database"]
    )
except Exception as e:
    print(f"error: {e}")
    exit()

cursor = conn.cursor()

def check_null_values(table, column):
    query = f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL OR {column} = '';"
    cursor.execute(query)
    result = cursor.fetchone()[0]
    return result > 0

def check_unexpected_values(table, column, expected_value):
    query = f"SELECT COUNT({column}) FROM {table} WHERE {column} NOT IN ({expected_value});"
    cursor.execute(query)
    result = cursor.fetchone()[0]
    return result > 0

def check_unexpected_character_values(table, column):
    query = f"SELECT COUNT({column}) FROM {table} WHERE {column} ~ '[^a-zA-Z0-9]';"
    cursor.execute(query)
    result = cursor.fetchone()[0]
    return result > 0

def check_negative_values(table, column):
    query = f"SELECT COUNT({column}) FROM {table} WHERE {column} < 0;"
    cursor.execute(query)
    result = cursor.fetchone()[0]
    return result > 0

def check_zero_values(table, column):
    query = f"SELECT COUNT({column}) FROM {table} WHERE {column} = 0;"
    cursor.execute(query)
    result = cursor.fetchone()[0]
    return result > 0

def check_time_integrity_issues():
    query = f"SELECT COUNT(*) FROM trades WHERE close_time < open_time;"
    cursor.execute(query)
    result = cursor.fetchone()[0]
    return result > 0

def check_invalid_dates():
    query = f"SELECT COUNT(*) FROM trades WHERE open_time < '1900-01-01' OR close_time > NOW();"
    cursor.execute(query)
    result = cursor.fetchone()[0]
    return result > 0

def check_unmatched_logins():
    query = "SELECT COUNT(*) FROM trades t LEFT JOIN users u ON t.login_hash = u.login_hash WHERE u.login_hash IS NULL;"
    cursor.execute(query)
    result = cursor.fetchone()[0]
    return result > 0

'''
some edge cases (but needs more business requirements):
# if trades are closed at the same time by the trader
# if trades are opened for an unusually time
# if symbols aren't valid FI
# if trades are duplicates
# if trades are done beyond market hours
# if trades have values that are extreme
'''

# run method
def run_data_quality_checks():
    issues = []
    users_table = "users"
    trades_table = "trades"

    if check_null_values(users_table, "login_hash"):
        issues.append(f"table: {users_table}, column: login_hash contains null values.")
    if check_null_values(users_table, "server_hash"):
        issues.append(f"table: {users_table}, column: server_hash contains null values.")

    if check_null_values(trades_table, "login_hash"):
        issues.append(f"table: {trades_table}, column: login_hash contains null values.")
    if check_null_values(trades_table, "server_hash"):
        issues.append(f"table: {trades_table}, column: server_hash contains null values.")
    if check_null_values(trades_table, "symbol"):
        issues.append(f"table: {trades_table}, column: symbol contains null values.")

    if check_unexpected_values(users_table, "enable", "1, 0"):
        issues.append(f"table: {users_table}, column: enable contains unexpected values.")
    if check_unexpected_values(trades_table, "cmd", "1, 0"):
        issues.append(f"table: {trades_table}, column: cmd contains unexpected values.")
    
    if check_unexpected_character_values(users_table, "currency"):
        issues.append(f"table: {users_table}, column: currency contains unexpected characters.")
    if check_unexpected_character_values(trades_table, "symbol"):
        issues.append(f"table: {trades_table}, column: symbol contains unexpected characters.")

    if check_zero_values(trades_table, "volume"):
        issues.append(f"table: {trades_table}, column: volume contains 0 in some trades.")
    if check_zero_values(trades_table, "open_price"):
        issues.append(f"table: {trades_table}, column: open_price contains 0 in some trades.")
    if check_zero_values(trades_table, "contractsize"):
        issues.append(f"table: {trades_table}, column: contractsize contains 0 in some trades.")

    if check_negative_values(trades_table, "volume"):
        issues.append(f"table: {trades_table}, column: volume contains negative values in some trades.)")
    if check_negative_values(trades_table, "open_price"):
        issues.append(f"table: {trades_table}, column: open_price contains negative values in some trades.)")
    if check_negative_values(trades_table, "contractsize"):
        issues.append(f"table: {trades_table}, column: contractsize contains negative values in some trades.)")

    if check_time_integrity_issues():
        issues.append("'close_time' is earlier than 'open_time' in some trades.")

    if check_invalid_dates():
        issues.append("Some trades are set values too far in the past or future.")
    
    if check_unmatched_logins():
        issues.append("some trades have 'login_hash' values not present in the users table.")

    return issues

# run and display
issues = run_data_quality_checks()
if issues:
    print("Data Quality Issues Found:")
    for issue in issues:
        print(f"- {issue}")
else:
    print("No data quality issues found.")

cursor.close()
conn.close()