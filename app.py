import pyodbc
import pandas as pd
from flask import Flask, jsonify, render_template, request

pd.set_option('future.no_silent_downcasting', True)

app = Flask(__name__)

def fetch_company_names():
    try:
        conn = pyodbc.connect('DSN=TallyODBC64_9000;SERVER=localhost;DRIVER=Tally ODBC DRIVER64;PORT=9000')
        cursor = conn.cursor()
        query = "SELECT $Name FROM Company"
        cursor.execute(query)
        rows = cursor.fetchall()
        companies = [row[0].strip('$') for row in rows if row[0]]
        cursor.close()
        conn.close()
        print(f"[DEBUG] Fetched companies: {companies}")
        return companies
    except pyodbc.Error as e:
        print(f"[ERROR] ODBC connection failed: {e}")
        return ["Unknown"]
    except Exception as e:
        print(f"[ERROR] Failed to fetch companies: {e}")
        return ["Unknown"]

def fetch_balance_sheet(company_name):
    try:
        conn = pyodbc.connect('DSN=TallyODBC64_9000;SERVER=localhost;DRIVER=Tally ODBC DRIVER64;PORT=9000')
        cursor = conn.cursor()
        query = """
        SELECT $Name, $Parent, $ClosingBalance
        FROM Ledger
        WHERE $Name IS NOT NULL AND $ClosingBalance IS NOT NULL
        AND ($Name = 'Capital Account' OR $_PrimaryGroup = 'Capital Account' OR $_PrimaryGroup = 'Liabilities')
        """
        cursor.execute(query)
        columns = [column[0].strip('$') for column in cursor.description]
        rows = cursor.fetchall()
        print(f"[DEBUG] Raw Balance Sheet rows for {company_name}: {rows}")
        df = pd.DataFrame.from_records(rows, columns=columns) if rows else pd.DataFrame(columns=columns)
        if not df.empty and 'ClosingBalance' in df.columns:
            df['ClosingBalance'] = pd.to_numeric(df['ClosingBalance'], errors='coerce').fillna(0)
            df['Company'] = company_name
        else:
            df['Company'] = company_name
        cursor.close()
        conn.close()
        print(f"[DEBUG] Fetched Balance Sheet for {company_name}: {len(df)} rows")
        return df.to_dict(orient='records') if not df.empty else []
    except pyodbc.Error as e:
        print(f"[ERROR] ODBC error fetching Balance Sheet: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] Failed to fetch Balance Sheet: {e}")
        return []

def fetch_profit_loss(company_name):
    try:
        conn = pyodbc.connect('DSN=TallyODBC64_9000;SERVER=localhost;DRIVER=Tally ODBC DRIVER64;PORT=9000')
        cursor = conn.cursor()
        query = """
        SELECT $Name, $_PrimaryGroup, $ClosingBalance
        FROM Ledger
        WHERE $Name IS NOT NULL
        """
        cursor.execute(query)
        columns = [column[0].strip('$') for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns) if rows else pd.DataFrame(columns=columns)
        if not df.empty and 'ClosingBalance' in df.columns:
            df['ClosingBalance'] = pd.to_numeric(df['ClosingBalance'], errors='coerce').fillna(0)
            df['Company'] = company_name
        else:
            df['Company'] = company_name
        cursor.close()
        conn.close()
        print(f"[DEBUG] Fetched Profit & Loss for {company_name}: {len(df)} rows")
        return df.to_dict(orient='records') if not df.empty else []
    except pyodbc.Error as e:
        print(f"[ERROR] ODBC error fetching Profit & Loss: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] Failed to fetch Profit & Loss: {e}")
        return []

def fetch_trial_balance(company_name):
    try:
        conn = pyodbc.connect('DSN=TallyODBC64_9000;SERVER=localhost;DRIVER=Tally ODBC DRIVER64;PORT=9000')
        cursor = conn.cursor()
        query = """
        SELECT $Name, $_PrimaryGroup, $OpeningBalance, $DebitAmount, $CreditAmount, $ClosingBalance
        FROM Ledger
        WHERE $Name IS NOT NULL
        """
        cursor.execute(query)
        columns = [column[0].strip('$') for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns) if rows else pd.DataFrame(columns=columns)
        if not df.empty:
            for col in ['OpeningBalance', 'DebitAmount', 'CreditAmount', 'ClosingBalance']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            df['Company'] = company_name
        else:
            df['Company'] = company_name
        cursor.close()
        conn.close()
        print(f"[DEBUG] Fetched Trial Balance for {company_name}: {len(df)} rows")
        return df.to_dict(orient='records') if not df.empty else []
    except pyodbc.Error as e:
        print(f"[ERROR] ODBC error fetching Trial Balance: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] Failed to fetch Trial Balance: {e}")
        return []

def fetch_voucher(company_name):
    try:
        conn = pyodbc.connect('DSN=TallyODBC64_9000;SERVER=localhost;DRIVER=Tally ODBC DRIVER64;PORT=9000')
        cursor = conn.cursor()
        query = """
        SELECT $Date, $VoucherTypeName, $VoucherNumber, $Narration, $Amount
        FROM DayBook
        WHERE $Date IS NOT NULL AND $Amount IS NOT NULL
        """
        cursor.execute(query)
        columns = [column[0].strip('$') for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns) if rows else pd.DataFrame(columns=columns)
        if not df.empty and 'Amount' in df.columns:
            df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce').fillna(0)
        df['Company'] = company_name
        cursor.close()
        conn.close()
        print(f"[DEBUG] Fetched Voucher for {company_name}: {len(df)} rows")
        return df.to_dict(orient='records') if not df.empty else []
    except pyodbc.Error as e:
        print(f"[ERROR] ODBC error fetching Voucher: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] Failed to fetch Voucher: {e}")
        return []

def fetch_day_book(company_name):
    try:
        conn = pyodbc.connect('DSN=TallyODBC64_9000;SERVER=localhost;DRIVER=Tally ODBC DRIVER64;PORT=9000')
        cursor = conn.cursor()
        query = """
        SELECT $Date, $VoucherTypeName, $VoucherNumber, $Narration, $Amount
        FROM DayBook
        WHERE $Date IS NOT NULL AND $Amount IS NOT NULL
        """
        cursor.execute(query)
        columns = [column[0].strip('$') for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns) if rows else pd.DataFrame(columns=columns)
        if not df.empty and 'Amount' in df.columns:
            df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce').fillna(0)
            df['Company'] = company_name
        else:
            df['Company'] = company_name
        cursor.close()
        conn.close()
        print(f"[DEBUG] Fetched Day Book for {company_name}: {len(df)} rows")
        return df.to_dict(orient='records') if not df.empty else []
    except pyodbc.Error as e:
        print(f"[ERROR] ODBC error fetching Day Book: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] Failed to fetch Day Book: {e}")
        return []

@app.route('/')
def dashboard():
    company_name = fetch_company_names()[0] if fetch_company_names() else "Unknown"
    balance_sheet = fetch_balance_sheet(company_name)
    return render_template('index.html', company_name=company_name, balance_sheet=balance_sheet)

@app.route('/company-list')
def company_list():
    companies = fetch_company_names()
    return render_template('company_list.html', companies=companies)

@app.route('/company-details')
def company_details():
    company_name = request.args.get('company', 'Unknown')
    balance_sheet = fetch_balance_sheet(company_name)
    profit_loss = fetch_profit_loss(company_name)
    trial_balance = fetch_trial_balance(company_name)
    voucher = fetch_voucher(company_name)
    day_book = fetch_day_book(company_name)
    return render_template(
        'company_details.html',
        company_name=company_name,
        balance_sheet=balance_sheet,
        profit_loss=profit_loss,
        trial_balance=trial_balance,
        voucher=voucher,
        day_book=day_book
    )

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)