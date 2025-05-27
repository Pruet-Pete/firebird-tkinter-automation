import fdb
import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox

# --- Firebird Database Path ---
QUBE_REPORT_DB_PATH = "C:\\Users\\DIY1208\\Desktop\\QubeReportDB\\POSSALES_DIY.FDB"

# --- Database Credentials ---
DB_CREDENTIALS = {
    "POSSTK_TH_C2": {"user": "RPTUSR", "password": "Rptusr@4567"},
    "QubeReport_Sales": {"user": "SYSDBA", "password": "masterkey"}
}

# --- Connect to Firebird Database ---
def connect_db(db_path, db_name):
    return fdb.connect(
        dsn=db_path,
        user=DB_CREDENTIALS[db_name]["user"],
        password=DB_CREDENTIALS[db_name]["password"]
    )

# --- Extract Balance Data ---
def extract_balance_data(year, month):
    try:
        formatted_month = str(month).zfill(2)
        column_suffix = str(int(month))  # Used for column naming

        db_path = (
            "TSERVER02:D:\\QasDev\\QubeV10\\BackEnd\\Db\\POSSTK.FDB"
            if USE_FIXED_DB.get()
            else f"TSERVER02:H:\\DANIEL TEMP\\AGING\\TH_C2\\POSSTK_TH_C2_{year}_{formatted_month}.FDB"
        )

        balance_columns = [f"S_BALANCE{column_suffix}", f"S_BALANCEAMT{column_suffix}"]

        conn = connect_db(db_path, "POSSTK_TH_C2")
        cur = conn.cursor()

        sql_query = f"""
            SELECT S_PLUCODE,
                   SUM(S_BALANCE) AS {balance_columns[0]},
                   SUM(S_BALANCE * S_AVGCOST) AS {balance_columns[1]}
            FROM STK_MOVEMENT_LMTH
            WHERE S_BALANCE <> 0
            GROUP BY S_PLUCODE;
        """

        cur.execute(sql_query)
        data = cur.fetchall()
        conn.close()

        if not data:
            return pd.DataFrame(), balance_columns

        df = pd.DataFrame(data, columns=["S_PLUCODE"] + balance_columns)
        df["S_PLUCODE"] = df["S_PLUCODE"].str.strip()
        df["S_PLULINKID"] = df["S_PLUCODE"] + "-" + df["S_PLUCODE"]

        return df, balance_columns

    except Exception as e:
        messagebox.showerror("Error", f"Database Error: {e}")
        return pd.DataFrame(), []

# --- Insert/Update to SALES_BYPLU_<year> ---
def update_balance_byplu(df, balance_columns, year):
    try:
        destination_table = f"SALES_BYPLU_{year}"
        conn = connect_db(QUBE_REPORT_DB_PATH, "QubeReport_Sales")
        cur = conn.cursor()

        for _, row in df.iterrows():
            cur.execute(
                f"SELECT COUNT(*) FROM {destination_table} WHERE S_PLULINKID = ?",
                (row["S_PLULINKID"],),
            )
            exists = cur.fetchone()[0]

            if exists:
                update_sql = f"""
                UPDATE {destination_table}
                SET {', '.join(f"{col} = ?" for col in balance_columns)}
                WHERE S_PLULINKID = ?
                """
                cur.execute(update_sql, tuple(row[balance_columns]) + (row["S_PLULINKID"],))
            else:
                insert_sql = f"""
                INSERT INTO {destination_table} (S_PLUCODE, S_PLULINKID, {', '.join(balance_columns)})
                VALUES ({', '.join(['?'] * (len(balance_columns) + 2))})
                """
                cur.execute(insert_sql, (row["S_PLUCODE"], row["S_PLULINKID"]) + tuple(row[balance_columns]))

        conn.commit()
        conn.close()
        messagebox.showinfo("Success", f"{destination_table} updated successfully!")

    except Exception as e:
        messagebox.showerror("Error", f"Error updating database: {e}")

# --- Run and Display ---
def run_query():
    year = entry_year.get().strip()
    month = entry_month.get().strip()

    if not year.isdigit() or not month.isdigit():
        messagebox.showerror("Input Error", "Year and Month must be numbers.")
        return

    df, balance_columns = extract_balance_data(year, month)

    if df.empty:
        messagebox.showinfo("No Data", "No matching records found.")
        return

    update_balance_byplu(df, balance_columns, year)

    window = tk.Toplevel()
    window.title(f"Updated Balance Data - {month} {year}")
    window.geometry("700x400")

    tree = ttk.Treeview(window, columns=["S_PLUCODE", "S_PLULINKID"] + balance_columns, show="headings")
    for col in ["S_PLUCODE", "S_PLULINKID"] + balance_columns:
        tree.heading(col, text=col)

    for _, row in df.iterrows():
        tree.insert("", "end", values=(row["S_PLUCODE"], row["S_PLULINKID"]) + tuple(row[balance_columns]))

    tree.pack(expand=True, fill="both")
    window.mainloop()

# --- Main GUI Setup ---
root = tk.Tk()
root.title("Balance Qty & Amt Query")
root.geometry("400x200")

USE_FIXED_DB = tk.BooleanVar(value=False)

tk.Label(root, text="Enter Year (e.g., 2025):").pack()
entry_year = tk.Entry(root)
entry_year.pack()

tk.Label(root, text="Enter Month (1-12):").pack()
entry_month = tk.Entry(root)
entry_month.pack()

tk.Checkbutton(root, text="Use Fixed Path", variable=USE_FIXED_DB).pack()
tk.Button(root, text="Run Query", command=run_query).pack()

root.mainloop()
