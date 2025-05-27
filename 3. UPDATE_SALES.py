import fdb
import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox

# Firebird Database Paths
QUBE_REPORT_DB_PATH = "C:\\Users\\DIY1208\\Desktop\\QubeReportDB\\POSSALES_DIY.FDB"

# Database Credentials
DB_CREDENTIALS = {
    "POSSTK_TH_C2": {"user": "RPTUSR", "password": "Rptusr@4567"},
    "QubeReport_Sales": {"user": "SYSDBA", "password": "masterkey"}
}

# Function to Connect to Firebird Database
def connect_db(db_path, db_name):
    return fdb.connect(
        dsn=db_path,
        user=DB_CREDENTIALS[db_name]["user"],
        password=DB_CREDENTIALS[db_name]["password"]
    )

# Function to Extract Data from POSSTK_TH_C2
def extract_sales_data(year, month):
    try:
        formatted_month = str(month).zfill(2)  # Converts '1' to '01'
        if USE_FIXED_DB.get():
            db_path = "TSERVER02:D:\\QasDev\\QubeV10\\BackEnd\\Db\\POSSTK.FDB"
        else:
            db_path = f"TSERVER02:H:\\DANIEL TEMP\\AGING\\TH_C2\\POSSTK_TH_C2_{year}_{formatted_month}.FDB"

        conn = connect_db(db_path, "POSSTK_TH_C2")
        cur = conn.cursor()

        # Determine column names based on user input month
        column_suffix = str(int(month))  # Convert to integer to remove leading zero
        columns = [f"S_QTY{column_suffix}", f"S_SALES{column_suffix}", f"S_PROFIT{column_suffix}", 
                   f"S_SALESNOTAX{column_suffix}", f"S_PROFITNOTAX{column_suffix}", ]

        # Query to extract data
        sql_query = f"""
            SELECT SDTL_PLUNO, 
                SUM(SDTL_QTY) AS {columns[0]}, 
                SUM(SDTL_NETPRICE) AS {columns[1]}, 
                SUM(SDTL_NETPRICE) - SUM(SDTL_QTY * SDTL_UNITCOST) AS {columns[2]}, 
                (SUM(SDTL_NETPRICE) - 
                    SUM((SDTL_TAXABLEAMT1 / (1 + (SDTL_TAX1PCN * 0.01))) * SDTL_TAX1PCN * 0.01) - 
                    SUM((SDTL_TAXABLEAMT2 / (1 + (SDTL_TAX2PCN * 0.01))) * SDTL_TAX2PCN * 0.01) - 
                    SUM((SDTL_TAXABLEAMT3 / (1 + (SDTL_TAX3PCN * 0.01))) * SDTL_TAX3PCN * 0.01) - 
                    SUM((SDTL_TAXABLEAMT4 / (1 + (SDTL_TAX4PCN * 0.01))) * SDTL_TAX4PCN * 0.01)) AS {columns[3]}, 
                (SUM(SDTL_NETPRICE) - 
                    SUM((SDTL_TAXABLEAMT1 / (1 + (SDTL_TAX1PCN * 0.01))) * SDTL_TAX1PCN * 0.01) - 
                    SUM((SDTL_TAXABLEAMT2 / (1 + (SDTL_TAX2PCN * 0.01))) * SDTL_TAX2PCN * 0.01) - 
                    SUM((SDTL_TAXABLEAMT3 / (1 + (SDTL_TAX3PCN * 0.01))) * SDTL_TAX3PCN * 0.01) - 
                    SUM((SDTL_TAXABLEAMT4 / (1 + (SDTL_TAX4PCN * 0.01))) * SDTL_TAX4PCN * 0.01) - 
                    SUM(SDTL_QTY * SDTL_UNITCOST)) AS {columns[4]}
            FROM POS_SALESDTL
            WHERE EXTRACT(YEAR FROM SDTL_CLOSEDATE) = {year}
                AND EXTRACT(MONTH FROM SDTL_CLOSEDATE) = {month}
                AND SDTL_STATUS = 'NS'
            GROUP BY SDTL_PLUNO;
        """       

        cur.execute(sql_query)
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=["S_PLUCODE"] + columns)
        
        # Create S_PLULINKID
        df["S_PLULINKID"] = df["S_PLUCODE"].str.strip() + "-" + df["S_PLUCODE"].str.strip()
        
        conn.close()
        return df, columns  # Returning both the data and column names

    except Exception as e:
        messagebox.showerror("Error", f"Database Error: {e}")
        return pd.DataFrame(), []

# Function to Insert/Update Data in SALES_BYPLU_2025
def update_sales_byplu(df, columns):
    try:
        conn = connect_db(QUBE_REPORT_DB_PATH, "QubeReport_Sales")
        cur = conn.cursor()

        for _, row in df.iterrows():
            s_plucode = row["S_PLUCODE"]
            s_plulinkid = row["S_PLULINKID"]
            s_qty = row[columns[0]]
            s_sales = row[columns[1]]
            s_profit = row[columns[2]]
            s_salesnotax = row[columns[3]]
            s_profitnotax = row[columns[4]]

            # Check if the SKU exists
            cur.execute("SELECT COUNT(*) FROM SALES_BYPLU_2025 WHERE S_PLULINKID = ?", (s_plulinkid,))
            exists = cur.fetchone()[0]

            if exists:
                # Update existing record
                update_sql = f"""
                UPDATE SALES_BYPLU_2025 
                SET {columns[0]} = ?, {columns[1]} = ?, {columns[2]} = ?, {columns[3]} = ?, {columns[4]} = ?
                WHERE S_PLULINKID = ?
                """
                cur.execute(update_sql, (s_qty, s_sales, s_profit, s_salesnotax, s_profitnotax, s_plulinkid))
            else:
                # Insert new record
                insert_sql = f"""
                INSERT INTO SALES_BYPLU_2025 (S_PLUCODE, S_PLULINKID, {columns[0]}, {columns[1]}, {columns[2]}, {columns[3]}, {columns[4]})
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                cur.execute(insert_sql, (s_plucode, s_plulinkid, s_qty, s_sales, s_profit, s_salesnotax, s_profitnotax))

        conn.commit()
        conn.close()
        messagebox.showinfo("Success", "Database Updated Successfully!")

    except Exception as e:
        messagebox.showerror("Error", f"Error updating database: {e}")


# Tkinter UI for User Input
def run_query():
    year = entry_year.get().strip()
    month = entry_month.get().strip()

    # Validate inputs
    if not year.isdigit() or not month.isdigit():
        messagebox.showerror("Input Error", "Year and Month must be numbers.")
        return

    df, columns = extract_sales_data(year, month)
    if df.empty:
        messagebox.showinfo("No Data", "No matching records found for the selected Year and Month.")
        return

    update_sales_byplu(df, columns)

    # Display the updated data
    window = tk.Toplevel()
    window.title(f"Updated Sales Data - {year} {month}")
    window.geometry("700x400")

    tree = ttk.Treeview(window, columns=["S_PLUCODE", "S_PLULINKID"] + columns, show="headings")
    
    # Set headings
    tree.heading("S_PLUCODE", text="S_PLUCODE")
    tree.heading("S_PLULINKID", text="S_PLULINKID")
    for col in columns:
        tree.heading(col, text=col)

    # Insert data into treeview
    for _, row in df.iterrows():
        tree.insert("", "end", values=(row["S_PLUCODE"], row["S_PLULINKID"], row[columns[0]], row[columns[1]], row[columns[2]], row[columns[3]], row[columns[4]]))

    tree.pack(expand=True, fill="both")
    window.mainloop()

# Main Tkinter App
root = tk.Tk()
root.title("Sales Data Query")
root.geometry("400x200")

USE_FIXED_DB = tk.BooleanVar(value=False)

tk.Label(root, text="Enter Year (e.g., 2025):").pack()
entry_year = tk.Entry(root)
entry_year.pack()

tk.Label(root, text="Enter Month (1-12):").pack()
entry_month = tk.Entry(root)
entry_month.pack()

chk = tk.Checkbutton(root, text="Use Fixed Path", variable=USE_FIXED_DB)
chk.pack()

tk.Button(root, text="Run Query", command=run_query).pack()

root.mainloop()
