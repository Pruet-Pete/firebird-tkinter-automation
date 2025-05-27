import fdb
import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox

# Firebird Database Connection Details
POSAPP_DB_PATH = 'TSERVER02:D:\\QasDev\\QubeV10\\BackEnd\\Db\\POSAPP.FDB'  # Adjust path
QUBE_REPORT_DB_PATH = 'C:\\Users\\DIY1208\\Desktop\\QubeReportDB\\POSSALES_DIY.FDB'  # Adjust path

DB_CREDENTIALS = {
    "POSAPP": {"user": "RPTUSR", "password": "Rptusr@4567"},
    "QubeReport_Sales": {"user": "SYSDBA", "password": "masterkey"}
}

# Function to connect to Firebird database with different credentials
def connect_db(db_path, db_name):
    return fdb.connect(
        dsn=db_path,
        user=DB_CREDENTIALS[db_name]["user"],
        password=DB_CREDENTIALS[db_name]["password"]
    )

# Step 1: Extract SKU Data from POSAPP (MAS_STOCK)
def extract_sku_data():
    try:
        conn = connect_db(POSAPP_DB_PATH, "POSAPP")
        cur = conn.cursor()

        sql_query = """
            SELECT M_PLUCODE, M_GROUP, M_DEPARTMENT, M_CATEGORY, M_BRAND
            FROM MAS_STOCK
        """
        
        cur.execute(sql_query)
        data = cur.fetchall()
        
        # Create DataFrame and generate S_PLULINKID as S_PLUCODE + '-' + S_PLUCODE
        df = pd.DataFrame(data, columns=["S_PLUCODE", "S_GROUP", "S_DEPARTMENT", "S_CATEGORY", "S_BRAND"])
        df["S_PLULINKID"] = df["S_PLUCODE"] + "-" + df["S_PLUCODE"]
        
        conn.close()
        return df
    
    except Exception as e:
        messagebox.showerror("Error", f"Database Error: {e}")
        return pd.DataFrame()

# Step 2: Insert or Update SKU Data in SALES_BYPLU_2025
def update_sales_byplu(df):
    try:
        conn = connect_db(QUBE_REPORT_DB_PATH, "QubeReport_Sales")
        cur = conn.cursor()

        for _, row in df.iterrows():
            s_plucode = row["S_PLUCODE"]
            s_plulinkid = row["S_PLULINKID"]
            s_group = row["S_GROUP"]
            s_department = row["S_DEPARTMENT"]
            s_category = row["S_CATEGORY"]
            s_brand = row["S_BRAND"]

            # Check if the SKU exists
            cur.execute("SELECT COUNT(*) FROM SALES_BYPLU_2025 WHERE S_PLULINKID = ?", (s_plulinkid,))
            exists = cur.fetchone()[0]

            if exists:
                # Update existing record (without changing the primary key)
                update_sql = """
                UPDATE SALES_BYPLU_2025 
                SET S_PLUCODE = ?, S_GROUP = ?, S_DEPARTMENT = ?, S_CATEGORY = ?, S_BRAND = ?
                WHERE S_PLULINKID = ?
                """
                cur.execute(update_sql, (s_plucode, s_group, s_department, s_category, s_brand, s_plulinkid))
            else:
                # Insert new record
                insert_sql = """
                INSERT INTO SALES_BYPLU_2025 (S_PLUCODE, S_PLULINKID, S_GROUP, S_DEPARTMENT, S_CATEGORY, S_BRAND)
                VALUES (?, ?, ?, ?, ?, ?)
                """
                cur.execute(insert_sql, (s_plucode, s_plulinkid, s_group, s_department, s_category, s_brand))

        conn.commit()
        conn.close()
        messagebox.showinfo("Success", "Database Updated Successfully!")

    except Exception as e:
        messagebox.showerror("Error", f"Error updating database: {e}")

# Step 3: Display Data in Tkinter
def show_updated_data():
    df = extract_sku_data()
    if df.empty:
        return
    
    update_sales_byplu(df)

    # Create Tkinter Window
    window = tk.Tk()
    window.title("Updated SKU Data")
    window.geometry("700x400")

    # Treeview for displaying data
    tree = ttk.Treeview(window, columns=("S_PLUCODE", "S_PLULINKID", "S_GROUP", "S_DEPARTMENT", "S_CATEGORY", "S_BRAND"), show="headings")
    tree.heading("S_PLUCODE", text="S_PLUCODE")
    tree.heading("S_PLULINKID", text="S_PLULINKID")
    tree.heading("S_GROUP", text="S_GROUP")
    tree.heading("S_DEPARTMENT", text="S_DEPARTMENT")
    tree.heading("S_CATEGORY", text="S_CATEGORY")
    tree.heading("S_BRAND", text="S_BRAND")

    # Insert data into treeview
    for _, row in df.iterrows():
        tree.insert("", "end", values=(row["S_PLUCODE"], row["S_PLULINKID"], row["S_GROUP"], row["S_DEPARTMENT"], row["S_CATEGORY"], row["S_BRAND"]))

    tree.pack(expand=True, fill="both")
    window.mainloop()

# Run the Process
if __name__ == "__main__":
    show_updated_data()  # Extract, update, and display data
