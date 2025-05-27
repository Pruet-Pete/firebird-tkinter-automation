# Firebird Tkinter Automation

This is a personal project repository used to prototype automation scripts with Python, Tkinter, Pandas, and Firebird SQL. The goal is to support internal data workflows for operational analytics, especially in retail and inventory settings.

---

##  Projects

###  `month_end_etl/`

A 3-step automation process for extracting month-end SKU-level data from multiple Firebird databases, transforming it with Pandas, and updating a central `.FDB` file on the shared drive.

#### Includes:
- `1. UPDATE_SKU.py` – Extracts SKU metadata from `MAS_STOCK`
- `2. UPDATE_BAL.py` – Fetches stock balances
- `3. UPDATE_SALES.py` – Merges and inserts month-end sales data into `SALES_BYPLU_2025`

#### Tools Used:
- `fdb` (Firebird Python connector)
- `pandas`
- `.env` configuration for secure DB paths and credentials

---

## 🔐 Credentials

All connection strings and credentials are stored in a `.env` file, and are excluded from the repo using `.gitignore`.

Example `.env`:
