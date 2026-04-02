# Flow Battery Reproducibility Study — Dashboard

This is the dashboard for the international flow battery reproducibility study. It shows electrochemical data collected by participating institutions and allows you to explore and compare results interactively.

## 🌐 View the Dashboard Online
No installation needed — just open the link below in any web browser:

**https://flowbatterydata.org**

---

## 📊 About the Dataset

All experimental data from this study is stored in a database called **Google BigQuery** and is freely available to anyone. 

### Why a database?
The study involves hundreds of thousands of rows of electrochemical measurements from dozens of institutions. A database is the most efficient and reliable way to store, query, and share this volume of data — far better than sharing individual Excel files. It also means the data is always up to date as new participants submit results.

### What is BigQuery?
BigQuery is Google's cloud database service. Think of it like a very powerful spreadsheet in the cloud that anyone can query using a language called SQL. You don't need to download anything — you just ask it questions and it returns the data you need.

### Accessing the data
The dataset is public — you just need a free Google account to access it.

- **Project:** `flow-battery-data-ingestion`
- **Dataset:** `electrochem`
- **Tables:**
  - `charge-discharge-data` — raw time-series cycling data
  - `charge-discharge-metrics` — per-cycle efficiency and capacity metrics
  - `polarisation-metrics` — polarisation pulse data
  - `eis-data` — electrochemical impedance spectra
  - `participant-metadata` — experimental protocol details submitted by participants

**To browse the data in BigQuery:**
1. Go to [console.cloud.google.com/bigquery](https://console.cloud.google.com/bigquery)
2. Sign in with a Google account
3. In the Explorer panel on the left, search for `flow-battery-data-ingestion`
4. Browse the tables and run queries

**Example query — get average coulombic efficiency by participant:**
```sql
SELECT ParticipantID, Phase, AVG(CoulombicEfficiency_pct) as avg_CE
FROM `flow-battery-data-ingestion.electrochem.charge-discharge-metrics`
GROUP BY ParticipantID, Phase
ORDER BY ParticipantID
```

### Don't want to use Google?
No problem. You can export any table as a CSV file directly from BigQuery by running a query and clicking **Save Results → CSV**. You can then open it in Excel, Python, R, or any other tool you prefer.

Alternatively, you can use the [BigQuery API](https://cloud.google.com/bigquery/docs/reference/rest) to access the data programmatically from any language.

---

## 🖥️ Run the Dashboard on Your Own Computer

You can download and run this dashboard locally on your own computer — useful if you want to explore the data offline, modify the charts, or adapt it for your own research.

### What is Marimo?
The dashboard is built using **Marimo**, an open-source Python tool for building interactive data dashboards. It runs in your web browser but is powered by Python running on your computer. You don't need to know Python to run it — just follow the steps below.

### What is a terminal?
A terminal (also called a command prompt or shell) is a text-based window where you type instructions for your computer. It sounds intimidating but you only need to type a few simple commands — we'll walk you through each one.

---

### Step 1 — Install Python
Python is the programming language the dashboard is written in.

1. Go to [python.org/downloads](https://www.python.org/downloads/)
2. Download the latest version (3.12 or newer)
3. Run the installer
4. **Important:** During installation, tick the box that says **"Add Python to PATH"** before clicking Install

### Step 2 — Install Google Cloud SDK
This lets your computer connect to the BigQuery database.

1. Go to [cloud.google.com/sdk/docs/install](https://cloud.google.com/sdk/docs/install)
2. Download the installer for your operating system (Windows, Mac, or Linux)
3. Run the installer and follow the prompts

### Step 3 — Download the dashboard files
1. Click the green **Code** button at the top of this GitHub page
2. Select **Download ZIP**
3. Extract the ZIP file to a folder on your computer (e.g. your Desktop)

### Step 4 — Open a terminal
**On Windows:**
- Press the `Windows` key, type `PowerShell`, and press Enter

**On Mac:**
- Press `Cmd + Space`, type `Terminal`, and press Enter

### Step 5 — Navigate to the dashboard folder
In the terminal, type `cd` followed by the path to the folder where you extracted the files. For example:

**Windows:**
```
cd C:\Users\YourName\Desktop\flow-battery-dashboard
```
**Mac:**
```
cd ~/Desktop/flow-battery-dashboard
```

### Step 6 — Install the required packages
Type this and press Enter:
```
pip install -r requirements.txt
```
This downloads and installs everything the dashboard needs. It may take a few minutes — you'll see a lot of text scrolling by, that's normal.

### Step 7 — Log in to Google
Type this and press Enter:
```
gcloud auth application-default login
```
A browser window will open — sign in with your Google account. This gives the dashboard permission to read the public dataset. You only need to do this once.

### Step 8 — Run the dashboard
Type this and press Enter:
```
python -m marimo run dashboard.py
```
Then open your web browser and go to:
```
http://localhost:2718
```
The dashboard should appear. It connects to the same live data as the online version.

---

## ✏️ Want to Modify the Dashboard?
If you want to edit the charts or add new features, run this instead of Step 8:
```
python -m marimo edit dashboard.py
```
This opens an editable version in your browser where you can modify any part of the dashboard interactively.

---

## ☁️ Deploying Your Own Online Version
If you want to host your own version of this dashboard online, you can deploy it to Google Cloud Run using Docker. This requires a Google Cloud account. See the [Google Cloud Run documentation](https://cloud.google.com/run/docs) for guidance, or open a GitHub issue and we can help.

---

## 📬 Questions?
If you have any questions about the data, the dashboard, or the study, please open a GitHub issue or contact the study coordinators.
