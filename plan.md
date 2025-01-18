### **Simplified Plan**

#### **1. Streamlit App**
- Create a **Streamlit app** to serve as the user interface.
- Include the following features:
  1. **Project Overview**: Describe what the project does and its components.
  2. **Button to Generate Fake Data**: Allow users to initiate the process of generating synthetic data.
  3. **SQL Dashboard Integration**: Embed or link a Databricks SQL dashboard showing analytics based on updated data.

---

#### **2. Fake Data Generation**
- Use a Python-based solution (e.g., a library) to generate fake data that mimics your real dataset.
- Define the schema for this fake data.
- The process is triggered by Databricks calling an API that creates the fake data.
- Save the generated data to a temporary location (e.g., file storage or directly into Databricks).

---

#### **3. ETL Process in Databricks**
- Build a **SQL-based ETL pipeline** in Databricks to process and load the data:
  - **Extract**: Load the fake data into a staging table in Databricks.
  - **Transform**: Normalize and clean the data into a star schema with fact and dimension tables.
  - **Load**: Update the data warehouse tables to reflect the new data.

---

#### **4. Databricks SQL Dashboard**
- Create a SQL dashboard in Databricks to visualise the data stored in the warehouse.
- Include charts and tables to represent key metrics and analytics.
- Generate a **shareable link** or embed the dashboard into the Streamlit app.

---

#### **5. Integration**
- Link all components together:
  - The **"Generate Fake Data"** button in Streamlit triggers a process in Databricks that calls an API to create fake data and starts the ETL pipeline.
  - Embed or provide a link to the **Databricks SQL dashboard** for users to view updated analytics.

---

### **High-Level Workflow**

1. **User Interaction**:
   - User accesses the Streamlit app.
   - User presses a button to generate fake data, which triggers a process in Databricks.

2. **Backend Process**:
   - Databricks calls an API to generate fake data.
   - The ETL pipeline is triggered, processing the data, normalising it into a star schema, and updating the warehouse.

3. **Dashboard**:
   - Updated analytics are displayed in the Databricks SQL dashboard, either embedded in the Streamlit app or accessible via a link.
