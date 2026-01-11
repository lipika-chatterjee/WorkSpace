import streamlit as st
import pandas as pd
import sqlite3



connection=sqlite3.connect("bankDB.db")
my_cur=connection.cursor()

def load_data():
    customer=pd.read_csv("customers.csv")
    branch=pd.read_csv("branches.csv")
    account=pd.read_csv("accounts.csv")
    loan=pd.read_csv("loans.csv")
    support=pd.read_csv("support_tickets.csv")
    transcation=pd.read_csv("transactions.csv")
    creditCard=pd.read_json("credit_cards.json")

# query to insert data into tables
    # query to create tables
    my_cur.execute(""" drop table if exists customer """)
    my_cur.execute(""" drop table if exists account """)
    my_cur.execute(""" drop table if exists transactions """)
    my_cur.execute(""" drop table if exists loan """)
    my_cur.execute(""" drop table if exists credit_card """)
    my_cur.execute(""" drop table if exists branch """)
    my_cur.execute(""" drop table if exists support_tickets """)   
    connection.commit() 
    my_cur.execute(""" create table if not exists customer (customer_id varchar(10) not null primary key,name varchar(100),gender char(1)
    ,age INT,city varchar(50),aaccount_type varchar(50),join_date date )""")

    my_cur.execute("""create table if not exists account (cust_id varchar(10) primary key,acc_bal float, last_updated datetime,
              foreign key(cust_id) references customer(customer_id)) """)

    my_cur.execute(""" create table if not exists transactions (txn_id varchar(50) primary key,cust_id varchar(50),
               txn_type varchar(50),amount float,txn_date datetime,status varchar(10),
                foreign key(cust_id) references customer(customer_id))""")
    my_cur.execute(""" create table if not exists loan (loan_id int primary key,cust_id varchar(50),
               account_id int,branch varchar(500),loan_type varchar(50),loan_amount int,interest_rate float,loan_tern_month int
               ,start_date date,end_date date,loan_status varchar(50),
                foreign key(cust_id) references customer(customer_id))""")
    my_cur.execute(""" create table if not exists credit_card (card_id varchar(50) primary key, cust_id varchar(50),
               account_id int, branch varchar(500), card_type varchar(50),card_network varchar(50), credit_limit int,
               current_balance float, issued_date date,expiry_date date,status varchar(50),
                foreign key(cust_id) references customer(customer_id))""")
    my_cur.execute(""" create table if not exists branch (branch_id int primary key,branch_name varchar(100),
               city varchar(50),manager_name varchar(100),Total_Employee int,branch_revenue float,open_date date,
               performance_rating int)""")  
    my_cur.execute(""" create table if not exists support_tickets (ticket_id varchar(50) primary key,cust_id varchar(50),
               account_id VARCHAR(50),loan_id int,branch_name varchar(100),
               issue_category varchar(500),Description varchar(500),Date_opened date,date_closed date,priority varchar(50)
               ,status varchar(50),resolution_details varchar(500),support_agent varchar(100),channel varchar(50)
               ,customer_rating int,
                foreign key(cust_id) references customer(customer_id),
                foreign key(loan_id) references loan(loan_id),
                foreign key(account_id) references account(cust_id),
                foreign key(branch_name) references branch(branch_name))""")    

    for _,row in customer.iterrows(): 
        my_cur.execute(""" insert into customer (customer_id,name,gender,age,city,aaccount_type,join_date)
                values (?,?,?,?,?,?,?) """, (row['customer_id'],row['name'],row['gender'],row['age'],row['city'],
                      row['account_type'],row['join_date'] ))

    for _,row in account.iterrows():
        my_cur.execute(""" insert into account (cust_id,acc_bal,last_updated)
                values (?,?,?) """, (row['customer_id'],row['account_balance'],row['last_updated']))

    for _,row in transcation.iterrows():
        my_cur.execute(""" insert into transactions (txn_id,cust_id,txn_type,amount,txn_date,status)
            values (?,?,?,?,?,?) """, (row['txn_id'],row['customer_id'],row['txn_type'],
                   row['amount'],row['txn_time'],row['status']))
    for _,row in loan.iterrows():
        my_cur.execute(""" insert into loan (loan_id,cust_id,account_id,branch,loan_type,loan_amount,interest_rate,
                loan_tern_month,start_date,end_date,loan_status)
            values (?,?,?,?,?,?,?,?,?,?,?) """, (row['Loan_ID'],row['Customer_ID'],row['Account_ID'],row['Branch'],
                   row['Loan_Type'],row['Loan_Amount'],row['Interest_Rate'],row['Loan_Term_Months'],
                   row['Start_Date'],row['End_Date'],row['Loan_Status']))
    for _,row in creditCard.iterrows():
        my_cur.execute(""" insert into credit_card (card_id,cust_id,account_id,branch,card_type,card_network,
                credit_limit,current_balance,issued_date,expiry_date,status)
            values (?,?,?,?,?,?,?,?,?,?,?) """, (row['Card_ID'],row['Customer_ID'],row['Account_ID'],row['Branch'],
                   row['Card_Type'],row['Card_Network'],row['Credit_Limit'],row['Current_Balance'],
                   row['Issued_Date'],row['Expiry_Date'],row['Status']))
    for _,row in branch.iterrows():
        my_cur.execute(""" insert into branch (branch_id,branch_name,city,manager_name,Total_Employee,
                branch_revenue,open_date,performance_rating)
            values (?,?,?,?,?,?,?,?) """, (row['Branch_ID'],row['Branch_Name'],row['City'],row['Manager_Name'],
                   row['Total_Employees'],row['Branch_Revenue'],row['Opening_Date'],row['Performance_Rating']))
    for _,row in support.iterrows():
        my_cur.execute(""" insert into support_tickets (ticket_id,cust_id,loan_id,account_id,branch_name,
               issue_category,Description,Date_opened,date_closed,priority,status,resolution_details,support_agent,channel,customer_rating)
            values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) """, (row['Ticket_ID'],row['Customer_ID'],row['Loan_ID'],row['Account_ID'],row['Branch_Name'],
                  row['Issue_Category'],row['Description'],row['Date_Opened'],row['Date_Closed'],row['Priority'],
                  row['Status'],row['Resolution_Remarks'],row['Support_Agent'],row['Channel'],row['Customer_Rating']))
    #st.write(my_cur.execute (""" PRAGMA table_info(customer) """))
    connection.commit()
select_val=st.sidebar.radio("Navigation",["Home Page","Information","Filter Data","Crud Operations","Q&A","Reload Data"])
st.markdown(
    """
    <style>
    [data-testid="stSidebar"] {
        background-color: #E8DFFF;
    }
    </style>
    """,
    unsafe_allow_html=True
)
if select_val=="Home Page":
    st.title("Welcome to BluePlanet Bank")
    st.text("At Blue Planet Bank, we combine trusted financial services with a commitment to innovation, security, and environmental responsibility." \
    " Whether you’re saving for the future, growing a business, or managing daily finances, we’re here to support your journey.")
    st.markdown(
    """
    <style>
    .stApp {
        background: linear-gradient(to right, #8360f3, #2ebfe1);
    }
    </style>
    """,
    unsafe_allow_html=True
)


    #check null values
    #st.write(support.isnull().sum())
elif select_val=="Information":
    
    st.title("Details of all the tables")
    select_details=st.selectbox("Select one option to view details",["Customer Details","Account Details","Transactions Details"
                            ,"Loan Details",
                            "Credit Card Details","Branch Details","Support Ticket Details"]) 
    if select_details=="Customer Details":
        df = pd.read_sql_query("SELECT * FROM customer", connection)
        st.dataframe(df, width=1000, height=9000)
    elif select_details=="Account Details":
        df = pd.read_sql_query("SELECT * FROM account", connection)
        st.dataframe(df, width=1000, height=9000)
    elif select_details=="Transactions Details":
        df= pd.read_sql_query("select * FROM transactions", connection)
        st.dataframe(df, width=1000, height=9000)
    elif select_details=="Loan Details":
        df = pd.read_sql_query("SELECT * FROM loan", connection)
        st.dataframe(df, width=1000, height=9000)
    elif select_details=="Credit Card Details":
        df = pd.read_sql_query("SELECT * FROM credit_card", connection)
        st.dataframe(df, width=1000, height=9000)
    elif select_details=="Branch Details":
        df = pd.read_sql_query("SELECT * FROM branch", connection)
        st.dataframe(df, width=1000, height=9000)
    elif select_details=="Support Ticket Details":
        df=pd.read_sql_query("SELECT * FROM support_tickets", connection)
        st.dataframe(df, width=1000, height=9000)
        #st.write(transcation)
    #st.write(my_cur.execute("""desc account """))
    #st.write(my_cur.execute("PRAGMA table_info(account)").fetchall())
elif select_val=="Filter Data":
    st.title("Filter Data from tables")
    select_filter=st.selectbox("Select one option to view details",["Customer Details","Account Details"]) 
    if select_filter=="Customer Details":
        cu_query="select * from Customer where 1=1 "
        cust_id=[row[0] for row in my_cur.execute("SELECT distinct customer_id FROM customer").fetchall()]
        cust_id=[""]+cust_id
        cu_cust_id=st.selectbox("Search by Customer ID",cust_id)

        cu_name_val=[row[0] for row in my_cur.execute(
        "SELECT distinct name FROM customer").fetchall()]
        cu_name=[""]+cu_name_val
        cu_name=st.selectbox("Search by Customer Name",cu_name)
        cu_gender=st.selectbox("Search by Customer Gender",["","M","F"])
        ages = [row[0] for row in my_cur.execute(
               "SELECT DISTINCT age FROM customer").fetchall()] 
        ages = [""] + ages   # first value is NULL
        cu_age = st.selectbox("Search by Customer Age", ages)
        cu_cities = [row[0] for row in my_cur.execute(
        "SELECT distinct city FROM customer").fetchall()]
        cu_cities=[""]+cu_cities
        cu_city=st.selectbox("Search by Customer City",cu_cities)
        cu_account_type=[row [0] for row in my_cur.execute(
            "SELECT distinct aaccount_type FROM customer").fetchall()]
        cu_account_type=[""]+cu_account_type
        cu_account_type=st.selectbox("Search by Account Type",cu_account_type)   
        cu_date_val=st.radio("Have the exact Join Date?",["NO","YES"],horizontal=True)  
        cu_join_date_str=""
        if cu_date_val=="NO":
            cu_join_date=""
        else:
            cu_join_date=st.date_input("Search by Join Date")
            cu_join_date_str=str(cu_join_date)
        if cu_cust_id!="":
            cu_query =cu_query+"" + " AND customer_id = " + "'"+cu_cust_id+"'"
        if cu_name!="":
            cu_query =cu_query+"" + " AND name = " + "'"+cu_name+"'"
        if cu_age!="":
            cu_query =cu_query+"" + " AND age = " + ""+str(cu_age)
        if cu_gender!="":
            cu_query =cu_query+"" + " AND gender = " + "'"+cu_gender+"'"
        if cu_city!="":
            cu_query =cu_query+"" + " AND city = " + "'"+cu_city+"'"
        if cu_account_type!="":
            cu_query =cu_query+"" + " AND aaccount_type = " + "'"+cu_account_type+"'"
        if cu_join_date_str!="":
            cu_query =cu_query+"" + " AND join_date = " + "'"+cu_join_date_str+"'"
        st.dataframe(my_cur.execute(cu_query))
    
    elif select_filter=="Account Details":
        cust_id_val= [x[0] for x in (my_cur.execute("select cust_id from account").fetchall())]
        cust_id_val=[""]+cust_id_val
        ac_acc_id=st.selectbox("Search by customer id",cust_id_val)
        if ac_acc_id:
         st.dataframe(my_cur.execute("""select * from account where cust_id=?""",(ac_acc_id,)))
    
elif select_val=="Crud Operations":
    st.title("Add, Update, Delete Operations")
    select_crud=st.selectbox("Select one option to perform Curd Operations",["Add Customer","Update Customer","Delete Customer"])
    if select_crud=="Add Customer":
        st.subheader("Add Customer Details")
        cust_id=st.text_input("Enter Customer ID")
        cust_name=st.text_input("Enter Customer Name")
        cust_age=int(st.number_input("Enter Customer Age"))
        cust_gender=st.text_input("Enter Customer Gender")
        cust_city=st.text_input("Enter Customer City")
        cust_account_type=st.text_input("Enter Customer Account Type")
        cust_join_date=st.date_input("Enter Customer Join Date")
        if st.button("Add Customer"):
            #exception handling for primary key
            try:
                if not cust_id:
                    st.error("customer id cannot be empty.")
                    st.stop()
                else:
                    my_cur.execute(""" select * from customer where customer_id=? """,(cust_id,))
                    row=my_cur.fetchone()
                    if row:
                        st.error("Customer ID already exists. Please use a different Customer ID.")
                        st.stop()
            except Exception as e:
                st.error(f"An error occurred while checking for existing Customer ID: {e}")
                st.stop()
             #inserting data    
            my_cur.execute(""" insert into customer (customer_id,name,  gender,age,city,aaccount_type,join_date)
                values (?,?,?,?,?,?,?) """, (cust_id,cust_name,cust_gender,cust_age,cust_city,
                      cust_account_type,cust_join_date ))
            connection.commit()
            st.success("Customer Added Successfully")
    elif select_crud=="Update Customer":
        st.subheader("Update Customer Details")
        cust_id=st.text_input("Enter Customer ID to Update")
        if cust_id:
            #cust_name=st.text_input("Enter New Customer Name")
            cust_name=st.text_input("Enter New Customer Name",value=my_cur.execute(""" select name from customer where customer_id=? """,(cust_id,)).fetchone()[0])
            cust_age=st.number_input("Enter New Customer Age",value=my_cur.execute(""" select age from customer where customer_id=? """,(cust_id,)).fetchone()[0])
            cust_gender=st.text_input("Enter New Customer Gender",value=my_cur.execute(""" select gender from customer where customer_id=? """,(cust_id,)).fetchone()[0])
            cust_city=st.text_input("Enter New Customer City",value=my_cur.execute (""" select city from customer where customer_id=? """,(cust_id,)).fetchone()[0])
            cust_account_type=st.text_input("Enter New Customer Account Type",value=my_cur.execute (""" select aaccount_type from customer where customer_id=? """,(cust_id,)).fetchone()[0])
            cust_join_date=st.date_input("Enter New Customer Join Date",value=pd.to_datetime(my_cur.execute (""" select join_date from customer where customer_id=? """,(cust_id,)).fetchone()[0]))
        if st.button("Update Customer"):
            my_cur.execute(""" update customer set name=?, gender=?, age=?, city=?, aaccount_type=?, join_date=?
                where customer_id=? """, (cust_name,cust_gender,cust_age,cust_city,
                      cust_account_type,cust_join_date,cust_id ))
            connection.commit()

            st.success("Customer Updated Successfully")
    elif select_crud=="Delete Customer":
        st.subheader("Delete Customer Details")
        cust_id=st.text_input("Enter Customer ID to Delete")
        if st.button("Delete Customer"):
            my_cur.execute(""" delete from customer where customer_id=? """, (cust_id,))
            connection.commit()
            st.success("Customer Deleted Successfully") 
elif select_val=="Q&A":
    st.title("Question and Answers")
    select_ques=st.selectbox("Select a question",["Q1.How many customers exist per city,and what is the average account balance?",
                                                  "Q2.Which account type(Saving,Current,Loan,ect.)holds the highest total balance?",
                                                  "Q3.Who are the top 10 customers by total account balanace accorss all account types?",
                                                  "Q4.Which customers opened accounts in 2023 with a balance above $10,000?",
                                                  "Q5.Whats is the total transaction volumne (sum of amounts) by transaction type?",
                                                  "Q6.How many failed transactions occurred for each transaction type?",
                                                  "Q7.What is the total number of transactions per transcation type?",
                                                  "Q8.Which accounts have 5 or more high-value transcations aboove 20,000?",
                                                  "Q9.What is the average loan amount and interest rate by loan type (Personal,Auto,Home,etc.)?",
                                                  "Q10.Which customers currently hold more than one active or approved loan?",
                                                  "Q11.Who are the top 5 customers with the highest outstanding (non-closed) loan amounts?",
                                                  "Q12.what is the average loan amount per branch?",
                                                  "Q13.How many customers exist in each age group (e.g. 18-25,26-35, etc.)?",
                                                  "Q14.Which issue categories have the longest average resolution time?",
                                                  "Q15.Which support agents have resolved the most critical tickets with high customer ratings (≥4)?"])
    if select_ques=="Q1.How many customers exist per city,and what is the average account balance?":
        st.write(my_cur.execute(""" select cu.city,avg((ac.acc_bal)) as avgerge_account_balance,count(*) as customer_count from customer cu, account ac
                        where cu.customer_id=ac.cust_id group by cu.city """))
    elif select_ques=="Q2.Which account type(Saving,Current,Loan,ect.)holds the highest total balance?":
        st.write(my_cur.execute(""" select sum(ac.acc_bal) as total_balance,cu.aaccount_type,count(*)
                                from account ac, customer cu where cu.customer_id=ac.cust_id  group by cu.aaccount_type
                                order by total_balance desc limit 1 """))
    elif select_ques=="Q3.Who are the top 10 customers by total account balanace accorss all account types?":
        st.write(my_cur.execute(""" select cu.customer_id,cu.name,sum(ac.acc_bal) as total_balance from customer cu, account ac
                        where cu.customer_id=ac.cust_id group by cu.customer_id,cu.name order by total_balance desc limit 10 """))

    elif select_ques=="Q4.Which customers opened accounts in 2023 with a balance above $10,000?":
        st.write(my_cur.execute(""" select cu.customer_id,cu.name,ac.acc_bal,cu.join_date from customer cu, account ac where
                        cu.customer_id=ac.cust_id and ac.acc_bal>10000 and strftime('%Y',cu.join_date)='2023' """))
    elif select_ques=="Q5.Whats is the total transaction volumne (sum of amounts) by transaction type?":
        st.write(my_cur.execute(""" select txn_type,sum(amount) as total_transaction_volume from transactions
                        group by txn_type """))
    elif select_ques=="Q6.How many failed transactions occurred for each transaction type?":
        st.write(my_cur.execute(""" select txn_type,count(*) as failed_transaction_count from transactions
                        where status='Failed' group by txn_type """))
    elif select_ques=="Q7.What is the total number of transactions per transcation type?":
        st.write(my_cur.execute(""" select txn_type,count(*) as total_transactions from transactions
                        group by txn_type """))
    elif select_ques=="Q8.Which accounts have 5 or more high-value transcations aboove 20,000?":
        st.write(my_cur.execute(""" select cust_id,count(*) from transactions where amount>20000 
                                group by cust_id having count(*)>=5 """))
    elif select_ques=="Q9.What is the average loan amount and interest rate by loan type (Personal,Auto,Home,etc.)?":
        st.write(my_cur.execute(""" select loan_type,avg(loan_amount) as average_loan_amount,avg(interest_rate) as average_interest_rate
                        from loan group by loan_type """))
    elif select_ques=="Q10.Which customers currently hold more than one active or approved loan?":
        st.write(my_cur.execute(""" select cust_id,count(*) as active_loan_count from loan
                        where loan_status in ('Active','Approved') group by cust_id having active_loan_count>1 """))
    elif select_ques=="Q11.Who are the top 5 customers with the highest outstanding (non-closed) loan amounts?":
        st.write(my_cur.execute (""" select ln.cust_id,sum(ln.loan_amount) as outstanding_loan_amount from loan ln          
                     where ln.loan_status!='Closed' group by ln.cust_id order by outstanding_loan_amount desc limit 5 """))
    elif select_ques=="Q12.what is the average loan amount per branch?":
        st.write(my_cur.execute (""" select branch,avg(loan_amount) as average_loan_amount from loan
                     group by branch """))
    elif select_ques=="Q13.How many customers exist in each age group (e.g. 18-25,26-35, etc.)?":
        st.write(my_cur.execute (""" select 
                    CASE 
                        WHEN age BETWEEN 18 AND 25 THEN '18-25'
                        WHEN age BETWEEN 26 AND 35 THEN '26-35'
                        WHEN age BETWEEN 36 AND 45 THEN '36-45'
                        WHEN age BETWEEN 46 AND 55 THEN '46-55'
                        WHEN age BETWEEN 56 AND 65 THEN '56-65'
                        ELSE '66+' 
                    END AS age_group,
                    COUNT(*) AS customer_count
                from customer
                group by age_group """))
    elif select_ques=="Q14.Which issue categories have the longest average resolution time?":
        st.write("and 14")
        st.write(my_cur.execute (""" select issue_category, avg(julianday(date_closed)) - julianday(Date_opened) )
                                  as average_resolution_time
                     from support_tickets where date_closed is not null
                     group by issue_category order by average_resolution_time desc """))
    elif select_ques=="Q15.Which support agents have resolved the most critical tickets with high customer ratings (≥4)?":
        st.write(my_cur.execute (""" select support_agent, count(*) as resolved_critical_tickets
                     from support_tickets
                     where priority='High' and customer_rating>=4
                     group by support_agent order by resolved_critical_tickets desc """))
elif select_val=="Reload Data":
 st.title("Reload Data into Tables")
 st.warning("This will delete all existing data and reload from source files. Are you sure you want to proceed?")    
 if st.button("Reload Data"):   
     load_data() 
     st.success("Data Reloaded Successfully")
   
 

     
                                        


        
