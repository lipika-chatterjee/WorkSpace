import streamlit as st
import pandas as pd
import sqlite3



connection=sqlite3.connect("bankDB.db")
my_cur=connection.cursor()
my_cur.execute("""select * from customer """)

