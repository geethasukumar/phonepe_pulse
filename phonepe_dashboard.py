import pandas as pd
import mysql.connector
import plotly.express as px
import plotly.graph_objects as go
import plotly.express as px
import json
import streamlit as st

class phonepe_dashboard:
    
    def __init__(self):
        self.db = 'phonepe_data'
        self.host="localhost"
        self.user="root"
        
        
        
    # MySQL DB connect
    def mysql_db_connect(self):
        try:
          
            self.db_mysql_cnx= mysql.connector.connect(
                                host=self.host,
                                user=self.user,
                                database=self.db
                            )
            self.mysql_cursor = self.db_mysql_cnx.cursor()
       
        except:
            print('Error mysql_db_connect - MYSQL DB connection failed!!')
    
     # get all states
    def get_states(self):
     
       
        trans_query = "SELECT distinct(state) FROM agg_trans order by state asc"
        self.mysql_cursor.execute(trans_query)
        
        # Fetch all the rows returned by the query
        trans_rows =  self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        states = [st[0] for st in trans_rows]

        return states
    
     
     # get all year
    def get_year(self):
     
       
        trans_query = "SELECT distinct(year) FROM agg_trans order by year asc"
        self.mysql_cursor.execute(trans_query)
        
        # Fetch all the rows returned by the query
        trans_rows =  self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        yr = [st[0] for st in trans_rows]

        return yr
    
    
     
     # get all modes
    def get_mode(self):
     
       
        trans_query = "SELECT distinct(metric) FROM agg_trans"
        self.mysql_cursor.execute(trans_query)
        
        # Fetch all the rows returned by the query
        trans_rows =  self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        mode = [st[0] for st in trans_rows]

        return mode
    
    
    
    
    # get DF for Transaction data for State and mode selected
    def get_transaction_data(self,state,mode):
     
        
        trans_query = "SELECT count,concat(year,'-Q',quarter) as year  from agg_trans where metric=\'{0}\' and state=\'{1}\' order by year asc".format(mode,state)
        self.mysql_cursor.execute(trans_query)
        
        # Fetch all the rows returned by the query
        trans_rows =  self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        columns = [desc[0].upper() for desc in  self.mysql_cursor.description]
        
        # Create a Pandas DataFrame from the query result
        df = pd.DataFrame(trans_rows, columns=columns)
        return df
        
    # get DF for Transaction amount for State and mode selected
    def get_transaction_amount(self,state,mode):
                    
        
        trans_query = "SELECT amount,year,quarter from agg_trans where metric=\'{0}\' and state=\'{1}\' order by year asc".format(mode,state)
        self.mysql_cursor.execute(trans_query)
        
        # Fetch all the rows returned by the query
        trans_rows = self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        columns = [desc[0].upper() for desc in  self.mysql_cursor.description]
        
        # Create a Pandas DataFrame from the query result
        df = pd.DataFrame(trans_rows, columns=columns)

        return df    
      
        
    # get DF for Transaction amount for State and yr, qtr selected
    def get_district_transaction(self,state,yr,qtr):
        
        trans_query = "SELECT metric as district, count, amount from top_trans where state=\'{0}\' and year={1} and quarter={2} order by year asc".format(state,yr,qtr)
        self.mysql_cursor.execute(trans_query)
        
        # Fetch all the rows returned by the query
        trans_rows = self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        columns = [desc[0].upper() for desc in self.mysql_cursor.description]
        
        # Create a Pandas DataFrame from the query result
        df = pd.DataFrame(trans_rows, columns=columns)
        
        return df   
        
        
    # get DF for Transaction amount for mode and year selected
    def get_yr_transaction(self,mode,yr):
       
        trans_query = "SELECT state, sum(count) count from agg_trans where year={0} and metric=\'{1}\' group by state".format(yr,mode)
        self.mysql_cursor.execute(trans_query)
        
        # Fetch all the rows returned by the query
        trans_rows = self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        columns = [desc[0].upper() for desc in self.mysql_cursor.description]
        
        # Create a Pandas DataFrame from the query result
        df = pd.DataFrame(trans_rows, columns=columns)

        return df        
        
    
         
    # get DF for Transaction amount for mode and year selected
    def get_overall_trans(self):
       
        trans_query = "SELECT sum(count) count, year from agg_trans group by year"
        self.mysql_cursor.execute(trans_query)
        
        # Fetch all the rows returned by the query
        trans_rows = self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        columns = [desc[0].upper() for desc in self.mysql_cursor.description]
        
        # Create a Pandas DataFrame from the query result
        df = pd.DataFrame(trans_rows, columns=columns)

        return df        
        
    
    
    
    # get DF for User data for State and mode selected
    def get_user_data_statewise(self,state):
     
        
        trans_query = "SELECT sum(registeredUsers) registeredusers,sum(appOpens) appopens,year from map_user where state=\'{0}\'  group by year".format(state)
        self.mysql_cursor.execute(trans_query)
        
        # Fetch all the rows returned by the query
        trans_rows =  self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        columns = [desc[0].upper() for desc in  self.mysql_cursor.description]
        
        # Create a Pandas DataFrame from the query result
        df = pd.DataFrame(trans_rows, columns=columns)
       
        return df
        
        
    # get DF for User data for State and mode selected
    def get_user_brand_statewise(self,state,yr):
     
        trans_query = "SELECT metric as brand,sum(count) count from agg_user where state=\'{0}\' and year = {1} group by metric".format(state,yr)
        self.mysql_cursor.execute(trans_query)
        
        # Fetch all the rows returned by the query
        trans_rows =  self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        columns = [desc[0].upper() for desc in  self.mysql_cursor.description]
        
        # Create a Pandas DataFrame from the query result
        df = pd.DataFrame(trans_rows, columns=columns)
       
        return df
        
   
   
    # get DF for User data for State and mode selected
    def get_map_user_statewise(self):
     
        trans_query = "SELECT state, sum(registeredUsers) as 'registered users' from map_user group by state"
        self.mysql_cursor.execute(trans_query)
        
        # Fetch all the rows returned by the query
        trans_rows =  self.mysql_cursor.fetchall()

        # Get the column names from the cursor description
        columns = [desc[0] for desc in  self.mysql_cursor.description]
        
        # Create a Pandas DataFrame from the query result
        df = pd.DataFrame(trans_rows, columns=columns)
        
        df['state'] = df['state'].str.title()
        df['state'] = df['state'].str.replace('-', ' ')   
        df['state'] = df['state'].str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
        df['state'] = df['state'].str.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
        
        df = df.astype({'registered users':'int'})
         
        return df

    
###################### Main Streamlit Application starts here #####################

def main():
    st.title("PhonePe Data Analysis and Visualisation")
    st.subheader("By Geetha Sukumar")
   

    pp_dash = phonepe_dashboard()
    pp_dash.mysql_db_connect()

    m = st.markdown("""
                    <style>
                    div.stButton > button:first-child {
                        background-color: #0A6EBD;
                        color:#ffffff;
                    }


                    </style>""", unsafe_allow_html=True)

    states = pp_dash.get_states()
    yr = pp_dash.get_year()
    st.session_state.pp_dash = pp_dash
    
    mode = pp_dash.get_mode()

    state_selected = st.sidebar.selectbox(
        "Choose a State",
        states
    )

    yr_selected = st.sidebar.selectbox(
        "Choose a Year",
        (yr)
    )
    
    qtr_selected = st.sidebar.selectbox(
        "Choose a Quarter",
        (1,2,3,4)
    )

    mode_selected = st.sidebar.selectbox(
        "Choose a Transaction Mode",
        (mode)
    )
    
    
    
    if st.sidebar.button("Analyse & Visualise"):
        pp_dash.state_selected=state_selected
        pp_dash.mode_selected=mode_selected
        pp_dash.yr_selected=yr_selected
        pp_dash.qtr_selected=qtr_selected
        visualise()
    
    pp_dash.mysql_cursor.close()
    pp_dash.db_mysql_cnx.close() 

def visualise():

    tab1, tab2, tab3, tab4 = st.tabs(["GeoMap", "Transaction Data", "User Data", "Overall Analysis"])
    
    if "pp_dash" in st.session_state:
        pp_dash = st.session_state.pp_dash
     
        with tab1:

            ###### Geomap

            map_user_state_df = pp_dash.get_map_user_statewise()

            # Create scatter map
            map_user_state_df.sort_values(by=['state'])
            geo_regusers_fig = px.choropleth(
                map_user_state_df,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations='state',
                color_continuous_scale='Reds',
                color='registered users',
                title="Insight on PhonePe Registered users across Indian States",
                height=800,
                width=800
            )

            geo_regusers_fig.update_geos(fitbounds="locations", visible=False,)
            geo_regusers_fig.update_layout(
                autosize=False,
                margin = dict(
                        l=20,
                        r=20,
                        b=20,
                        t=20,
                     
                        autoexpand=True
                    ),
                    width=800,
                     height=400,
            )
            st.plotly_chart(geo_regusers_fig, theme="streamlit", use_container_width=True, title="Total Registered users across Indian States")

    

        with tab2:
            title=pp_dash.state_selected.upper().replace('-',' ')
          
            st.subheader(title)
            
            ####### Transaction Count analysis
            ch_title='Total Count of ' + pp_dash.mode_selected + ' Transactions per Year-Quarter'
            trans_count_df = pp_dash.get_transaction_data(pp_dash.state_selected,pp_dash.mode_selected)


            trans_fig = px.bar(trans_count_df, x="YEAR", y="COUNT", color="COUNT",
                        hover_data=['COUNT'], barmode = 'stack', title=ch_title)

            st.plotly_chart(trans_fig, theme="streamlit", use_container_width=True)

           
           ###### Transaction amount
            ch_title='Total Amount of ' + pp_dash.mode_selected + ' Transactions per Year-Quarter'
            trans_amount_df = pp_dash.get_transaction_amount(pp_dash.state_selected,pp_dash.mode_selected)

            trans_amount_fig = px.bar(trans_amount_df, x="YEAR", y="AMOUNT", color="QUARTER",
                        hover_data=['AMOUNT'],  barmode = 'stack',title=ch_title)

            st.plotly_chart(trans_amount_fig)


            ###### District count
            ch_title="District wise all Transactions for Year " + pp_dash.yr_selected + ' Q' +str(pp_dash.qtr_selected)
            dist_count_df = pp_dash.get_district_transaction(pp_dash.state_selected,pp_dash.yr_selected,pp_dash.qtr_selected )

            dist_count_df['DISTRICT'] = dist_count_df['DISTRICT'].str.capitalize()
            dist_fig = px.pie(dist_count_df, names="DISTRICT", values="COUNT", color="COUNT",
                        hover_data=['AMOUNT'], title=ch_title)

            st.plotly_chart(dist_fig)


        with tab3:
            title=pp_dash.state_selected.upper().replace('-',' ')
            st.subheader(title)
            
            ###### User Statewide analysis
            ch_title="Users Data Analysis"
            user_state_df = pp_dash.get_user_data_statewise(pp_dash.state_selected)

            user_state_fig = go.Figure(data=[
                    go.Bar(name='AppOpenings %', y=user_state_df['APPOPENS'], x=user_state_df['YEAR'], marker={'color': 'lightgreen'}),
                    go.Bar(name='Registered Users %', y=user_state_df['REGISTEREDUSERS'], x=user_state_df['YEAR'],marker={'color': 'orange'})
                    ])
            user_state_fig.update_layout(barmode='group', title=ch_title) 


            st.plotly_chart(user_state_fig)


            ###### User Brand Statewide analysis
            ch_title="Brand wise User count for the year "+pp_dash.yr_selected
            user_brand_df = pp_dash.get_user_brand_statewise(pp_dash.state_selected,pp_dash.yr_selected)

            user_brand_fig = go.Figure(data=[go.Pie(labels=user_brand_df['BRAND'], values=user_brand_df['COUNT'],
                                                    hole=.4,textinfo='label+percent',
                                                    texttemplate='%{label}<br>%{percent:1%f}'
                                                    ,insidetextorientation='horizontal',textfont=dict(color='#000000')
                                                    ,marker_colors=px.colors.qualitative.Prism)])
            user_brand_fig.update_layout(title=ch_title) 

            st.plotly_chart(user_brand_fig)


            
        with tab4:
            title="Overall Analysis of the Transactions"
            st.subheader(title)
             ###### Year Transaction analysis
            ch_title="Total Transactions in all States for " +pp_dash.mode_selected +" in the year " + pp_dash.yr_selected
            yr_count_df = pp_dash.get_yr_transaction(pp_dash.mode_selected,pp_dash.yr_selected)

            yr_count_df['STATE'] = yr_count_df['STATE'].str.capitalize()

            yr_fig = px.bar(yr_count_df, x="STATE", y="COUNT", 
                        hover_data=['COUNT'], title=ch_title)

            st.plotly_chart(yr_fig, theme="streamlit", use_container_width=True)
            
            ###### Overall analysis
            ch_title="Total Transaction Count over the Years"
            overall_df = pp_dash.get_overall_trans()
            overall_fig = px.pie(overall_df, names="YEAR", values="COUNT", color_discrete_sequence=px.colors.sequential.RdBu,
                        hover_data=['COUNT'], title=ch_title)
            st.plotly_chart(overall_fig)



########## Calling the Main program
main()


        
       
