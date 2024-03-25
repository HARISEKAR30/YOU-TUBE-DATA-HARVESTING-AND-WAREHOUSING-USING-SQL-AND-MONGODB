# YOU-TUBE-DATA-HARVESTING-AND-WAREHOUSING-USING-SQL-AND-MONGODB
This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a SQL database, and enables users to search for channel details and join tables to view data in the Streamlit app.

Steps taken to complete the project:

Set up a Streamlit app: Streamlit is a great choice for building data visualization and analysis tools quickly and easily. Use Streamlit to create a simple UI where users can enter a YouTube channel ID, view the channel details, and select channels to migrate to the data warehouse.

Connect to the YouTube API: Need to use the YouTube API to retrieve channel and video data. We can use the Google API client library for Python to make requests to the API.

Store and Clean data : Once we retrieve the data from the YouTube API, store it in a suitable format for temporary storage before migrating to the data warehouse. We can use pandas DataFrames or other in-memory data structures.

Migrate data to a SQL data warehouse: After collecting data for multiple channels, we can migrate it to a SQL data warehouse. We can use a SQL database such as MySQL or PostgreSQL for this.

Query the SQL data warehouse: We can use SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input. We can use a Python SQL library such as SQLAlchemy to interact with the SQL database.

Display data in the Streamlit app: Finally,display the retrieved data in the Streamlit app. We can use Streamlit's data visualization features to create charts and graphs to help users analyze the data.

Libraries Used:
google-api-python-client,
pymongo,
psycopg2,
streamlit,
pandas

Overall, this approach involves building a simple UI with Streamlit, retrieving data from the YouTube API, storing the data SQL as a warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app.
