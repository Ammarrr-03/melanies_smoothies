# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col

st.title("Customise Your Smoothie🥤")
st.write("Choose the fruits you want in your custom smoothie")

name_on_order = st.text_input("Name on smoothie:")
if name_on_order:
    st.write("The name on your smoothie will be:", name_on_order)

# Use Streamlit connection (requires secrets configured under [connections.snowflake])
cnx = st.connection("snowflake")
session = cnx.session()

# Load fruit options from Snowflake using Snowpark, then convert to pandas for display & widgets
sp_df = session.table("smoothies.public.fruit_options").select(col("FRUIT_NAME"))
pd_df = sp_df.to_pandas()  # Convert for Streamlit UI
st.dataframe(pd_df, use_container_width=True)

# st.multiselect needs a list of strings
fruit_options = pd_df["FRUIT_NAME"].dropna().astype(str).tolist()

ingredients_list = st.multiselect(
    "Choose up to 5 ingredients:",
    fruit_options,
    max_selections=5
)

if ingredients_list:
    # Nice, comma-separated string
    ingredients_string = ", ".join(ingredients_list)

    st.write("Your selection:", ingredients_string)

    # Build INSERT. Prefer parameterization to avoid SQL injection
    # Using Snowflake Snowpark SQL with bindings
    my_insert_stmt = """
        INSERT INTO smoothies.public.orders (ingredients, name_on_order)
        SELECT :ingredients, :name
    """

    # Enable order button only when a name is provided
    time_to_insert = st.button("Submit Order", disabled=(not name_on_order))

    if time_to_insert:
        session.sql(my_insert_stmt, {
            "ingredients": ingredients_string,
            "name": name_on_order
        }).collect()
        st.success("Your Smoothie is ordered!", icon="✅")
