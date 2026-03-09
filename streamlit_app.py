import streamlit as st
from snowflake.snowpark.functions import col

st.title("Customise Your Smoothie🥤")
st.write("Choose the fruits you want in your custom smoothie")

name_on_order = st.text_input("Name on smoothie:")
if name_on_order:
    st.write("The name on your smoothie will be:", name_on_order)

# Use the Streamlit connection (outside Snowflake) OR get_active_session() (inside Snowflake)
cnx = st.connection("snowflake")          # if running locally/Streamlit Cloud
session = cnx.session()

# If you're running inside Snowflake's Streamlit, use:
# from snowflake.snowpark.context import get_active_session
# session = get_active_session()

# --- Load options ---
sp_df = session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS").select(col("FRUIT_NAME"))
pd_df = sp_df.to_pandas()
st.dataframe(pd_df, use_container_width=True)

fruit_options = pd_df["FRUIT_NAME"].dropna().astype(str).tolist()

ingredients_list = st.multiselect(
    "Choose up to 5 ingredients:",
    fruit_options,
    max_selections=5
)

if ingredients_list:
    ingredients_string = ", ".join(ingredients_list)
    st.write("Your selection:", ingredients_string)

    # Button disabled until a name is present (avoid NOT NULL violations)
    submit = st.button("Submit Order", disabled=(not name_on_order))

    if submit:
        try:
            # Create a one-row Snowpark DF and append to the table
            to_insert = session.create_dataframe(
                [(ingredients_string, name_on_order)],
                schema=["INGREDIENTS", "NAME_ON_ORDER"]
            )
            to_insert.write.mode("append").save_as_table("SMOOTHIES.PUBLIC.ORDERS")
            st.success("Your Smoothie is ordered!", icon="✅")
        except Exception as e:
            # Show query id for deeper debugging in Snowflake UI
            qid = None
            try:
                qid = session.get_last_query_id()
            except Exception:
                pass
            st.error(f"Insert failed. Query ID: {qid or 'n/a'}")
            st.exception(e)
