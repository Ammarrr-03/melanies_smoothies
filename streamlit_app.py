import streamlit as st
from snowflake.snowpark.functions import col
from uuid import uuid4
from datetime import datetime, timezone

st.title("Customise Your Smoothie🥤")
st.write("Choose the fruits you want in your custom smoothie")

name_on_order = st.text_input("Name on smoothie:")

# ✅ Outside Snowflake (local / Streamlit Cloud): use st.connection
cnx = st.connection("snowflake")   # requires [connections.snowflake] in .streamlit/secrets.toml
session = cnx.session()

# --- Load fruit options from Snowflake with Snowpark, convert to pandas for Streamlit UI ---
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

    submit = st.button("Submit Order", disabled=(not name_on_order))

    if submit:
        try:
            # ❗ ORDERS table columns: ORDER_UID, ORDER_FILLED, NAME_ON_ORDER, INGREDIENTS, ORDER_TS
            # Provide all 5 values explicitly
            order_uid = str(uuid4())                # assumes ORDER_UID is VARCHAR (if NUMBER via sequence, see SQL option below)
            order_filled = False                    # assumes BOOLEAN
            order_ts = datetime.now(timezone.utc)   # TIMESTAMP_TZ/NTZ is fine

            to_insert = session.create_dataframe(
                [(order_uid, order_filled, name_on_order, ingredients_string, order_ts)],
                schema=["ORDER_UID", "ORDER_FILLED", "NAME_ON_ORDER", "INGREDIENTS", "ORDER_TS"],
            )
            to_insert.write.mode("append").save_as_table("SMOOTHIES.PUBLIC.ORDERS")

            st.success("Your Smoothie is ordered!", icon="✅")
        except Exception as e:
            # Help yourself debug in Snowflake → History
            qid = None
            try:
                qid = session.get_last_query_id()
            except Exception:
                pass
            st.error(f"Insert failed. Query ID: {qid or 'n/a'}")
            st.exception(e)
