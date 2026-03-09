import streamlit as st
import pandas as pd
import requests
from snowflake.snowpark.functions import col, current_timestamp

st.title("Customise Your Smoothie 🥤")
st.write("Choose the fruits you want in your custom smoothie.")

name_on_order = st.text_input("Name on smoothie:")

# -------------------------------------------------------------------
# Connect to Snowflake via Streamlit's connection (outside SiS)
# -------------------------------------------------------------------
cnx = st.connection("snowflake")
session = cnx.session()

# -------------------------------------------------------------------
# Load FRUIT_OPTIONS with FRUIT_NAME and (optionally) SEARCH_ON
# If SEARCH_ON doesn't exist in the table, we'll add it to the DataFrame.
# -------------------------------------------------------------------
# Try to select both columns; if SEARCH_ON is missing, fall back to FRUIT_NAME only.
try:
    sp_df = session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS").select(
        col("FRUIT_NAME"), col("SEARCH_ON")
    )
except Exception:
    # Column doesn't exist in the table; just select FRUIT_NAME.
    sp_df = session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS").select(col("FRUIT_NAME"))

# Convert to pandas for Streamlit widgets
pd_df = sp_df.to_pandas()

# Ensure FRUIT_NAME is string
pd_df["FRUIT_NAME"] = pd_df["FRUIT_NAME"].astype(str)

# If SEARCH_ON isn't present (or present but has nulls), create/fill it in the DataFrame.
if "SEARCH_ON" not in pd_df.columns:
    pd_df["SEARCH_ON"] = pd_df["FRUIT_NAME"].str.lower().str.strip()
else:
    # Fill any NULL/NaN SEARCH_ON with a lowercase FRUIT_NAME fallback
    pd_df["SEARCH_ON"] = pd_df["SEARCH_ON"].fillna(pd_df["FRUIT_NAME"].str.lower().str.strip()).astype(str)

# Show the source DataFrame (optional, helpful while developing)
st.dataframe(pd_df, use_container_width=True)

# -------------------------------------------------------------------
# Feed the multiselect from FRUIT_NAME (GUI label)
# -------------------------------------------------------------------
fruit_options = pd_df["FRUIT_NAME"].dropna().astype(str).tolist()

ingredients_list = st.multiselect(
    "Choose up to 5 ingredients:",
    fruit_options,
    max_selections=5
)
st.caption(f"Selected {len(ingredients_list)}/5")

# -------------------------------------------------------------------
# If fruits are selected, display nutrition and then allow submit
# -------------------------------------------------------------------
if ingredients_list:
    ingredients_string = ", ".join(ingredients_list)
    st.write("Your selection:", ingredients_string)

    st.subheader("Nutrition Info for Selected Fruits")

    for fruit_chosen in ingredients_list:
        # --- Your requested 'strange-looking' statement using pandas.loc ---
        search_on = pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]
        st.write('The search value for ', fruit_chosen, ' is ', search_on, '.')

        # Call the API using SEARCH_ON (lowercase already ensured)
        url = f"https://my.smoothiefroot.com/api/fruit/{search_on}"
        try:
            resp = requests.get(url, timeout=10)
            if resp.ok:
                st.json(resp.json())
            else:
                st.warning(f"No nutrition data found for {fruit_chosen} (HTTP {resp.status_code}).")
        except Exception as e:
            st.error(f"API error for {fruit_chosen}: {e}")

    # Submit button
    submit = st.button("Submit Order", disabled=(not name_on_order))

    if submit:
        try:
            # ORDER_UID from sequence in SMOOTHIES.PUBLIC
            order_uid = session.sql(
                "SELECT SMOOTHIES.PUBLIC.ORDER_SEQ.NEXTVAL"
            ).collect()[0][0]

            order_filled = False

            # Build Snowpark DF for first 4 columns
            base = session.create_dataframe(
                [(order_uid, order_filled, name_on_order, ingredients_string)],
                schema=["ORDER_UID", "ORDER_FILLED", "NAME_ON_ORDER", "INGREDIENTS"]
            )

            # Add ORDER_TS as server-side LTZ
            df_with_ts = base.with_column(
                "ORDER_TS",
                current_timestamp().cast("timestamp_ltz")
            ).select(
                "ORDER_UID", "ORDER_FILLED", "NAME_ON_ORDER", "INGREDIENTS", "ORDER_TS"
            )

            # Insert
            df_with_ts.write.mode("append").save_as_table("SMOOTHIES.PUBLIC.ORDERS")

            st.success("Your Smoothie is ordered! ✅")
        except Exception as e:
            qid = None
            try:
                qid = session.get_last_query_id()
            except Exception:
                pass
            st.error(f"Insert failed. Query ID: {qid or 'n/a'}")
            st.exception(e)
