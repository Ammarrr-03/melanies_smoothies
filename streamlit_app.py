import streamlit as st
import requests
from snowflake.snowpark.functions import col, current_timestamp

# -------------------------------------------------------------------
# Streamlit App Header
# -------------------------------------------------------------------
st.title("Customise Your Smoothie 🥤")
st.write("Choose the fruits you want in your custom smoothie.")

name_on_order = st.text_input("Name on smoothie:")

# -------------------------------------------------------------------
# Connect to Snowflake using Streamlit connection
# -------------------------------------------------------------------
cnx = st.connection("snowflake")   
session = cnx.session()

# -------------------------------------------------------------------
# Load fruit options from Snowflake
# -------------------------------------------------------------------
sp_df = session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS").select(col("FRUIT_NAME"))
pd_df = sp_df.to_pandas()
st.dataframe(pd_df, use_container_width=True)

# Convert column to Python list
fruit_options = pd_df["FRUIT_NAME"].dropna().astype(str).tolist()

# -------------------------------------------------------------------
# Let users choose up to 5 fruits
# -------------------------------------------------------------------
ingredients_list = st.multiselect(
    "Choose up to 5 ingredients:",
    fruit_options,
    max_selections=5
)
st.caption(f"Selected {len(ingredients_list)}/5")

# -------------------------------------------------------------------
# If the user selected fruits, show nutrition section + submit button
# -------------------------------------------------------------------
if ingredients_list:

    ingredients_string = ", ".join(ingredients_list)
    st.write("Your selection:", ingredients_string)

    # ---------------------------------------------------------------
    # SmoothieFroot API — show nutrition per selected fruit
    # ---------------------------------------------------------------
    st.subheader("Nutrition Info for Selected Fruits")

    for fruit_chosen in ingredients_list:
        api_name = fruit_chosen.lower().strip()
        url = f"https://my.smoothiefroot.com/api/fruit/{api_name}"

        st.markdown(f"### 🥝 {fruit_chosen}")
        try:
            resp = requests.get(url, timeout=10)
            if resp.ok:
                st.json(resp.json())      # pretty JSON output
            else:
                st.warning(f"No nutrition data found for {fruit_chosen}.")
        except Exception as e:
            st.error(f"API error for {fruit_chosen}: {e}")

    # ---------------------------------------------------------------
    # Submit Order button
    # ---------------------------------------------------------------
    submit = st.button("Submit Order", disabled=(not name_on_order))

    if submit:
        try:
            # -------------------------------------------------------
            # Generate ORDER_UID from Snowflake sequence
            # (Ensure ORDER_SEQ exists in SMOOTHIES.PUBLIC)
            # -------------------------------------------------------
            order_uid = session.sql(
                "SELECT SMOOTHIES.PUBLIC.ORDER_SEQ.NEXTVAL"
            ).collect()[0][0]

            order_filled = False  # BOOLEAN

            # -------------------------------------------------------
            # Build Snowpark DataFrame for ORDER insert
            # Insert first 4 columns here
            # -------------------------------------------------------
            base = session.create_dataframe(
                [(order_uid, order_filled, name_on_order, ingredients_string)],
                schema=["ORDER_UID", "ORDER_FILLED", "NAME_ON_ORDER", "INGREDIENTS"]
            )

            # -------------------------------------------------------
            # Add ORDER_TS using server-side LTZ timestamp
            # -------------------------------------------------------
            df_with_ts = base.with_column(
                "ORDER_TS",
                current_timestamp().cast("timestamp_ltz")
            )

            # Ensure column order matches the ORDERS table
            df_with_ts = df_with_ts.select(
                "ORDER_UID",
                "ORDER_FILLED",
                "NAME_ON_ORDER",
                "INGREDIENTS",
                "ORDER_TS"
            )

            # -------------------------------------------------------
            # Write to ORDERS table
            # -------------------------------------------------------
            df_with_ts.write.mode("append").save_as_table("SMOOTHIES.PUBLIC.ORDERS")

            st.success("Your Smoothie is ordered! ✅")

            # -------------------------------------------------------
            # Show the 5 most recent orders
            # -------------------------------------------------------
            recent = session.table("SMOOTHIES.PUBLIC.ORDERS") \
                            .sort(col("ORDER_TS").desc()) \
                            .limit(5) \
                            .to_pandas()

            st.subheader("Recent Orders")
            st.dataframe(recent, use_container_width=True)

        except Exception as e:
            qid = None
            try:
                qid = session.get_last_query_id()
            except Exception:
                pass
            st.error(f"Insert failed. Query ID: {qid or 'n/a'}")
            st.exception(e)
