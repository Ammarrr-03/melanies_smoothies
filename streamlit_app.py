import streamlit as st
from snowflake.snowpark.functions import col, current_timestamp
from uuid import uuid4   # only used if you later want varchar IDs
import requests

# ------------------------------------------------------------
# Streamlit App
# ------------------------------------------------------------

st.title("Customise Your Smoothie 🥤")
st.write("Choose the fruits you want in your custom smoothie.")

name_on_order = st.text_input("Name on smoothie:")

# ------------------------------------------------------------
# Create Snowflake session (outside Snowflake → cnx method)
# ------------------------------------------------------------
cnx = st.connection("snowflake")   # requires secrets.toml
session = cnx.session()

# ------------------------------------------------------------
# Load fruit options
# ------------------------------------------------------------
sp_df = session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS").select(col("FRUIT_NAME"))
pd_df = sp_df.to_pandas()
st.dataframe(pd_df, use_container_width=True)

fruit_options = pd_df["FRUIT_NAME"].dropna().astype(str).tolist()

# ------------------------------------------------------------
# User chooses up to 5 ingredients
# ------------------------------------------------------------
ingredients_list = st.multiselect(
    "Choose up to 5 ingredients:",
    fruit_options,
    max_selections=5
)
st.caption(f"Selected {len(ingredients_list)}/5")

if ingredients_list:
    ingredients_string = ", ".join(ingredients_list)
    st.write("Your selection:", ingredients_string)

    # ------------------------------------------------------------
    # SmoothieFroot API call (correct and properly placed)
    # ------------------------------------------------------------
    st.subheader("Fruit API Response (Watermelon Example)")
    try:
        api_url = "https://my.smoothiefroot.com/api/fruit/watermelon"
        smoothiefroot_response = requests.get(api_url)
        #st.json(smoothiefroot_response.json())
        sf_df = st.dataframe(data=smoothiefroot_response.json(),use_container_width = True)
    except Exception as e:
        st.error(f"API request failed: {e}")

    # ------------------------------------------------------------
    # Submit Order button
    # ------------------------------------------------------------
    submit = st.button("Submit Order", disabled=(not name_on_order))

    if submit:
        try:
            # ------------------------------------------------------------
            # 1. Generate ORDER_UID using Snowflake sequence
            # ------------------------------------------------------------
            order_uid = session.sql(
                "SELECT SMOOTHIES.PUBLIC.ORDER_SEQ.NEXTVAL"
            ).collect()[0][0]

            order_filled = False  # BOOLEAN

            # ------------------------------------------------------------
            # 2. Build Snowpark DataFrame (without ORDER_TS first)
            # ------------------------------------------------------------
            base = session.create_dataframe(
                [(order_uid, order_filled, name_on_order, ingredients_string)],
                schema=["ORDER_UID", "ORDER_FILLED", "NAME_ON_ORDER", "INGREDIENTS"]
            )

            # ------------------------------------------------------------
            # 3. Add ORDER_TS as server-side LTZ timestamp
            # ------------------------------------------------------------
            df_with_ts = base.with_column(
                "ORDER_TS",
                current_timestamp().cast("timestamp_ltz")
            )

            # Ensure column order matches table
            df_with_ts = df_with_ts.select(
                "ORDER_UID", "ORDER_FILLED", "NAME_ON_ORDER", "INGREDIENTS", "ORDER_TS"
            )

            # ------------------------------------------------------------
            # 4. Append into ORDERS table
            # ------------------------------------------------------------
            df_with_ts.write.mode("append").save_as_table("SMOOTHIES.PUBLIC.ORDERS")

            st.success("Your Smoothie is ordered! ✅")

            # ------------------------------------------------------------
            # 5. Show recent orders (optional)
            # ------------------------------------------------------------
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
