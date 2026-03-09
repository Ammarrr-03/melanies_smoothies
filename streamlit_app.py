import streamlit as st
from snowflake.snowpark.functions import col, lit, current_timestamp

st.title("Customise Your Smoothie🥤")
st.write("Choose the fruits you want in your custom smoothie")

name_on_order = st.text_input("Name on smoothie:")

# Create session via st.connection (outside Snowflake)
cnx = st.connection("snowflake")
session = cnx.session()

# Load fruit options
sp_df = session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS").select(col("FRUIT_NAME"))
pd_df = sp_df.to_pandas()
st.dataframe(pd_df, use_container_width=True)

fruit_options = pd_df["FRUIT_NAME"].dropna().astype(str).tolist()

ingredients_list = st.multiselect(
    "Choose up to 5 ingredients:",
    fruit_options,
    max_selections=5
)
st.caption(f"Selected {len(ingredients_list)}/5")

if ingredients_list:
    ingredients_string = ", ".join(ingredients_list)
    st.write("Your selection:", ingredients_string)

    submit = st.button("Submit Order", disabled=(not name_on_order))

    if submit:
        try:
            from uuid import uuid4

            order_uid = str(uuid4())      # assumes ORDER_UID is VARCHAR
            order_filled = False          # BOOLEAN

            # Build DF for first 4 columns
            base = session.create_dataframe(
                [(order_uid, order_filled, name_on_order, ingredients_string)],
                schema=["ORDER_UID", "ORDER_FILLED", "NAME_ON_ORDER", "INGREDIENTS"]
            )

            # Add ORDER_TS as server-side LTZ
            df_with_ts = base.with_column(
                "ORDER_TS",
                current_timestamp().cast("timestamp_ltz")
            )

            # Ensure column order matches the target table
            df_with_ts = df_with_ts.select(
                "ORDER_UID", "ORDER_FILLED", "NAME_ON_ORDER", "INGREDIENTS", "ORDER_TS"
            )

            df_with_ts.write.mode("append").save_as_table("SMOOTHIES.PUBLIC.ORDERS")
            st.success("Your Smoothie is ordered!", icon="✅")

        except Exception as e:
            qid = None
            try:
                qid = session.get_last_query_id()
            except Exception:
                pass
            st.error(f"Insert failed. Query ID: {qid or 'n/a'}")
            st.exception(e)
