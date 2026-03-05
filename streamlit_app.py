# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

# Write directly to the app
st.title(f"Customise Your Smoothie🥤")
st.write(
"Choose the fruits you want in your custom smoothie"
)

name_on_order = st.text_input("Name on smoothie:")
st.write("The name on your smoothie will be, ", name_on_order)



# option = st.selectbox(
#     "What is your favourite food?",
#     ("Apple","Banana","Strawberry"),
# )

# st.write("You selected the fruit:", option)

session = get_active_session()
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
st.dataframe(data=my_dataframe, use_container_width=True)

ingredients_list = st.multiselect (
    'Choose up to 5 ingridients:',
    my_dataframe,
    max_selections=5
)

if ingredients_list:
    

    ingredients_string = ' '

    for x in ingredients_list:
        ingredients_string += x
    # st.write(ingridients_string)

     
    my_insert_stmt = """ insert into smoothies.public.orders(ingredients,name_on_order)
            values ('""" + ingredients_string + """','""" + name_on_order + """')"""

    st.write(my_insert_stmt)
    # st.stop()

    time_to_insert = st.button('Submit Order')

    if time_to_insert:
       session.sql(my_insert_stmt).collect()
       st.success('Your Smoothie is ordered!', icon="✅")
