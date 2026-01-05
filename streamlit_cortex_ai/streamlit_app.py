#Script: streamlit_app.py
#Description: Create a simple Streamlit interface that accepts a user's prompt
#             ,sends it to a Snowflake Cortex AI_COMPLETE function, and gets a response
import streamlit as st
from snowflake.snowpark.functions import ai_complete
import json

st.title(":material/smart_toy: Hello Cortex!")

# Connect to Snowflake
try:
    # works in Streamlit in Snowflake
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()

except:
    # Works locally and Streamlit Community Cloud
    from snowflake.snowpark import Session
    session = Session.builder.configs(st.secrets["connections"]["snowflake"]).create()

# Model and Prompts
model = "claude-3-5-sonnet"
prompt = st.text_input("Enter your prompt:")

# Run LLM Inference
if st.button("Generate Response"):
    df = session.range(1).select(
        ai_complete(model=model, prompt=prompt).alias("response")
    )

    # Get and display response
    response_raw = df.collect()[0][0]
    response = json.loads(response_raw)
    st.write(response)

# Footer
st.divider()
st.caption("DSD Team - Hackathon - Streamlit Cortex AI Integration Demo")