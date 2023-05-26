"""
To run:
1) Go to Anaconda Prompt
2) Set CD to be root directory of project 
3) Type in: streamlit run streamlit.py
    - Will need streamlit installed
    - Other dependencies might be needed
    
"""

import streamlit as st
import subprocess
import sys
import os
from os.path import dirname, normpath
import toml

from src.utils.helpers import user_config_reader

PATH_TO_PACKAGE = os.getcwd()
PATH_TO_CONFIG = "config/userconfig.toml"
CONFIG = user_config_reader(PATH_TO_CONFIG)

###

# Some pre-defined functions - loops through TOML:

def Config_To_Input(toml_section):

    try:
        for i in range(0, len(toml_section)):
            key = list(toml_section.keys())[i]  
            val = list(toml_section.values())[i] 
            if type(val) == bool:
                globals()[f"{key}"] = st.checkbox(label=f"{key}", value=val )
            elif type(val) == int:
                globals()[f"{key}"] = st.number_input(label=f"{key}:", value=val )
            else:
                globals()[f"{key}"] = st.text_input(label=f"{key}:", value=val )
    except:
        return 

def Save_Config_Inputs(toml_section):

    try:

        # ---> Modify the the toml:
        for i in range(0, len(toml_section)):
            key = list(toml_section.keys())[i] 
            val = globals()[f"{key}"]
            toml_section[f'{key}'] = val

    except:
        return

###

st.set_page_config(page_title="Pipeline (__NAME__) - streamlit", layout="wide")

# ---> Set the page UI:
with st.sidebar:

    st.title('Pipeline: __NAME__')
    st.write(os.path.join(PATH_TO_PACKAGE, 'main.py'))

    # ---> Show periods:
    for header in CONFIG.keys():
        st.subheader(f"{header}:")
        Config_To_Input( CONFIG[header] )

    # ---> Save the parameters:
    # Note issue: deletes comments in the config file
    saveConstants = st.button('Save parameters')
    st.markdown("Please click 'Save' if you change any of the parameters to ensure the TOML is up to date and that the run is synced correctly.")

if saveConstants:

    for header in CONFIG.keys():
        Save_Config_Inputs( CONFIG[header] )

    # ---> Overwrite the toml file:
    with open(PATH_TO_CONFIG, "w") as config_file:
        toml.dump(CONFIG, config_file)
        config_file.close()


###

st.subheader("Run pipeline:")
st.markdown("Press 'Run' to run the pipeline with your specified parameters. You will see the status and logs of the pipeline below.")
btnResult = st.button('Run')

# ---> Once button is click, run pipeline:

if btnResult:
    
    # ---> Show parameters running with code:
    st.subheader("Run information:")

    param1, param2 = st.columns(2)
    with param1:
        st.write("Parameters set:")
        st.write(CONFIG)
    with param2:
        st.write("Show something else...")

    # ---> Run the code:
    st.write("Runtime status:")

    # ---> Run main.py and show any errors that occur: 
    with st.spinner('Wait for it...'):
        
        p = subprocess.run([f"{sys.executable}", normpath(PATH_TO_PACKAGE + '/main.py')], capture_output=True)
        if p.returncode != 0:
            st.error(f'Pipeline failed... { p.stderr }')
        else:
            st.success('Pipeline run successfully!')