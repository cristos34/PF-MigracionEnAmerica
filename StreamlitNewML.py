import numpy as np
#from flask import Flask, request, jsonify, render_template, url_for
import pickle
from sklearn import svm
import streamlit as st
import pandas as pd
from PIL import Image
#Es para quitar el menu de la pagina que sale por defecto y se a compaña con un #---> st.markdown(hide_menu,unsafe_allow_html=True)
hide_menu="""
<style>
#MainMenu {
     visibility:hidden;
}
footer{
     visibility:visible;
     
}
footer:after{
    content:"By Datos Estrategicos, Copyright @ 2023: Streamlit.";
    display:block;
    position:relative;
    color:tomato;
    padding:5px;
    top:3px;
}
</style>
"""
img=Image.open("https://github.com/Adapa22/PF-MigracionEnAmerica/blob/531ff6737abfd1b07f2b18ffbaf24ab2bd58fa6a/DatosEstrategicos.png")

# Path del modelo preentrenado
# MODEL_PATH = 'deployStreamlit\models'


st.set_page_config(
    page_title="Migrantes sin fronteras",
    page_icon=img,
    #page_icon="😎",
    layout="wide"
)

#------------------------------------------------------------------------------
#cargo los datos del modelo de recomendacion
#------------------------------------------------------------------------------
recomendacion=pd.read_csv("Recomendacion.csv",sep=",",index_col=0)

#------------------------------------------------------------------------------
# Cargar los datos en la app
#------------------------------------------------------------------------------
consul=pd.read_csv("DataPlataformas.csv",sep=";")

#-----------------------------------------------------------------------------
# Crear un título para la aplicación centrado con codigo HTML
#------------------------------------------------------------------------------
st.markdown("<h1 style='color:181082;text-align:center;'>MIGRANTES SIN FRONTERAS </h1>",unsafe_allow_html=True)
st.markdown(hide_menu,unsafe_allow_html=True) #--> para quitar el menu

#-----------------------------------------------------------------------------
# Crear una imagen de presentacion y un footer a la imagen
#------------------------------------------------------------------------------
image = Image.open("https://github.com/Adapa22/PF-MigracionEnAmerica/blob/531ff6737abfd1b07f2b18ffbaf24ab2bd58fa6a/portada3.PNG")
st.image(image, caption='Modelos de machine learning')

#-------------------------------------------------------------------------------------------------
# Funciones generales
#-------------------------------------------------------------------------------------------------
def modelo_RecomendacionTitulo(year:int, plataforma:str, duration_type:str):
    #Lectura de la base de datos:
   
    # Verificar que la plataforma sea una de las opciones válidas
    consul1=consul[(consul['release_year']==year) &
       (consul["plataforma"]==plataforma) &
       (consul["duration_type"]==duration_type)].sort_values(by='duration_int', ascending=False).head(1)

    return consul1["title"]

#---------------------------------------------------------------------------------------------------
# Opciones de consulta
#---------------------------------------------------------------------------------------------------
options = ['Inicio','Flujo migratorio por país próximo año', 'Remesas de países próximo año', 'PBI próximo año por país']
query = st.sidebar.selectbox('Seleccione su predicion', options)

#---------------------------------------------------------------------------------------------------
# Nos permite administrar el menu
#---------------------------------------------------------------------------------------------------
# Consulta general del machine learnig: Top 10 de los paises que mas flujo migratorios tienen
if query == 'Inicio':
         st.write('Bienvenido a la aplicación de prediciones basada en los flujos migratorios')
         st.subheader("Top 10, países con más flujo migratorio a otros países")
         st.write(recomendacion[recomendacion["ratings"]>240].sort_values(by="Prome_Raitings",ascending=False).head(10))

# Ejemplo 1
# Consulta 1: Duración máxima
if query == 'Duración máxima':
    st.subheader('Película con mayor duración por año, plataforma y tipo de duracion.')
    year = st.number_input('Año', min_value=2000, max_value=2023, value=2020, step=1)
    platform = st.selectbox('Seleccione una plataforma', ['disney','hulu','netflix',"amazon"])
    duration_type = st.selectbox('Tipo de duración', ['min', 'season'])
    if st.button('Consultar'):
        result = modelo_RecomendacionTitulo(year, platform, duration_type)
        if isinstance(result, str):
            st.write(f'La duración máxima en {duration_type.lower()}s en {year} en {platform} es: {result}.')
        else:
            st.write(result)
   
# Ejemplo 1
if query == 'Modelo de recomendacion':
    st.subheader('Peliculas y series recomendadas')
    titul = st.selectbox("Escoja la pelicula por su nombre",options=recomendacion.apply(lambda  i: i["title"],axis=1))
    #movid = st.selectbox("Escoja la pelicula su identificador",options=recomendacion.apply(lambda  i: i["movieId"],axis=1))
   
    if st.button('Consultar'):
        
           result = modelo_RecomendacionTitulo(titul)
           st.subheader("A otros usuarios tambien les gustaron estas peliculas.")
           st.write(result) 
             

#if __name__ == '__main__':
 #main()


   #link
#para EJECUTAR la api
# https://fastapi-platafomas-streaming.onrender.com/docs
    
#python -m venv venv #--> crear el ambiente virtual desde la terminal
#crear el main.py #--> crear un archivo en el ambinete virtual
#cd
#.\venv\
#Set-ExecutionPolicy -ExecutionPolicy Remotesigned -Scope process
#.\Scripts\
#.\activate
#cd ..
#cd ..
#uvicorn main:app
#uvicorn main:app --reload  #---> para que quede cocorriendo mientras dse programa
#http://localhost:8000
#http://localhost:8000/docs 
#Para hacer del deploy --> renderizar 
#pip list
#pip uninstall pandas,numpy,etc
#uvicorn main:app --host 0.0.0.0 --port 8080  
#environment environment variables--> key:PIP_VERSION   value:22.3.1 #-->version
#pip install --upgrade scikit-learn
#pip freeze > requirements.txt
# Para correr este archivo en local
# streamlit run  StreamlitNewML.py

 
