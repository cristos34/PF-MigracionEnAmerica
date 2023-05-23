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
img=Image.open(r"assets/DatosEstrategicos.png")

# Path del modelo preentrenado
# MODEL_PATH = 'deployStreamlit/models'


st.set_page_config(
    page_title="Prueba Migrantes sin fronteras",
    page_icon=img,
    #page_icon="😎",
    layout="wide"
)

#------------------------------------------------------------------------------
#cargo los datos del modelo de recomendacion
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
# Cargar los datos en la app
#------------------------------------------------------------------------------
VulnerabilidadAmerica=pd.read_csv(r"Prediciones/PredicionesVulnerabilidadAmerica.csv",sep=",")
RemesasAmerica=pd.read_csv(r"Prediciones/PredicionesRemesasAmerica.csv",sep=",")
FlujosMigratoriosAmerica=pd.read_csv(r"Prediciones/PredicionesMigracionNeta.csv",sep=",")
#-----------------------------------------------------------------------------
# Crear un título para la aplicación centrado con codigo HTML
#------------------------------------------------------------------------------
st.markdown("<h1 style='color:181082;text-align:center;'>MIGRANTES SIN FRONTERAS </h1>",unsafe_allow_html=True)
st.markdown(hide_menu,unsafe_allow_html=True) #--> para quitar el menu

#-----------------------------------------------------------------------------
# Crear una imagen de presentacion y un footer a la imagen
#------------------------------------------------------------------------------
image = Image.open(r"assets/portada3.PNG")
st.image(image, caption='Modelos de machine learning')

#-------------------------------------------------------------------------------------------------
# Funciones generales
#-------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------
# Inicio - Funciones Predicción Flujos Migratorios
#-------------------------------------------------------------------------------------------------

def modelo_prediccionFlujosMigratoriosTOP5():

    a=FlujosMigratoriosAmerica.sort_values('prediccion',ascending=False).copy()
    consulMigra=a[['codigopais', 'nombrepais','2018', '2019', '2020','prediccion']].head(5)
   
    return consulMigra

#-------------------------------------------------------------------------------------------------
# Funciones Predicción Flujos Migratorios
#-------------------------------------------------------------------------------------------------
# Predicción de flujos migratorios para el proximo año 
#--------------------------------------------------------------------------------------------------
def modelo_prediccionFlujosMigratoriosPornombre(nombre_pais: str): # year:str, 
    
    consulMigraN=FlujosMigratoriosAmerica["prediccion"][FlujosMigratoriosAmerica["nombrepais"]==nombre_pais]

    return consulMigraN

def modelo_prediccionFlujosMigratoriosPorTOP(top):

    a=FlujosMigratoriosAmerica.sort_values('prediccion',ascending=False).copy()
    consulMigra=a[['codigopais', 'nombrepais','2018', '2019', '2020','prediccion']].head(top)
   
    return consulMigra

#-------------------------------------------------------------------------------------------------
# Funciones Predicción Vulnerabilidad
#-------------------------------------------------------------------------------------------------
def modelo_prediccionVulnerabilidad(nombre_pais: str): # year:str, 
    
    consulVul=VulnerabilidadAmerica["prediccion"][VulnerabilidadAmerica["nombrepais"]==nombre_pais]

    return consulVul
#-------------------------------------------------------------------------------------------------
# Funciones prueba Predicción Vulnerabilidad
#-------------------------------------------------------------------------------------------------

def modelo_PruebaprediccionVulnerabilidad(top): # year:str, 
    
    a=VulnerabilidadAmerica.sort_values('prediccion',ascending=False).copy()
    consulVul=a[['codigopais', 'nombrepais','2018', '2019', '2020','prediccion']].head(top)
   
    return consulVul
#-------------------------------------------------------------------------------------------------
# Funciones Predicción Remesas
#-------------------------------------------------------------------------------------------------
def modelo_prediccionRemesas(nombre_pais: str): # year:str, 
    #Lectura de la base de datos:
   
    # Verificar que la plataforma sea una de las opciones válidas
    consulRe=RemesasAmerica["prediccion"][RemesasAmerica["nombrepais"]==nombre_pais]

    return consulRe

#---------------------------------------------------------------------------------------------------
# Opciones de consulta
#---------------------------------------------------------------------------------------------------
options = ["Inicio","Predicción de flujos migratorios para el próximo año","Predicción de vulnerabilidad migratoria para el próximo año","Predicción de Remesas para el año 2021"]
query = st.sidebar.selectbox('Seleccione su predicción', options)

#---------------------------------------------------------------------------------------------------
# Nos permite administrar el menu
#---------------------------------------------------------------------------------------------------
# Consulta general del machine learnig: Top 10 de los paises que tienen mas flujo migratorios
#-----------------------------------------------------------------------------------------------------
if query == 'Inicio':     
         st.subheader('TOP 5 de los países con mayor flujo migratorio para el próximo año.')
         result = modelo_prediccionFlujosMigratoriosTOP5() 
         st.write(round(result,2))
#---------------------------------------------------------------------------------------------------
# consulMigracion: "Predicción
#  de Migracion Neta para proximo año"
#---------------------------------------------------------------------------------------------------
if query == "Predicción de flujos migratorios para el próximo año":
    st.subheader('Países con mayor flujo migratorio para el próximo año.')
    nombre_pais = st.selectbox('Seleccione el nombre del país', options=FlujosMigratoriosAmerica.apply(lambda  i: i["nombrepais"],axis=1))
    if st.button('Consultar'):

        result = modelo_prediccionFlujosMigratoriosPornombre(nombre_pais)
        
        if isinstance(str(result), str):
             st.write(f'El país llamado {nombre_pais} para el próximo año estima un flujo migratorio de: {result.values[0]}.')
            
        else:
             st.write(result)

    st.subheader('TOP de países con mayor flujo migratorio para el próximo año.')
    top = st.selectbox('Seleccione el número Top de países con mayor flujo migratorio', options=[1,2,3,4,5,6,7,8,9,10])
    result = modelo_prediccionFlujosMigratoriosPorTOP(top)
    st.write(result)


# consulVul: "Predicción de vulnerabilidad 2021"
          
if query == "Predicción de vulnerabilidad migratoria para el próximo año":
    st.subheader('Escoja el país que desee conocer el índice de vulnerabilidad para el próximo año.')
      
    nombre_pais = st.selectbox('Seleccione el nombre del país', options=VulnerabilidadAmerica.apply(lambda  i: i["nombrepais"],axis=1))
    if st.button('Consultar'):
        result = modelo_prediccionVulnerabilidad(nombre_pais)
        
        if isinstance(str(result), str):
             st.write(f'Este es el índice de vulnerabilidad migratoria del país {nombre_pais} para el año 2021 es de: {round(result.values[0])}.')
            
        else:
             st.write(result)

    st.subheader('TOP de países con mayor vulnerabilidad migratoria para el próximo año.')
    top = st.selectbox('Seleccione el número Top de países con mayor vulnerabilidad', options=[1,2,3,4,5,6,7,8,9,10])
    result = modelo_PruebaprediccionVulnerabilidad(top)
    st.write(result)

#---------------------------------------------------------------------------------------------------
# consulRe: "Predicción de Remesas proximo año"
#---------------------------------------------------------------------------------------------------
if query == "Predicción de Remesas para el año 2021":
    st.subheader('Países con mayor recepción de remesas para el próximo año.')
      
    nombre_pais = st.selectbox('Seleccione el nombre del país', options=VulnerabilidadAmerica.apply(lambda  i: i["nombrepais"],axis=1))
   
    if st.button('Consultar'):
        
        result = modelo_prediccionRemesas(nombre_pais)
        
        if isinstance(str(result), str):
             st.write(f'Este es el índice de Remesas migratoria del país {nombre_pais} para el año 2021 es de: {round(result.values[0],2)}.')
        else:
             st.write(round(result,2))
            
            # Aplicar estilos CSS para centrar el contenedor
              


             #streamlit run StreamlitNewML.py
 







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
#streamlit run StreamlitNewML.py

 
