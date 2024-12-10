Globant’s Data Engineering Coding Challenge
Este repositorio contiene dos soluciones para el desafío técnico, una aplicación interactiva con Streamlit y un pipeline automatizado con Snowflake y AWS S3.

Solución 1: Aplicación Interactiva con Streamlit
Una aplicación en Streamlit que permite:
Cargar datos desde archivos CSV (departments, jobs, hired_employees).
Validar y cargar datos en una base de datos Snowflake.
Generar reportes basados en:
* Empleados contratados por trimestre en 2021.
* Departamentos con contrataciones superiores al promedio en 2021.

Instrucciones
Clonar el repositorio:
bash
Copy code
git clone https://github.com/usuario/repo.git
Instalar dependencias:
bash
Copy code
pip install -r requirements.txt
Ejecutar:
bash
Copy code
streamlit run app.py
Solución 2: Pipeline Automatizado con Snowflake y AWS S3
Descripción
Pipeline escalable que:

Procesa datos desde un bucket S3 usando stages en Snowflake.
Separa los datos en esquemas raw (datos crudos) y target (datos procesados).
Usa tareas programadas y MERGE para cargar nuevos registros evitando duplicados.
Instrucciones
Configurar un bucket S3 con los CSV.
Configurar Snowflake:
Ejecutar el script snowflake_pipeline.sql para crear tablas, tareas y stages.
Activar la tarea programada:
sql
Copy code
ALTER TASK company_data.raw.process_hired_employees RESUME;
Comparación Rápida
Aspecto	Streamlit	Snowflake + AWS S3
Interacción	Interfaz gráfica	Automatización sin intervención manual
Carga de datos	Manual desde la app	Archivos en S3 procesados automáticamente
Escalabilidad	Limitada	Altamente escalable
Uso ideal	Demo interactiva	Producción y grandes volúmenes de datos
Elección según Caso de Uso
Streamlit: Ideal para pruebas rápidas y demostraciones.
Snowflake + S3: Mejor opción para un entorno de producción.
Autor: [Tu Nombre]
Contacto: [Correo Electrónico]
