from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType
import json

# Datos de prueba para el fichero JSON
data = [
    {
        "type": "Cron",
        "displayName": "Cron",
        "description": "Triggers periodically based on a specified CRON expression.",
        "category": "Timers",
        "properties": [
            {
                "name": "CronExpression",
                "type": "System.String",
                "isReadOnly": False,
                "isBrowsable": True
            }
        ],
        "inputProperties": [
            {
                "name": "CronExpression",
                "type": "System.String",
                "label": "Cron Expression"
            }
        ]
    },
    {
        "type": "HTTP",
        "displayName": "HTTP Trigger",
        "description": "Triggers an action based on an HTTP request.",
        "category": "Webhooks",
        "properties": [
            {
                "name": "RequestURL",
                "type": "System.String",
                "isReadOnly": False,
                "isBrowsable": True,
                "defaultValue": "* * * *"
            },
            {
                "name": "Method",
                "type": "System.String",
                "isReadOnly": False,
                "isBrowsable": True,
                "allowedValues": ["GET", "POST", "PUT", "DELETE"]
            }
        ],
        "inputProperties": [
            {
                "name": "RequestURL",
                "type": "System.String",
                "label": "Request URL",
                "defaultValue": "http://example.com"
            },
            {
                "name": "Method",
                "type": "System.String",
                "label": "HTTP Method",
                "defaultValue": "GET"
            }
        ]
    },
    {
        "type": "FileWatcher",
        "displayName": "File Watcher",
        "description": "Triggers when a file is created or modified in a specified directory.",
        "category": "Filesystem",
        "properties": [
            {
                "name": "DirectoryPath",
                "type": "System.String",
                "isReadOnly": False,
                "isBrowsable": True
            },
            {
                "name": "Filter",
                "type": "System.String",
                "isReadOnly": False,
                "isBrowsable": True,
                "defaultValue": "*.txt"
            }
        ],
        "inputProperties": [
            {
                "name": "DirectoryPath",
                "type": "System.String",
                "label": "Directory Path",
                "defaultValue": "/path/to/directory"
            },
            {
                "name": "Filter",
                "type": "System.String",
                "label": "File Filter",
                "defaultValue": "*.txt"
            }
        ]
    }
]

# Convertir los datos a una cadena JSON
json_data = json.dumps(data, indent=4)

# Crear fichero json con datos de prueba
with open('datosJson.json', 'w') as json_file:
    json_file.write(json_data)

# Crear una sesión de Spark
spark = SparkSession.builder.appName("pruebaN5").getOrCreate()

# a) Leer el archivo y almacenar los datos en un dataframe
json_path = "datosJson.json"
df = spark.read.json(json_path, multiLine=True)

# b) Determinar el esquema del archivo
df.printSchema()

# c) Desarrollar método/función o transformaciones necesarias para aplanar el json
def aplanar_json(df):
    # Identificar campos complejos (Listas y Estructuras) en el esquema
    columnas_datos_complejos = dict([(c.name, c.dataType)
                             for c in df.schema.fields
                             if isinstance(c.dataType, (ArrayType, StructType))])
    # Continuar aplanando mientras existan campos complejos
    while len(columnas_datos_complejos) != 0:
        nombre_columna = list(columnas_datos_complejos.keys())[0] 
        # Si el tipo es StructType, convertir todos los subelementos a columnas planas
        if isinstance(columnas_datos_complejos[nombre_columna], StructType):
            columnas_aplanadas = [col(f"{nombre_columna}.{k}").alias(f"{nombre_columna}_{k}") 
                                   for k in [n.name for n in columnas_datos_complejos[nombre_columna].fields]]
            df = df.select("*", *columnas_aplanadas).drop(nombre_columna)
        
        # Si el tipo es ArrayType, agregar los elementos del array como filas usando la función explode_outer
        elif isinstance(columnas_datos_complejos[nombre_columna], ArrayType):    
            df = df.withColumn(nombre_columna, explode_outer(col(nombre_columna)))
        
        # Verificar si aún existen campos complejos restantes en el esquema
        columnas_datos_complejos = dict([(c.name, c.dataType)
                                 for c in df.schema.fields
                                 if isinstance(c.dataType, (ArrayType, StructType))])
    return df

# Aplicar la función de aplanamiento
df_aplanado = aplanar_json(df)

# Mostrar el esquema del DataFrame aplanado
df_aplanado.printSchema()

# d) Guardar el DataFrame resultante en formato Parquet. El archivo se guarda en local
ruta_parquet = "datosParque.parquet"  
df_aplanado.write.mode("overwrite").parquet(ruta_parquet)

# Mostrar una muestra del DataFrame final aplanado
df_aplanado.show()