from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql import functions as F
import matplotlib.pyplot as plt

class Datos():
    def __init__(self, df):
        self.df = df

    def muestra(self, noEntero = True):
        self.df.show(20 if noEntero else self.df.count(), False)
    
    def describe(self):
        self.df.describe().show()
        
    def cantidadEngrupo(self, nombre):
        return Datos(self.df.groupBy(nombre).count())
        
   #Nos quedamos con los viajes desde o hasta una de las estaciones
    def filtraEstaciones(self, Estacion):
        dfCU = self.df.filter(F.col("Estacion_Salida" ).contains(Estacion) 
                       | F.col("Estacion_Llegada").contains(Estacion))
        return Datos(dfCU)

    def barras(self, nombreX, nombreY):
        y = [val for val in self.df.select(nombreY).collect()]
        x = [val for val in self.df.select(nombreX).collect()]

        plt.bar(x, y)

        plt.ylabel(nombreY)
        plt.xlabel(nombreX)
        plt.title(f'{nombreX} vs. {nombreY}')
        plt.legend([nombreX], loc='upper left')

        plt.show()

    def grafico(self, nombreX, nombreY, barras=True):
        x,y=[],[]
        for vx,vy in self.df.select(nombreX, nombreY).collect():
            y.append(vy)
            x.append(vx)
        if barras:
            plt.bar(x, y)
        else:
            plt.plot(x, y)

        plt.ylabel(nombreY)
        plt.xlabel(nombreX)
        plt.title(f'{nombreX} vs. {nombreY}')
        plt.legend([nombreX], loc='upper left')

        plt.show()
        
    def formateaEstaciones(self):
        #Con los nombres de las estaciones, que se ve mejor :)
        df2 = self.spark.read.json('DatosBICIMAD/Estaciones.json',  multiLine=True)
        df2 = df2.drop('dock_bikes','free_bases','activate','address','latitude','light','longitude','no_available','number','reservations_count','total_bases')
        df3 = (self.df.join(df2, self.df.idunplug_station ==  df2.id,"inner").withColumnRenamed("name","Estacion_Salida")).drop('id')
        df3 = (df3.join(df2,df3.idplug_station ==  df2.id,"inner").withColumnRenamed("name","Estacion_Llegada")).drop('id')
        df3 = df3.drop('idunplug_station','idplug_station')
        return Datos(df3)

class Consulta(Datos):    
    def __init__(self, nombres):
        self.spark = SparkSession.builder.getOrCreate()        
        schema = StructType()\
            .add('_id', StructType().add('$oid', StringType(), False), False)\
            .add("travel_time", DoubleType(), False)\
            .add("user_type", IntegerType(), False)\
            .add("idunplug_station", IntegerType(), False)\
            .add("idplug_station", IntegerType(), False)\
            .add("ageRange", IntegerType(), False)\
            .add("unplug_hourTime", TimestampType(), False)
        df = self.spark.read.json(nombres, schema=schema)

        #Pasamos el tiempo a minutos, para que sea más legible 
        df = df.filter(df["travel_time"]>0).withColumn("travel_time", F.round((F.col("travel_time")/60),2))
        #Cambiamos los nombre por comodidad
        df = df.withColumnRenamed("user_type", "tipo_Usuario").withColumnRenamed("ageRange", "rango_Edad").\
            withColumnRenamed("travel_time", "Duracion")

        #Dividimos el tiempo del viaje en dia y hora
        df = df.withColumn('Dia', F.to_date(df.unplug_hourTime))
        df = df.withColumn('Hora', F.hour(df.unplug_hourTime)).drop('unplug_hourTime')
        self.df = df
"""
tipoUsuario: Número que indica el tipo de usuario que ha realizado el movimiento. Sus
posibles valores son:
0: No se ha podido determinar el tipo de usuario
1: Usuario anual (poseedor de un pase anual)
2: Usuario ocasional
3: Trabajador de la empresa
6, 7: ¿? 

rangoEdad: Número que indica el rango de edad del usuario que ha realizado el
movimiento. Sus posibles valores son:
0: No se ha podido determinar el rango de edad del usuario
1: El usuario tiene entre 0 y 16 años
2: El usuario tiene entre 17 y 18 años
3: El usuario tiene entre 19 y 26 años
4: El usuario tiene entre 27 y 40 años
5: El usuario tiene entre 41 y 65 años
6: El usuario tiene 66 años o más
"""

import os
#Para descargar los datos la primera vez que lo usamos (Tarda un rato...)
if not os.path.isdir('DatosBICIMAD'):
    print('Descargando Datos, puede tardar')
    import gdown
    url = "https://drive.google.com/drive/folders/1dqnPVK-5qzsJJarUBwj-xcOIWjjUtpBz"
    gdown.download_folder(url, quiet=True, use_cookies=False)
             
#Leemos todos los ficheros json de 2020
year = 2020
nombreArchivo = lambda y, x: f'DatosBICIMAD/BiciMAD_{y}/{y}{x if x>9 else (f"0{x}")}_movements.json'
consulta = Consulta([nombreArchivo(year, i) for i in range(1,13)])

print('Viajes hecho por cada tipo de usuario, por alguna razón aparecen números 6 y 7 que no están definidos en la documentación oficial')
consulta.cantidadEngrupo('tipo_Usuario').muestra()
consulta.describe()

dCU = consulta.formateaEstaciones().filtraEstaciones("Ciudad Universitaria")

print('Viajes hechos desde o hasta las estaciones de Ciudad Universitaria en 2020')
dCU.muestra(False)

print('Viajes por grupo de Edad, en todo 2020')
consulta.cantidadEngrupo('rango_Edad').muestra()

print('Viajes por grupo de Edad, entre los hechos por las estaciones de Ciudad Universitaria en 2020')
dE = dCU.cantidadEngrupo('rango_Edad')
dE.muestra()
dE.grafico('rango_Edad', 'count')
consulta.spark.stop()