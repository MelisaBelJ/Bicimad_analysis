from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import gdown
from descargarDatosYear import descargaY
import os
import sys

path = 'DatosBICIMAD'
estaciones = 'Estaciones.json'
pathYear = lambda y: path+f'/BiciMAD_{y}'

#Clase para centralizar las funciones sobre el dataFrame
class Datos():
    def __init__(self, df):
        self.df = df

#para mostrar por pantalla la tabla del dataFrame
#Si noEntero es True entonces solo se muestran los 20 primero en vez de la tabla completa
#Si es False se muestra completa
    def muestra(self, noEntero = True):
        self.df.show(20 if noEntero else self.df.count(), False)
 
#Muestra por pantalla la media, el mínimo y el máximo de los datos   
    def describe(self):
        self.df.describe().show()
  
#devuelve un dataFrame con una columna nombre con los elementos de esa columna
#y otra columna count con el total de estos en el dataframe     
    def cantidadEngrupo(self, nombre):
        return Datos(self.df.groupBy(nombre).count())

 #devuelve un dataframe con los viajes desde o hasta Estacion
    def filtraEstaciones(self, Estacion):
        dfCU = self.df.filter(F.col("Estacion_Salida" ).contains(Estacion) 
                       | F.col("Estacion_Llegada").contains(Estacion))
        return Datos(dfCU)

#devuelve un gráfico hecho con los datos de nombreX en el eje X y los 
# de nombreY en el Y. 
#Si barras es True, el gráfico es de barras
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

#Cambia idunplug_station e idplug_station por el nombre de la estación correspondiente
# bajo el nuevo nombre de "Estacion_Llegada" y "Estacion_Salida"        
    def formateaEstaciones(self):
        #Con los nombres de las estaciones, que se ve mejor :)
        df2 = self.spark.read.json(f'{path}/{estaciones}',  multiLine=True)
        df2 = df2.drop('dock_bikes','free_bases','activate','address','latitude','light','longitude','no_available','number','reservations_count','total_bases')
        df3 = (self.df.join(df2, self.df.idunplug_station ==  df2.id,"inner").withColumnRenamed("name","Estacion_Salida")).drop('id')
        df3 = (df3.join(df2,df3.idplug_station ==  df2.id,"inner").withColumnRenamed("name","Estacion_Llegada")).drop('id')
        df3 = df3.drop('idunplug_station','idplug_station')
        return Datos(df3)

#Extension de datos para la consulta inicial
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
"""

#Para leer los datos de un año y producir los resultados indicados en el readme
def datos(nEst, y):
    pathY = pathYear(y)
    #Descargamos los datos si no están ya
    if not os.path.isdir(path) or not 'Estaciones.json' in os.listdir(path):
        print('Descargando Datos Estaciones, puede tardar')
        url = "https://drive.google.com/drive/folders/1dqnPVK-5qzsJJarUBwj-xcOIWjjUtpBz"
        gdown.download_folder(url, quiet=True, use_cookies=False)
    if not os.path.isdir(pathY):
        print('Descargando Datos Año, puede tardar')
        descargaY(y)

    #Hacemos la consulta para generar el dataframe del año
    consulta = Consulta([f'{pathY}/{item}' for item in os.listdir(pathYear) if item.endswith('.json')])

    print(f'Viajes hecho por cada tipo de usuario, por alguna razón aparecen números 6 y 7 que no están definidos en la documentación oficial, año {y}')
    consulta.cantidadEngrupo('tipo_Usuario').muestra()
    consulta.describe()

    dCU = consulta.formateaEstaciones().filtraEstaciones(nEst)

    print(f'Viajes hechos desde o hasta las estaciones de {nEst} en {y}')
    dCU.muestra()

    print(f'Viajes por grupo de Edad, en todo {y}')
    consulta.cantidadEngrupo('rango_Edad').muestra()

    print(f'Viajes por grupo de Edad, entre los hechos por las estaciones de {nEst} en {y}')
    dE = dCU.cantidadEngrupo('rango_Edad')
    dE.muestra()
    dE.grafico('rango_Edad', 'count')

    print(f'Afluencia por hora, por las estaciones de {nEst} en {y}')
    dH = dCU.cantidadEngrupo('hora')
    dH.muestra()
    dH.grafico('hora', 'count')

    consulta.spark.stop()

#Si se ejecuta sin argumentos da los datos de Sol en 2020
#Si tiene 1 argumento, los de la estación q se ha dado como argumento en 2020
#Si tiene 2 o más, da los datos de la estación q se ha dado en el primer argumento
#En los años de los sucesivos argumentos (del 2 en delante)
if __name__=="__main__":
    l = len(sys.argv)
    nEst = 'Sol' if l<1 else sys.argv[1]
    x = 2
    while l>x:
        y = sys.argv[x]
        if y in {'2017','2018','2019','2020','2021','2022','2023'}:
            datos(nEst, y)
        else:
            print(f'No hay datos para el año {y}')
        x+=1
    if l<2:
        datos(nEst, 2020)
