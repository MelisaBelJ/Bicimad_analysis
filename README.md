# Practica4
## Objetivos
Se necesita tener el modulo gdown de python instalado para que se descarguen los datos la primera vez que se ejecuta.

Centrándose en las estaciones de Puerta del Sol entre 2018 y 2020:

- [X] Ver rangos de Edad en comparación con los generales
- [ ] Comparar viajes de 2020 con los de otro año (para ver como afectaron las restricciones).
- [X] Ver la afluencia por hora cada año.
- [X] Ver duración media de los viajes.
- [ ] Comparar esta estación con los datos generales.

## Obtención de los datos
Para la obtención de los datos, se ha usado el programa adjunto _practica_4.py_ sobre los años y estación tratados.

```console
User@#PATH#Practica4:~$ python3 practica_4.py Sol 2018 2019 2020
```

Este descarga los datos a través del método _descargaY_ de _descargaDatosYear.py_ que recibe como argumento el año de cuyos datos debe descargar los datos de BiciMAD.

(Para que no tarde mucho en iniciarse _practica_4.py_ es recomendable tener los datos del año a analizar ya descargados y en sus directorios correspondientes, se proporcionan los ejecutables _descargaTodos.bat_ y _descargaTodos.sh_ para descargar los datos de todos los años de golpe en sus carpetas).

La forma en que funciona la descarga de datos es descargando los respectivos archivos Zip de la web en la carpeta y descomprimiéndolos en el directorio correspondiente y borrándolos. Por lo que en el proceso de descarga se verán unos 12 zips (1 por mes) aparecer y desaparecer por año.

## Resultados
Vamos a hacer una comparativa entre los años 2018 a 2020 del flujo de tráfico por las estaciones de Puerta del Sol.

Cabe esperar que en el año 2020, el flujo disminuyera bastante globalmente.
### Viajes por rango de edad
En primer lugar, veremos el número de viajes por rangos de edad, siendo las divisiones de las mismas según la documentación oficial:
- 0: No se ha podido determinar el rango de edad del usuario
- 1: El usuario tiene entre 0 y 16 años
- 2: El usuario tiene entre 17 y 18 años
- 3: El usuario tiene entre 19 y 26 años
- 4: El usuario tiene entre 27 y 40 años
- 5: El usuario tiene entre 41 y 65 años
- 6: El usuario tiene 66 años o más
#### 2018
...
#### 2019
...
#### 2020
...
### Tráfico por hora
... 
#### 2018
...
#### 2019
...
#### 2020
...
### Duración media de los viajes
...
#### 2018
...
#### 2019
...
#### 2020
...
### Comparación Puerta del Sol con el resto de estaciones
...
#### 2018
...
#### 2019
...
#### 2020
...
### Comparación entre años
...
