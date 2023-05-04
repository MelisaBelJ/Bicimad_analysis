# Practica4
## Objetivos
Se necesita tener el modulo gdown de python instalado para que se descarguen los datos la primera vez que se ejecuta.

Centrándose en las estaciones de Puerta del Sol en 2020:

- [X] Ver cantidad de viajes por rangos de Edad en comparación con los generales.
- [X] Ver la afluencia por hora.
- [X] Ver duración media de los viajes.
- [X] Comparar esta estación con los datos generales.
- [X] Comparar cantidad de viajes y edad de los usuarios con los de 2019.

## Obtención de los datos
Para la obtención de los datos de 20201, se ha usado el programa adjunto _practica_4.py_:

```console
User@#PATH#Practica4:~$ python3 practica_4.py Sol 2020
```
En concreto en el documento, se emplean las clases _Consulta_ y _Datos_ de dicho programa.

Se han implementado las funciones en clases para que estuvieran todas centralizadas
y para que el documento final fuera más legible, pudiendo
ver el funcionamiento exacto de cada método empleado en el fichero 
_practica_4.py_ .

Este descarga los datos a través del método _descargaY_ de _descargaDatosYear.py_ que recibe como argumento el año de cuyos datos debe descargar los datos de BiciMAD.

(Para que no tarde mucho en iniciarse _practica_4.py_ es recomendable tener los datos del año a analizar ya descargados y en sus directorios correspondientes, se proporcionan los ejecutables _descargaTodos.bat_ y _descargaTodos.sh_ para descargar los datos de todos los años de golpe en sus carpetas).

La forma en que funciona la descarga de datos es descargando los respectivos archivos Zip de la web en la carpeta y descomprimiéndolos en el directorio correspondiente y borrándolos. Por lo que en el proceso de descarga se verán unos 12 zips (1 por mes) aparecer y desaparecer por año.
