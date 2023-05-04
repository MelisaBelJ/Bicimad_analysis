# Practica4

Se necesita tener el modulo gdown de python instalado para que se descarguen los datos la primera vez que se ejecuta.

Centrándose en las estaciones de Ciudad Universitaria

- [X] Ver rangos de Edad en comparación con los generales en 2020
- [ ] Comparar viajes de 2020 con los de otro año (para ver como afectaron las clases online)
- [ ] Comparar viajes por meses o días

## Obtención de los datos
Para la obtención de los datos, se ha usado el programa adjunto _practica_4.py_ sobre los años tratados.

```console
User@#PATH#Practica4:~$ python3 practica_4.py 2019 2020 2021
```

Este descarga los datos a través del método _descargaY_ de _descargaDatosYear.py_ que recibe como argumento el año de cuyos datos debe descargar los datos de BiciMAD.

(Para que no tarde mucho en iniciarse _practica_4.py_ es recomendable tener los datos del año a analizar ya descargados y en sus directorios correspondientes, se proporcionan los ejecutables _descargaTodos.bat_ y _descargaTodos.sh_ para descargar los datos de todos los años de golpe en sus carpetas).

La forma en que funciona la descarga de datos es descargando los respectivos archivos Zip de la web en la carpeta y descomprimiéndolos en el directorio correspondiente y borrándolos. Por lo que en el proceso de descarga se verán unos 12 zips (1 por mes) aparecer y desaparecer por año.
