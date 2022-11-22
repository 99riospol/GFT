# DataLearningProject

Proyecto creado para apoyar al programa de juniors en su desempeño. La idea de este repositorio es servir como base para que el Junior pueda desarrollarse dentro del area de data. 
A continuación, tenéis una lista de etapas por las que queremos que paséis para ir asentando vuestro conocimiento.

La idea de estos retos es que vayáis asentando conocimientos y podáis evaluar en qué fase estáis de convertiros en Data Engineers. 

# ¿Cómo funciona?

La idea es que cada uno de vosotros, vaya cumpliendo etapas y validando los resultados con vuestro responsable (Javier Briones / Esteban Chiner). Para hacer debéis completar el reto y dejar constancia mediante capturas, logs etc de que lo habéis completado, así como el código necesario en vuestra carpeta del reto.

Por cada reto disponéis de una carpeta con la información necesaria para llevar a cabo el ejercicio. Como nota general, siempre que hablamos de una nueva herramienta nos referimos a crearla dentro de un docker-compose que tendrá que ser el eje principal en cada reto.

# LISTADO DE RETOS


## 0	Crear Rama con tus 4 letras en GitLearning Project

<img src="https://download.logo.wine/logo/GitLab/GitLab-Logo.wine.png" alt="Gitlab" width="200"/>

Esta es vuestra tarea inicial y consiste en crear una rama partiendo de main con vuestras 4 letras, una vez completado hacer un clonado a vuestro ordenador y modificar el README.md con una captura.

## 1 Instalar docker y python

<img src="https://www.docker.com/wp-content/uploads/2022/03/vertical-logo-monochromatic.png" alt="Docker" width="200"/>

Para llevar a cabo este reto, debéis crear un itop solicitando derechos de administrador. 

Link: https://workspace.gft.com/iTop/pages/exec.php/?exec_module=itop-portal-base&exec_page=index.php&portal_id=itop-portal

Type: "I need administrator rights on this computer"


Una vez aceptados, debéis poder instalar docker y python en vuestro ordenador y levantar un contenedor de prueba. Una vez hecho esto adjuntad la captura en el README.md de este reto. 


## 2	Levantar Nifi

<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/f/ff/Apache-nifi-logo.svg/1280px-Apache-nifi-logo.svg.png" alt="Docker" width="200"/>

Para llevar a cabo este reto, debéis levantar Apache nifi con docker y preparar un pipeline que lea de un fichero y lo escriba en otro. Lectura csv y escritura a json, pero por ponerlo más interesante, solo queremos aquellas películas que sean para mayores de 14 años. Para ello debéis utilizar el fichero que hemos dejado dentro de la carpeta del reto. Si no sabes lo que es apache nifi... Google puede ayudarte!!! :-)

## 3	Nifi + API

<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/9/98/The_Simpsons_yellow_logo.svg/1200px-The_Simpsons_yellow_logo.svg.png" alt="API" width="200"/>

Una vez aprendido nifi en el reto anterior, debéis dar un paso más y conseguir consumir la api de los simpsons y generar un fichero csv con una columna por campo. Para ello debéis conseguir que Nifi cada 30 seg haga una consulta a la api y una vez procesado lo concatene en fichero csv.

LINK: https://thesimpsonsquoteapi.glitch.me/quotes


## 4	Nifi + Kafka

<img src="https://m.media-amazon.com/images/I/31jW4UZENjL._AC_.jpg" alt="Kafka" width="200"/>

Ya habéis conseguido procesar datos en algo parecido a tiempo real mediante una API, al menos almacenarlos en un fichero, ¿pero es un fichero el mejor ejemplo para guardar datos? LA respuestas es no, y ahí es donde aparece como primer paso Kafka. Si no sabéis lo que es... ya sabéis id a Google o Youtube. 

Una vez aclaradas vuestras dudas, para superar este reto debéis, levantar un docker-compose con kafka, nifi y kafka-ui y adjuntar una captura de pantalla de Kafka-UI donde los datos que recibis de la api de Simpsons se vean en el topico.

Link: https://github.com/provectus/kafka-ui

## 5	Nifi + Kafka + Python

<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/f/f8/Python_logo_and_wordmark.svg/2560px-Python_logo_and_wordmark.svg.png" alt="Python" width="200"/>

Si habéis llegado hasta aquí! Felicidades! ya tenéis media arquitectura completa, los datos están llegando de manera continua y se están guardando en una cola de mensajes de Kafka ¡Great Job!. Ahora viene el siguiente paso... vamos a leer los datos de ese tópico usando python. Para ello debéis crear un script de python que lea de kafka e imprima por pantalla los mensajes que le lleguen. Al final de este reto, deberías tener un ciclo donde veas como llegan mensajes a tu pantalla de manera continua.

Extra: Si este reto, te ha sabido a poco... puedes probar a dockerizar el script y meterlo como un contenedor en el docker-compose.

## 6	Nifi + Kafka + Python + Base de datos

<img src="https://1000marcas.net/wp-content/uploads/2021/06/PostgreSQL-Logo.png" alt="PostgreSQL" width="200"/>

Una vez has conseguido ver los mensajes... ¿Estaría bien guardarlos en algun sitio no? No queremos que se pierdan. Para ello deberás levantar una base de datos de PostgreSQL y crear usuario, esquema, tabla... para albergar los datos de los mensajes. Como idea... quizá con una tabla sola no te valga no? (Personajes, Cita...). Para visualizar la base de datos quizá esto te ayude... "pgadmin".

Una vez tengas esto, realiza una captura de pantalla de una consulta SQL donde sea cuantos mensajes tienes por cada personaje y ya lo tendrás hecho!

## 7	Nifi + Kafka + Python + Base de datos NoSQL

<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/9/93/MongoDB_Logo.svg/2560px-MongoDB_Logo.svg.png" alt="MongoDB" width="200"/>

Genial!! Ya has conseguido almacenar los datos en una base de datos relacional, pero la vida no es siempre tan estructurada :-) ahora el reto es que adaptando el mismo script de python ahora puedas escribir en mongodb. Para ello, tendrás que levantarlo en el docker-compose y conseguir tener dos clientes cada uno escribiendo en una base de datos distinta ¿Te atreves?


## 8	Nifi + Kafka + Python Notebook + BD

<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/3/38/Jupyter_logo.svg/1200px-Jupyter_logo.svg.png" alt="Jupyter" width="200"/>

El Visual Studio es una herramienta clave para el desarrollo de código, pero hay veces que nos encontramos en escenarios dónde necesitamos herramientas más enfocadas a análisis de datos que llamamos notebooks. En este caso, el reto consiste en levantar un notebook de Jupyter y ejecutar el script que has creado en pasos anteriores, así como realizar desde python consultas a las bases de datos creadas. 


## 9	Nifi + Kafka + PySpark Notebook + BD
## 10	Nifi + Kafka + Spark Job + BD
## 11	Nifi + Kafka + Flink Job
## 12	Nifi + Kafka + Flink Job + BD
## 13	Nifi + Kafka + Python + Base de datos NoSQL
## 14	Nifi + Pub/Sub + DataFlow + BigQuery










