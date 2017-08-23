# Proyecto Final
## Sentiment Analysis en Twitter.

El proyecto consiste en reproducir, utilizando Spark, el estudio realizado en la tesis de grado que se encuentra en este directorio.

Los alumnos de posgrado tendrán que agregar alguna mejora. Pueden elegir una y/o basarse en los trabajos futuros que propone la tesis.  

### Objetivos

El proyecto no es pautado de forma tal que los pasos para realizarlo no estarán explicitados.
La idea es que, tomando la tesis como hoja de ruta, se decida una arquitectura del sistema y se realicen los programas que la implementen.

Se contará con los docentes para consultar cualquier duda al respecto y se harán recomendaciones para llevar a cabo la tarea.

Para aprobar el proyecto  se deberá realizar un coloquio (de 45 minutos como máximo) en el cual se realizará una presentación del trabajo realizado junto con una demo de la implementación. 


#### Recomendaciones

* Se recomienda seguir en orden las etapas (pipeline) que propone la tesis.
* Comenzar implementando las etapas con los algoritmos que ya brinda Spark, aunque sean distintos a los de la tesis.
* Para comenzar la primera etapa (recolección de datos y su pre-procesamiento) se sugiere computar las estadísticas sobre los tweets (que se explican en la sección 2.1 de la tesis) en Spark, y ver si los resultados son coinciden. 
* Para hacer esta etapa (tokenizador, vectorizacion, df, tf, etc.) en Spark ver [Extracting, transforming and selecting features](http://spark.apache.org/docs/latest/ml-features.html).
* Ver si en la tesis se tokeniza a lowercase
* (continuará)

### Datos para sentiment análisis
* En carpeta [proyecto](posgrado_optativa/labs/jupiterace/bin/zeppelins/doc/proyecto)
    - [tweets de los premios Oscar](posgrado_optativa/labs/jupiterace/bin/zeppelins/doc/proyecto/oscars_dataset.tar.gz)
    - [movie review polaridad](posgrado_optativa/labs/jupiterace/bin/zeppelins/doc/proyecto/review_polarity.tar.gz)
      que viene de: http://www.cs.cornell.edu/people/pabo/movie-review-data/
      por el paper: http://www.cs.cornell.edu/home/llee/papers/sentiment.pdf

### Otros materiales de ayuda

* [Repo](https://github.com/fuxes/twitter-sentiment-clustering) con código de la tesis
* Ejemplo [Twitter Streaming Language Classifier](https://databricks.gitbooks.io/databricks-spark-reference-applications/content/twitter_classifier/index.html) de Databricks
* [Script](https://github.com/fuxes/twitter-sentiment-clustering/blob/master/server/spider.py) para extaer información de Twitter con [Tweepy](http://www.tweepy.org/)


