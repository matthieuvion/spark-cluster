### How to : deploy a local Spark cluster (standalone) w/Docker (Linux)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![made-with-python](https://img.shields.io/badge/Made%20with-Python-1f425f.svg)](https://www.python.org/)

> Dockerized env : [JupyterLab server => Spark (master <-> 1 worker) ]

Deploying a local spark *cluster* (standalone) can be tricky  
Most of online ressources focus on single driver installation w/ Spark in a custom env or using [jupyter-docker-stacks](https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html)  
Here my notes to work with Spark locally, with a Jupyter Labs interface, with one Master and one Worker, using Docker Compose.  
All the PySpark dependencies already configured in a container, access to your local files (in an existing directory)  
You might also want to do it the easy way --not local though, using [Databricks](https://docs.databricks.com/getting-started/community-edition.html) community (free)

### 1. Prerequisites
---
- Install Docker Engine, either through Docker Desktop or directly Docker engine. Personally, using the latter.  
- Make sure Docker Compose is installed or install it.  
- Ressources :  
Medium [article](https://towardsdatascience.com/learning-docker-the-easy-way-52b7bdec5e86) install and basic use of Docker . Docker official ressources should be enough though.  
[Jupyter-Docker-Stacks](https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html). "A set of ready-to-run Docker images containing Jupyter applications".    
The source [article](https://towardsdatascience.com/machine-learning-on-a-large-scale-2eef3bb749ee) I (very slightly) adapted the docker-compose file from.  
Install Docker engine (apt get), [official ressource](https://docs.docker.com/engine/install/ubuntu/).  
Install Docker compose (apt get), [official ressource](https://docs.docker.com/compose/install/linux/#install-using-the-repository).  

### 2. How to
---

After Docker Engine/compose installation, on linux, do not forget the post-installation [steps](https://docs.docker.com/engine/install/linux-postinstall/)
1. Git clone this repository or create a new one (name of your choice)
2. Open terminal, cd into custom directory, make sure `docker-compose.yml` file is present (copy it in if needed)  
https://github.com/matthieuvion/spark-cluster/blob/6e5cb821a7236de94d005fce9168abe07eb306c7/docker-compose.yml#L1-L30

Basically, the yml file tells Docker compose how to run the Spark Master, Worker, Jupyterlab. You will have access to your local disk/current working directory every time you run this command.

3. Run docker compose
```
cd my-directory
docker compose up
# or depending of your Docker Compose install:   
docker-compose up
```
Docker compose will automatically download the needed images (spark:3.3.1 for Master and Worker, pyspark-notebook for the JupyterLab interface) and run the whole thing. The next times, it will only run them.  

### 3. Profit : JupyterLab interface, Spark cluster (standalone) mode 
Jupyter lab interface : http://localhost:8888  
Spark Master : http://localhost:8080  
Spark Worker : http://localhost:8081  
You can use the demo file `spark-cluster.ipynb` for a ready to run PySpark notebook, or simply create a new one and run it using this code snippet to build the SparkSession :   

```
from pyspark.sql import SparkSession

# SparkSession
URL_SPARK = "spark://spark:7077"

spark = (
    SparkSession.builder
    .appName("spark-ml")
    .config("executor.memory", "4g")
    .master(URL_SPARK)
    .getOrCreate()
)
```

### Bonus : Notebook, predict using spark.ml Pipeline()
---
If you use `spark-cluster.ipynb`, a demo example shows how to build a spark.ml predict Pipeline() with a random forest regressor, on a well known dataset.
