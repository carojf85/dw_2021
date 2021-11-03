import json
import os
import shutil
import fileinput
import jinja2

dags_config = [
    {
        "dag_id": "test-pyspark-hive",
        "schedule": "@daily",
        "description": "This is a test",
        "usecase": "test-pyspark-hive"
    },
    {
        "dag_id": "test-pyspark-csv-to-nooba",
        "schedule": "@daily",
        "description": "This is a test",
        "usecase": "test-pyspark-csv-to-nooba"
    },
    {
        "dag_id": "test-pyspark-csv-to-pg",
        "schedule": "@daily",
        "description": "This is a test",
        "usecase": "test-pyspark-csv-to-pg"
    }
    ]

jinja_env = jinja2.Environment(loader = jinja2.FileSystemLoader('template'))
dag_template = jinja_env.get_template('dag-template.py')
yaml_template = jinja_env.get_template('yaml-template.yaml')


for config in dags_config:
    #DAG
    output_dag = dag_template.render(dag_id = config['dag_id']
                    ,schedule = config['schedule']
                    ,usecase = config['usecase']
                    ,description = config['description'])

    new_dag = config['dag_id']+'.py'
    print(new_dag)

    with open(f'.\{new_dag}','w') as f:
        f.write(output_dag)

    #YAML
    output_yaml = yaml_template.render(usecase = config['usecase'])
    new_yaml = config['dag_id']+'.yaml'
    print(new_yaml)

    with open(f'.\{new_yaml}','w') as f:
        f.write(output_yaml)
 

