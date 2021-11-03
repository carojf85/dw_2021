from airflow import DAG
from airflow.models import Variable
#from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.lineage.entities import File
#from pyarrow import csv
import pyarrow
import pyarrow.parquet as pq
import boto3
import pathlib 
import re
import psycopg2
import json
import os
import io
import pandas as pd #provisional

BUCKET_NAME = str(os.environ.get("BUCKET_NAME"))
NAMESPACE = os.environ.get('AIRFLOW__KUBERNETES__NAMESPACE')

SFTP_source = '/opt/zibel/arrived/'
s3_landing = 'data/landing/'
s3_raw = 'data/raw/'
AWS_ACCESS_KEY_ID = 'MHmvfvpGXQxERYv7jcuJ'
AWS_SECRET_KEY = 'r1GDVvFebYKoczdjzLFuMZhzzOvBiCs2cOaZw/SQ'


def create_dag(dag_id,
               process_name,
               schedule,
               tasks,
               default_args,
               sparkapp = None,):


    def csv_to_parquet(file_path,landing_dir,raw_zone_dir):
        s3 = boto3.client('s3',aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_KEY,endpoint_url='http://s3.openshift-storage.svc')

        obj = s3.get_object(Bucket=BUCKET_NAME, Key=landing_dir+file_path)
        df = pd.read_csv(obj['Body'])

        table = pyarrow.Table.from_pandas(df)
        writer = pyarrow.BufferOutputStream()
        pq.write_table(table, writer)
        body = bytes(writer.getvalue())

        output_name = s3_raw + file_path.replace('.csv','.parquet')
        print("Ready to write to " + output_name)
        s3.put_object(Body=body, Bucket=BUCKET_NAME, Key=output_name)

    def get_k8_token():
        f = open('/var/run/secrets/kubernetes.io/serviceaccount/token')
        token = f.read()
        f.close
        return token


    dag = DAG(dag_id,
              schedule_interval=schedule,
              is_paused_upon_creation = False,
              default_args=default_args,
              user_defined_macros={
                    "k8_token": get_k8_token,
                }
              )

    with dag:

        
        workflow = []
        for index, item in enumerate(tasks, start = 0):
            task = None
            if (item == 'SFTP'):
                task = SFTPToS3Operator(task_id=item + process_name + '-file'
                ,s3_conn_id='cornerstone-dfs'
                ,s3_bucket=BUCKET_NAME
                ,s3_key=s3_landing + '{{ params.file_name }}'
                ,sftp_conn_id='sftp_laspgtst03'
                ,sftp_path=SFTP_source + '{{ params.file_name }}'
                ,inlets=File(url=SFTP_source + '{{ params.file_name }}')
                ,outlets=File(url=BUCKET_NAME +'/'+ s3_landing + '{{ params.file_name }}')
                )
            elif (item == 'parquet'):
                task = PythonOperator(task_id=process_name+'-to-parquet'
                ,python_callable=csv_to_parquet
                ,op_kwargs={'file_path': '{{ params.file_name }}'
                        ,'landing_dir': s3_landing
                        ,'raw_zone_dir': BUCKET_NAME + s3_raw}
                ,inlets=File(url=BUCKET_NAME + '/' + s3_landing + '{{ params.file_name }}')
                ,outlets=File(url=BUCKET_NAME + '/' + s3_raw + '{{ params.file_name }}'.replace('.csv', '.parquet'))
            )
            elif (item == 'pyspark-s'):
                task = SparkKubernetesOperator(task_id=process_name+'-submit'
                ,application_file= sparkapp
                ,kubernetes_conn_id="kubernetes_default"
                ,api_version="v1beta1"
                ,do_xcom_push=True,
                )
            elif (item == 'pyspark-m'):
                task = SparkKubernetesSensor(task_id=process_name + '-monitor'
                ,application_name="{{{{ task_instance.xcom_pull(task_ids='{}-submit')['metadata']['name'] }}}}".format(process_name)
                ,kubernetes_conn_id="kubernetes_default"
                ,api_version="v1beta1",
                )
            elif (item == 'pyspark-d'):
                task = BashOperator(task_id='bash-del-'+ process_name +'-app'
                ,bash_command="""curl -k \
    -X DELETE \
    -d @- \
    -H "Authorization: Bearer {{ k8_token() }} " \
    -H 'Accept: application/json' \
    -H 'Content-Type: application/json' \
https://api.lv-tst01.ocp.c1b:6443/apis/sparkoperator.k8s.io/v1beta1/namespaces/"""+NAMESPACE+"""/sparkapplications/pyspark-placeholder <<'EOF'
{
}
EOF
"""
                )
            else:
                task =DummyOperator(task_id='Dummy-'+item,)

            task.dag = dag
            workflow.append(task)
            if index not in [0]: 
                workflow[index-1] >> workflow[index]

    return dag


##################################################


list_of_dags = Variable.get("dynamic_dags",deserialize_json=True)
for k,v in list_of_dags.items():
    dag_id = 'dynamic-{}-dag'.format(k)
    print(k)

    default_args = {'owner': 'airflow',
                    'start_date': days_ago(1),
                    'namespace': NAMESPACE,
                    'params':{
                        'file_name': v['file']
                        #,'sparkapp': v['sparkapp']
                        }
                    }

    globals()[dag_id] = create_dag(dag_id,
                                k,
                                v['schedule'],
                                v['tasks'],
                                default_args,
                                v['sparkapp'],)
