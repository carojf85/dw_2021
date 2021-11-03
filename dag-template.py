# [START import_module]
import datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
# Operators; we need this to operate!
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

# [END import_module]

default_args = {'owner': 'airflow',
                'start_date': datetime.datetime(2021, 7, 19),
                'depends_on_past': False,
                }

dag = DAG(
    '{{ dag_id }}',
    default_args=default_args,
    description='{{ description }}',
    schedule_interval='{{ schedule }}',
)
"""
GET PySpark configurations as Json 
Argument (optional): app_type

1) pySpark Application
json_conf = get_pyspark_conf_json()

2) Deequ pySpark Application
json_conf = get_pyspark_conf_json(app_type='deequ')

# Update Json Configurations 

method Name: update_spark_conf

Mandatory arguments:
a) json_conf => which is getting from above method
b) app_name => Application Name (Should be unique to avoid conflict)
c) app_file_name => pySpark job fully qualified file name

Optional arguments:
a) arguments => default is None, Argument list for pySpark application file, 
                Provide string array ['arg1', 'arg2',..., 'argn']
b) driver_cores => default 1 , Provide valid integer number as driver cores 
c) driver_core_lmt => default "1200m" , Provide value as String as driver core CPU limit
d) driver_mem => default "512m" , Provide value as String as driver memory
e) exec_core => default 1 , Provide integer value for number of executor cores
f) exec_instances => default 1, Provide integer value for number of executor instances
g) exec_mem => default "512m" Provide String value for executor memory
h) extra_secret => default None ,Provide dictionary as 
                 { "ENV_VAR_NAME" : { "name" : "secret name" } , { "key" : "key in secret" }} (e.g. below)

extra_secrets = {"SQLSERVER_URL" : {"name": "sqlserver", "key": "url"}}

pyspark_hive_json_conf = update_spark_conf(json_conf=json_data, app_name="My-test-hive", app_file_name="Test_file.py",
                                 arguments=['cstonedb1.master_result', 'dq.detailed_results'],
                                 driver_cores=2, driver_mem="1024m", exec_core=2, exec_mem="1024m",
                                 exec_instances=4, extra_secret=extra_secrets)
                                 
t1 = SparkKubernetesOperator(
    task_id='pyspark-hive-submit',
    application_file=pyspark_hive_json_conf,  # Add Json conf here
    kubernetes_conn_id="kubernetes_default",
    api_version="v1beta1",
    do_xcom_push=True,
    dag=dag,
)



"""

with dag:
    start = DummyOperator(task_id='start', dag=dag)

    t1 = SparkKubernetesOperator(
        task_id="{{ spark-app-name }}-submit",
        namespace="cornerstone",
        application_file="{{ usecase }}.yaml",
        kubernetes_conn_id="kubernetes_default",
        api_version="v1beta1",
        do_xcom_push=True,
        dag=dag,
    )

    t2 = SparkKubernetesSensor(
        task_id="{{ usecase }}-monitor",
        namespace="cornerstone",
        application_name="{{ '{{' }} task_instance.xcom_pull(task_ids='{{ usecase }}-submit'{{ ')[' }}'metadata'{{ ']['}}'name'{{ ']}}' }}",
        kubernetes_conn_id="kubernetes_default",
        api_version="v1beta1",
        dag=dag,
    )

    f = open('/var/run/secrets/kubernetes.io/serviceaccount/token')
    token = f.read()
    f.close

    commands = """curl -k \
        -X DELETE \
        -d @- \
        -H "Authorization: Bearer """ + token + """ " \
        -H 'Accept: application/json' \
        -H 'Content-Type: application/json' \
    https://api.lv-tst01.ocp.c1b:6443/apis/sparkoperator.k8s.io/v1beta1/namespaces/cornerstone/sparkapplications/{{ usecase }} <<'EOF'
    {
    }
    EOF
    """

    t3 = BashOperator(
        task_id="bash-delete-spark-application",
        bash_command=commands,
        dag=dag
    )

    end = DummyOperator(task_id='end', dag=dag)

    start >> t3 >> t1 >> t2 >> end
