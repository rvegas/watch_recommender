from google.cloud import dataproc_v1 as dataproc


def create_cluster():

    # Crear cliente
    cluster_client = dataproc.ClusterControllerClient(client_options={
        'api_endpoint': '{}-dataproc.googleapis.com:443'.format('europe-west1')
    })

    # Crear el cluster config.
    cluster = {
        'project_id': 'erudite-stratum-301721',
        'cluster_name': 'dataproc-bda',
        'config': {
            'master_config': {
                'num_instances': 1,
                'machine_type_uri': 'n1-standard-1'
            },
            'worker_config': {
                'num_instances': 2,
                'machine_type_uri': 'n1-standard-1'
            },
            'config_bucket': 'bucket-cluster-ricardo',
            'initialization_actions': [ # !!!importante!
                {
                    'executable_file': 'gs://bucket-cluster-ricardo/scripts/copy_amazon_source.sh' # copy source 1 para evitar borrado desde HIVE
                },
                {
                    'executable_file': 'gs://bucket-cluster-ricardo/scripts/copy_crawl_source.sh' # copy source 2 para evitar borrado desde HIVE
                },
                {
                    'executable_file': 'gs://bda5-keepcoding-ricardo1/scripts/copy_hive_init_script.sh' # beeline no puede leer desde gs, se copia a local la query
                },
                {
                    'executable_file': 'gs://bda5-keepcoding-ricardo1/scripts/execute_hive_script_from_file.sh' # se ejecuta el script en HIVE
                }
            ]
        }
    }

    # Crear el cluster.
    operation = cluster_client.create_cluster('big-data-architecture-ricardo', 'europe-north1', cluster)
    result = operation.result()

    # Output a success message.
    return 'Cluster created successfully: {}'.format(result.cluster_name)


def deploy(request):
    return create_cluster()
