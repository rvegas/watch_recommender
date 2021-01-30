from google.cloud import dataproc_v1 as dataproc


def create_cluster():

    # Create a client with the endpoint set to the desired cluster region.
    cluster_client = dataproc.ClusterControllerClient(client_options={
        'api_endpoint': '{}-dataproc.googleapis.com:443'.format('europe-west1')
    })

    # Create the cluster config.
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
            'initialization_actions': [
                {
                    'executable_file': 'gs://bucket-cluster-ricardo/scripts/cp_watches.sh'
                },
                {
                    'executable_file': 'gs://bda5-keepcoding-ricardo1/scripts/cp_amazon.sh'
                },
                {
                    'executable_file': 'gs://bda5-keepcoding-ricardo1/scripts/cp_init.sh'
                },
                {
                    'executable_file': 'gs://bda5-keepcoding-ricardo1/scripts/init_hive.sh'
                }
            ]
        }
    }

    # Create the cluster.
    operation = cluster_client.create_cluster('big-data-architecture-ricardo', 'europe-north1', cluster)
    result = operation.result()

    # Output a success message.
    return 'Cluster created successfully: {}'.format(result.cluster_name)


def deploy(request):
    return create_cluster()
