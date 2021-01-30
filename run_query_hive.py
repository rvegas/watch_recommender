from google.cloud import dataproc_v1
from google.cloud.dataproc_v1.gapic.transports import (job_controller_grpc_transport)

def submit_job():  
    job_transport = (job_controller_grpc_transport.JobControllerGrpcTransport(address='{}-dataproc.googleapis.com:443'.format('europe-north1')))
    job_details = {
        'placement': {
            'cluster_name': 'dataproc-bda'
        },
        'hive_job': {
            'query_file_uri': 'gs://{}/{}'.format('bucket-cluster-ricardo', 'scripts/query_watches.txt')
        }
    }
    dataproc_job_client = dataproc_v1.JobControllerClient(job_transport)

    result = dataproc_job_client.submit_job(project_id='big-data-architecture-ricardo', region='europe-north1', job=job_details)
    job_id = result.reference.job_id
    print('Submitted job ID {}.'.format(job_id))

def query(request):
    submit_job()
    return f'done!'
