from elasticsearch import Elasticsearch
from dateutil.parser import parse as parse_date
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

sg = SendGridAPIClient(os.environ.get('XXXXXXXXXX'))

def send_mail(content):
    message = Mail(
        from_email='info@watchesrecommender.com',
        to_emails='clients@watchesrecommender.com',
        subject='Newsletter of the week',
        html_content=content
    )
    response = sg.send(message)

es = Elasticsearch("https://elastic:Ur22RjS6@35.228.217.66:9200", verify_certs=False)
es.info()

def get_info(results):
    watches = []
    for hit in results['hits']['hits']:
        info = {}
        info['score'] = hit['_score']
        info['name'] = hit['_source']['name'].split('\n')[0]
        info['model'] = hit['_source']['model'].split('\n')[0]
        info['price'] = hit['_source']['price']
        watches.append(info)
    return watches

def search_query(query):
    return get_info(es.search(index='shakespeare', params={"q": query}))
    
def get_best_watches():
    return search_query('best watch')

def send_newsletter()
    watches = get_best_watches()
    email_body = "<h1>These are the best of the week!</h1>"
    for watch in watches:
        email_body = "<h2>{}</h2><h3>{}</h3><p>Price: {}</p><p>score: {}</>".format(name, model, price, score)
    send_mail(email_body)
    
