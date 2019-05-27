from elasticsearch import Elasticsearch
from elasticsearch import helpers
import redis,json

r = redis.Redis(host='172.16.0.131', port=6379)
es = Elasticsearch(['172.16.0.131'], timeout=3600)

while True:
    actions = []
    nu = 0
    while True:
        if nu < 200:
            action = r.rpop('applog')
            if action == None:
                break
            actions.append(json.loads(action))
            nu += 1
        else:
            break
    if len(actions) > 0:
        try:
            a = helpers.bulk(es, actions)
        except:
            pass
