from flask import Flask
from flask import request, jsonify
from ch import ConsistentHash
import redis
import requests
import json
from html.parser import HTMLParser

app = Flask(__name__)

class OGParser(HTMLParser):
    def __init__(self):
        super(self.__class__, self).__init__()

        self.og = {}

    def handle_starttag(self, tag, attrs):
        tmp_og_contents = None
        tmp_og_name = None

        if tag == "meta":
            for attr in attrs:
                if "property" == attr[0] and attr[1].startswith("og:"):
                    tmp_og_name = attr[1][3:]
                    if tmp_og_contents:
                        self.og[tmp_og_name] = tmp_og_contents

                elif "content" == attr[0]:
                    tmp_og_contents = attr[1]
                    if tmp_og_name:
                        self.og[tmp_og_name] = tmp_og_contents
                        

def get_from_cache(key):
    idx, conn = ch.get(key)
    v = conn.get(key)
    if v:
        return json.loads(v.decode())

    return None

def put_to_cache(key, contents):
    idx, conn = ch.get(key)
    conn.set(key, json.dumps(contents))

def parse_og(html):
    og_parser = OGParser()
    og_parser.feed(html)
    return og_parser.og

def fetch_from_url(url):
    result = requests.get(url)
    if result.status_code != 200:
        return None

    og = parse_og(result.text)
    return og

def fetch(url):
    contents = get_from_cache(url)
    if not contents:
        contents = fetch_from_url(url)
        if contents:
            put_to_cache(url, contents)

    return jsonify(contents)

def consistent_hashing(hosts):
    rconns = [redis.StrictRedis(x, y) for x, y in hosts]
    hostlist = ["%s:%s"%(x,y) for x,y in hosts]
    kvlist = list(zip(hostlist, rconns))

    global ch
    ch = ConsistentHash(kvlist, 10)

@app.route('/')
def scrap():
    url = request.args.get('url')
    contents = fetch(url)
    return contents

consistent_hashing([("127.0.0.1", 6379), ("127.0.0.1", 6380), ("127.0.0.1", 6381)])

if __name__ == '__main__':
#    fetch("http://www.naver.com")
    app.run()
