import tornado.ioloop
import tornado.web
import redis
import json

from ch import ConsistentHash
from html.parser import HTMLParser
from tornado.httpclient import AsyncHTTPClient

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

def consistent_hashing(hosts):
    rconns = [redis.StrictRedis(x, y) for x, y in hosts]
    hostlist = ["%s:%s"%(x,y) for x,y in hosts]
    kvlist = list(zip(hostlist, rconns))

    global ch
    ch = ConsistentHash(kvlist, 10)

consistent_hashing([("127.0.0.1", 6379), ("127.0.0.1", 6380), ("127.0.0.1", 6381)])

class MainHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        url = self.get_argument('url')
        contents = get_from_cache(url)
        if not contents:
            self.async_fetch(url)
        else:
            self.write(contents)
            self.finish()

    def async_fetch(self, url):
        self.url = url
        http_client = http_client = AsyncHTTPClient()
        http_client.fetch(url, self.handle_response)

    def handle_response(self, response):
        if response.error:
            self.write(response.error)
        else:
            contents = parse_og(str(response.body))
            put_to_cache(self.url, contents)
            self.write(contents)
        self.finish()

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", MainHandler),
        ]

        super(Application, self).__init__(handlers)

if __name__ == "__main__":
    Application().listen(8888)
    tornado.ioloop.IOLoop.instance().start()
