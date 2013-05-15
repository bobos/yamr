from tornado.ioloop import IOLoop
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from phlask import app

if __name__ == "__main__":
    http_server = HTTPServer(WSGIContainer(app))
    http_server.bind(5000)
    http_server.start(0)
    IOLoop.instance().start()
