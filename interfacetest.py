#!/usr/bin/env python 
# -*- coding:utf-8 -*-

from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

class MyObject:
    def doc_score(self):
        return "hello xmlprc"


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)


# Create server
with SimpleXMLRPCServer(("10.9.1.199", 8892),requestHandler=RequestHandler) as server:
    server.register_introspection_functions()
    obj = MyObject()
    server.register_instance(obj)
    print("server is start...........")
    # Run the server's main loop
    server.serve_forever()