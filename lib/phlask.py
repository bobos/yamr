from flask import Flask, request
import json
import socket

app = Flask(__name__)

@app.route('/cluster/<name>', methods=['POST'])
def create_cluster(name):
    if '@' in name:
        return 'Invaild cluster name, cluster name can\'t contain @', 400
    quota = (request.json)['slave quota']
    ret = send2erl(form_msg('create_cluster', [name, quota], request.json))
    if ret == 'ok':
        return 'OK', 201
    else:
        return ret, 400

@app.route('/cluster/<name>', methods=['GET'])
def get_cluster(name):
    return 'OK' ## render to page

@app.route('/cluster/<name>', methods=['DELETE'])
def remove_cluster(name):
    ret = send2erl(form_msg('remove_cluster', [name], request.json))
    if ret == 'ok':
        return 'OK', 410
    else:
        return ret, 400

@app.route('/cluster/<name>/add', methods=['PUT'])
def add_servers(name):
    ##server = request.form['server']
    jsonResp = {}
    for server, number in ((request.json)['servers']).items():
        ret = send2erl(form_msg('add_server', [name, server, number],
                                request.json))
        jsonResp[server] = ret
    return json.dumps(jsonResp, indent=4)

@app.route('/cluster/<name>/remove', methods=['PUT'])
def remove_servers(name):
    jsonResp = {}
    for server, number in ((request.json)['servers']).items():
        ret = send2erl(form_msg('remove_server', [name, server, number],
                                request.json))
        jsonResp[server] = ret
    return json.dumps(jsonResp, indent=4)

@app.route('/job', methods=['POST'])
def submit_job():
    cluster = (request.json)['cluster']
    callbackModule = (request.json)['callback module']
    language = (request.json)['language']
    codePath = (request.json)['code path']
    vsn = (request.json)['version number']
    prio = (request.json)['priority']
    partition = (request.json)['partition']
    jobFile = (request.json)['job file']
    return send2erl(form_msg('submit_job', [cluster, callbackModule, 
                             language, codePath, vsn, prio, partition, 
                             jobFile], request.json))

def send2erl(message):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('127.0.0.1', 7654))
    sock.sendall(message)
    ## FIXME: hardcoded buffer
    resp = sock.recv(1024)
    sock.close()
    return resp

def form_msg(msgHdr, body, request):
    msg = msgHdr+'\x1e'+request['timeout']+'\x1e'
    for elem in body:
        msg = msg+'\x1e'+elem
    return msg
