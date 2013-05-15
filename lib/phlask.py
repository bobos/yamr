from flask import Flask
app = Flask(__name__)
@app.route('/', methods=['POST'])
def post():
    import time
    time.sleep(30*60)
    return 'Hello World!'

@app.route('/', methods=['GET'])
def get():
    return 'GET World!'

if __name__ == '__main__':
    import sys
    app.run(sys.argv[1])
