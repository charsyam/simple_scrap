from flask import Flask
from flask import request, jsonify

app = Flask(__name__)

@app.route('/')
def scrap():
    return "Hello, SOMA!"

if __name__ == '__main__':
    app.run()
