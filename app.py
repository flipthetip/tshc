
from flask import Flask, request, jsonify

from flask_restplus import Resource, Api, reqparse
from werkzeug.contrib.fixers import ProxyFix

import os
from filters import *
from parsers import *

app = Flask(__name__)
api = Api(app, version='1.0', title='THE SHADY CLASS',
    description='CHIBIFIED EKLAVU',
)



@api.route('/tshc/<int:num>') 
class collection(Resource): 
    def get(self,num): # Default to 200 OK 

        tshc_col = filters_col(num)
        
        return tshc_col

port = os.getenv('PORT', '5000')

if __name__ == "__main__":
    app.secret_key = 'mysecretkey'
    app.run(host='0.0.0.0', port=int(port), debug=True)


