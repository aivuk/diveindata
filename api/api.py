import diveindata

from flask import Flask
from flask_restful import Resource, Api
from flask_cors import CORS

import sys

app = Flask(__name__)
CORS(app)
api = Api(app)

data = diveindata.DataInfo(sys.argv[1])

class DataDiveColumns(Resource):
    def get(self):
        return data.columns

api.add_resource(DataDiveColumns, '/columns')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
