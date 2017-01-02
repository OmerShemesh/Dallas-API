from flask import Flask
from flask_restful import Api, Resource
from flask_pymongo import PyMongo
from flask import jsonify

app = Flask(__name__)
app.config['MONGO_DBNAME'] = 'dallas'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/dallas'
api = Api(app)
mongo = PyMongo(app)


class HostsResource(Resource):
    def get(self):
        hosts = mongo.db.host.find()
        output = []

        for host in hosts:
            output.append(host)
        return output


class VmsResource(Resource):
    def get(self):
        vms = mongo.db.vm.find()
        output = []

        for vm in vms:
            output.append(vm)
        return output


class HostResource(Resource):
    def get(self, host_id):
        return mongo.db.host.find_one({'_id': host_id})


class HostStatisticsResource(Resource):
    def get(self):
        # pipeline = [
        #     {'$match': {'cpu_manufacturer': 'Intel'}},
        #     {'$group': {'_id': None,'count': {'$sum':1}}}
        # ]
        intel = mongo.db.host.find({'cpu_manufacturer': 'Intel'}).count()
        amd = mongo.db.host.find({'cpu_manufacturer': 'AMD'}).count()
        return {'intel_hosts': intel, 'amd_hosts': amd}


api.add_resource(HostsResource, '/hosts')
api.add_resource(VmsResource, '/vms')
api.add_resource(HostResource, '/hosts/<host_id>')
api.add_resource(HostStatisticsResource, '/hosts/statistics')

if __name__ == '__main__':
    app.run(debug=True)
