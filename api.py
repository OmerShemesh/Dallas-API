from flask import Flask
from flask_restful import Api, Resource
from flask_pymongo import PyMongo
from webargs.flaskparser import use_args, use_kwargs, parser, abort
from marshmallow import fields

app = Flask(__name__)
app.config['MONGO_DBNAME'] = 'dallas'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/dallas'
api = Api(app)
mongo = PyMongo(app)


class SetupResource(Resource):

    def get(self):
        setups_count = mongo.db.setup.find().count()
        output = []
        pipe = [{'$group': {'_id': None, 'average_dcs_count': {'$avg': '$dcs_count'}}}]
        avg_dcs = list(mongo.db.setup.aggregate(pipeline=pipe))

        stats = {'setups_count': setups_count, 'average_dcs_count': avg_dcs[0]['average_dcs_count']}

        return stats


class DataCenterResource(Resource):
    def get(self):
        datacenters = mongo.db.datacenter.find()
        output = []

        for datacenter in datacenters:
            output.append(datacenter)
        return output


class ClustersResource(Resource):
    args = {
        'vms': fields.Integer(required=False),
        'hosts': fields.Integer(required=False)
    }

    @use_kwargs(args)
    def get(self, vms, hosts):
        output = []

        if vms and not hosts:
            clusters = mongo.db.cluster.find({"vms_count": {"$gte": vms}})
        elif hosts and not vms:
            clusters = mongo.db.cluster.find({"hosts_count": {"$gte": hosts}})
        elif hosts and vms:
            clusters = mongo.db.cluster.find(
                {"hosts_count": {"$gte": hosts}, "vms_count": {"$gte": vms}})
        else:
            clusters = mongo.db.cluster.find()

        for cluster in clusters:
            output.append(cluster)

        return output


class HostsResource(Resource):
    def get(self):
        output = []
        hosts = mongo.db.host.find()

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


class HostsStatisticsResource(Resource):
    def get(self):
        # pipeline = [
        #     {'$match': {'cpu_manufacturer': 'Intel'}},
        #     {'$group': {'_id': None,'count': {'$sum':1}}}
        # ]
        intel = mongo.db.host.find({'cpu_manufacturer': 'Intel'}).count()
        amd = mongo.db.host.find({'cpu_manufacturer': 'AMD'}).count()
        return {'intel_hosts': intel, 'amd_hosts': amd}


class TemplatesResource(Resource):
    def get(self):
        templates = mongo.db.template.find()

        output = []
        for template in templates:
            output.append(template)
        return output

api.add_resource(SetupResource, '/setups/statistics')

# hosts endpoints
api.add_resource(DataCenterResource, '/datacenters')
api.add_resource(HostResource, '/hosts/<host_id>')

api.add_resource(HostsStatisticsResource, '/hosts/statistics')
api.add_resource(HostsResource, '/hosts')



# clusters endpoints
api.add_resource(ClustersResource, '/clusters')

# vms endpoints
api.add_resource(VmsResource, '/vms')

# templates endpoints
api.add_resource(TemplatesResource, '/templates')
if __name__ == '__main__':
    app.run(debug=True)
