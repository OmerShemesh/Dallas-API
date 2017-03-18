from flask import Flask
from flask_restful import Api, Resource, abort
from flask_restful.utils import cors
from flask_pymongo import PyMongo
from webargs.flaskparser import use_args, use_kwargs, parser
from marshmallow import fields

app = Flask(__name__)
app.config['MONGO_DBNAME'] = 'dallas'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/dallas'
api = Api(app)
api.decorators = [
    cors.crossdomain(
        origin='*',
        methods=['GET'],
        attach_to_all=True,
        automatic_options=True
    )
]
mongo = PyMongo(app)


class GeneralStatisticsResource(Resource):
    setup_args = {
        'stats_for': fields.String(required=True)
    }

    @use_args(setup_args)
    def get(self, args):

        stats = {}
        if args['stats_for'] == 'setups':
            setups_count = mongo.db.setup.find().count()
            pipe = [{'$group': {'_id': None, 'average_dcs_count': {'$avg': '$dcs_count'}}}]
            avg_dcs = list(mongo.db.setup.aggregate(pipeline=pipe))

            stats = {'setups_count': setups_count, 'average_dcs_count': round(avg_dcs[0]['average_dcs_count'])}

        elif args['stats_for'] == 'hosts':

            hosts_count = mongo.db.host.find().count()
            # intel = mongo.db.host.find({'cpu_manufacturer': 'Intel'}).count()
            # amd = mongo.db.host.find({'cpu_manufacturer': 'AMD'}).count()
            # ibm = mongo.db.host.find({'cpu_manufacturer': 'IBM'}).count()

            vm_pipe = [{'$group': {'_id': None, 'average_running_vms': {'$avg': '$running_vms_count'}}}]
            cpu_pipe = [{'$group': {'_id': '$cpu_manufacturer', 'count': {'$sum': 1}}}]
            avg_vms = list(mongo.db.host.aggregate(pipeline=vm_pipe))
            cpus_list = list(mongo.db.host.aggregate(pipeline=cpu_pipe))
            cpus = {}

            for cpu in cpus_list:
                cpus[cpu['_id']] = round((cpu['count']*100) / hosts_count)

            stats = {'cpus': cpus, 'hosts_count': hosts_count,
                     'average_running_vms': round(avg_vms[0]['average_running_vms'])}

        elif args['stats_for'] == 'datacenters':
            datacenters_count = mongo.db.datacenter.find().count()
            pipe = [{'$group': {'_id': None, 'average_clusters_count': {'$avg': '$clusters_count'}}}]
            avg_clusters = list(mongo.db.datacenter.aggregate(pipeline=pipe))

            stats = {'datacenters_count': datacenters_count,
                     'average_clusters_count': round(avg_clusters[0]['average_clusters_count'])}

        elif args['stats_for'] == 'clusters':
            clusters_count = mongo.db.cluster.find().count()
            vm_pipe = [{'$group': {'_id': None, 'average_vms_count': {'$avg': '$vms_count'}}}]
            host_pipe = [{'$group': {'_id': None, 'average_hosts_count': {'$avg': '$hosts_count'}}}]
            ovirt_version_pipe = [{'$group': {'_id': '$ovirt_compatibility_version', 'count': {'$sum': 1}}}]

            avg_hosts = list(mongo.db.cluster.aggregate(pipeline=host_pipe))
            avg_vms = list(mongo.db.cluster.aggregate(pipeline=vm_pipe))
            ovirt_versions_list = list(mongo.db.cluster.aggregate(pipeline=ovirt_version_pipe))
            ovirt_versions = {}

            for version in ovirt_versions_list:
                ovirt_versions[version['_id']] = round((version['count'] * 100) / clusters_count)

            stats = {'clusters_count': clusters_count,
                     'average_hosts_count': round(avg_hosts[0]['average_hosts_count']),
                     'average_vms_count': round(avg_vms[0]['average_vms_count']), 'ovirt_versions': ovirt_versions}

        elif args['stats_for'] == 'vms':
            vms_count = mongo.db.vm.find().count()
            pipe = [{'$group': {'_id': None, 'average_mem_size': {'$avg': '$mem_size'}}}]
            avg_mem_size = list(mongo.db.vm.aggregate(pipeline=pipe))

            stats = {'vms_count': vms_count, 'average_mem_size': round(avg_mem_size[0]['average_mem_size'])}

        elif args['stats_for'] == 'storage':
            storage_count = mongo.db.storage.find().count()
            storage_type_pipe = [{'$group': {'_id': '$storage_type', 'count': {'$sum': 1}}}]
            storage_types_list = list(mongo.db.storage.aggregate(pipeline=storage_type_pipe))
            storage_types = {}

            for storage_type in storage_types_list:
                storage_types[storage_type['_id']] = round((storage_type['count'] * 100) / storage_count)

            stats = {'storage_count': storage_count, 'storage_types': storage_types}

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
        ibm = mongo.db.host.find({'cpu_manufacturer': 'IBM'}).count()
        return {'intel_hosts': intel, 'amd_hosts': amd, 'ibm_hosts': ibm}


class TemplatesResource(Resource):
    def get(self):
        templates = mongo.db.template.find()

        output = []
        for template in templates:
            output.append(template)
        return output


api.add_resource(GeneralStatisticsResource, '/statistics/general')

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
