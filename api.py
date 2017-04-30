from flask import Flask
from flask_restful import Api, Resource
from flask_restful.utils import cors
from flask_pymongo import PyMongo
from webargs.flaskparser import use_args
from marshmallow import fields

app = Flask(__name__)
app.config['MONGO_DBNAME'] = 'dallas'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/dallas'

api = Api(app, prefix='/api')
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

    @staticmethod
    def get_percentage_dict(stats_list, objects_count):
        stats_dict = {}
        for item in stats_list:
            if item['_id'] is not None:
                stats_dict[item['_id']] = float("{0:.2f}".format((item['count'] * 100) / objects_count))
        return stats_dict

    @staticmethod
    def setup_stats():
        setups_count = mongo.db.setup.find().count()
        dcs_pipe = [{'$group': {'_id': None, 'average_dcs_count': {'$avg': '$dcs_count'},
                                'max_dcs_count': {'$max': '$dcs_count'}}}]
        clusters_pipe = [{'$group': {'_id': None, 'average_clusters_count': {'$avg': '$clusters_count'},
                                     'max_clusters_count': {'$max': '$clusters_count'}}}]
        hosts_pipe = [{'$group': {'_id': None, 'average_hosts_count': {'$avg': '$hosts_count'},
                                  'max_hosts_count': {'$max': '$hosts_count'}}}]
        vms_pipe = [{'$group': {'_id': None, 'average_vms_count': {'$avg': '$vms_count'},
                                'max_vms_count': {'$max': '$vms_count'}}}]
        avg_dcs = list(mongo.db.setup.aggregate(pipeline=dcs_pipe))
        avg_clusters = list(mongo.db.setup.aggregate(pipeline=clusters_pipe))
        avg_hosts = list(mongo.db.setup.aggregate(pipeline=hosts_pipe))
        avg_vms = list(mongo.db.setup.aggregate(pipeline=vms_pipe))

        return {'setups_count': setups_count, 'average_dcs_count': round(avg_dcs[0]['average_dcs_count']),
                'max_dcs_count': avg_dcs[0]['max_dcs_count'],
                'average_clusters_count': round(avg_clusters[0]['average_clusters_count']),
                'max_clusters_count': avg_clusters[0]['max_clusters_count'],
                'average_hosts_count': round(avg_hosts[0]['average_hosts_count']),
                'max_hosts_count': avg_hosts[0]['max_hosts_count'],
                'average_vms_count': avg_vms[0]['average_vms_count'],
                'max_vms_count': avg_vms[0]['max_vms_count']}

    @staticmethod
    def dc_stats():
        datacenters_count = mongo.db.datacenter.find().count()
        clusters_pipe = [{'$group': {'_id': None, 'average_clusters_count': {'$avg': '$clusters_count'},
                                     'max_clusters_count': {'$max': '$clusters_count'}}}]
        storage_pipe = [{'$group': {'_id': None, 'average_storage_count': {'$avg': '$storage_count'},
                                    'max_storage_count': {'$max': '$storage_count'}}}]
        networks_pipe = [{'$group': {'_id': None, 'average_networks_count': {'$avg': '$networks_count'},
                                     'max_networks_count': {'$max': '$networks_count'}}}]
        avg_clusters = list(mongo.db.datacenter.aggregate(pipeline=clusters_pipe))
        avg_storage = list(mongo.db.datacenter.aggregate(pipeline=storage_pipe))
        avg_networks = list(mongo.db.datacenter.aggregate(pipeline=networks_pipe))

        return {'datacenters_count': datacenters_count,
                'average_clusters_count': round(avg_clusters[0]['average_clusters_count']),
                'max_clusters_count': avg_clusters[0]['max_clusters_count'],
                'average_storage_count': round(avg_storage[0]['average_storage_count']),
                'max_storage_count': avg_storage[0]['max_storage_count'],
                'average_networks_count': round(avg_networks[0]['average_networks_count']),
                'max_networks_count': avg_networks[0]['max_networks_count']}

    @staticmethod
    def cluster_stats():
        clusters_count = mongo.db.cluster.find().count()
        vm_pipe = [{'$group': {'_id': None, 'average_vms_count': {'$avg': '$vms_count'},
                               'max_vms_count': {'$max': '$vms_count'}}}]
        host_pipe = [{'$group': {'_id': None, 'average_hosts_count': {'$avg': '$hosts_count'},
                                 'max_hosts_count': {'$max': '$hosts_count'}}}]
        ovirt_version_pipe = [{'$group': {'_id': '$ovirt_compatibility_version', 'count': {'$sum': 1}}}]
        cpu_family_pipe = [{'$group': {'_id': '$cpu_family', 'count': {'$sum': 1}}}]

        avg_hosts = list(mongo.db.cluster.aggregate(pipeline=host_pipe))
        avg_vms = list(mongo.db.cluster.aggregate(pipeline=vm_pipe))
        cpu_families_list = list(mongo.db.cluster.aggregate(pipeline=cpu_family_pipe))
        ovirt_versions_list = list(mongo.db.cluster.aggregate(pipeline=ovirt_version_pipe))

        ovirt_versions = GeneralStatisticsResource.get_percentage_dict(ovirt_versions_list, clusters_count)
        cpu_families = GeneralStatisticsResource.get_percentage_dict(cpu_families_list, clusters_count)

        return {'clusters_count': clusters_count,
                'average_hosts_count': round(avg_hosts[0]['average_hosts_count']),
                'max_hosts_count': avg_hosts[0]['max_hosts_count'],
                'average_vms_count': round(avg_vms[0]['average_vms_count']),
                'max_vms_count': avg_vms[0]['max_vms_count'],
                'ovirt_versions': ovirt_versions,
                'cpu_families': cpu_families}

    @staticmethod
    def host_stats():
        hosts_count = mongo.db.host.find().count()
        vm_pipe = [{'$group': {'_id': None, 'average_running_vms': {'$avg': '$running_vms_count'},
                               'max_running_vms': {'$max': '$running_vms_count'}}}]
        cpu_pipe = [{'$group': {'_id': '$cpu_manufacturer', 'count': {'$sum': 1}}}]
        cpu_cores_pipe = [{'$group': {'_id': '$cpu_cores', 'count': {'$sum': 1}}}]
        mem_usage_pipe = [{'$group': {'_id': None, 'average_mem_usage': {'$avg': '$mem_usage'},
                                      'max_mem_usage': {'$max': '$mem_usage'}}}]
        cpu_usage_pipe = [{'$group': {'_id': None, 'average_cpu_usage': {'$avg': '$cpu_usage'}}}]
        mem_size_pipe = [{'$group': {'_id': None, 'average_mem_size': {'$avg': '$mem_size'},
                                     'max_mem_size': {'$max': '$mem_size'}}}]
        os_type_pipe = [{'$group': {'_id': '$os', 'count': {'$sum': 1}}}]
        nics_pipe = [{'$group': {'_id': None, 'average_nics_count': {'$avg': '$nics_count'},
                                 'max_nics_count': {'$max': '$nics_count'}}}]

        avg_vms = list(mongo.db.host.aggregate(pipeline=vm_pipe))
        cpus_list = list(mongo.db.host.aggregate(pipeline=cpu_pipe))
        cpu_cores_list = list(mongo.db.host.aggregate(pipeline=cpu_cores_pipe))
        avg_mem_size = list(mongo.db.host.aggregate(pipeline=mem_size_pipe))
        avg_mem_usage = list(mongo.db.host.aggregate(pipeline=mem_usage_pipe))
        avg_cpu_usage = list(mongo.db.host.aggregate(pipeline=cpu_usage_pipe))
        os_types_list = list(mongo.db.host.aggregate(pipeline=os_type_pipe))
        avg_nics_list = list(mongo.db.host.aggregate(pipeline=nics_pipe))

        cpus = GeneralStatisticsResource.get_percentage_dict(cpus_list, hosts_count)
        cpu_cores = GeneralStatisticsResource.get_percentage_dict(cpu_cores_list, hosts_count)
        os_types = GeneralStatisticsResource.get_percentage_dict(os_types_list, hosts_count)

        return {'cpus': cpus, 'hosts_count': hosts_count,
                'average_running_vms': round(avg_vms[0]['average_running_vms']),
                'cpu_cores': cpu_cores,
                'os_types': os_types,
                'max_running_vms': avg_vms[0]['max_running_vms'],
                'average_mem_size': round(avg_mem_size[0]['average_mem_size']),
                'max_mem_size': avg_mem_size[0]['max_mem_size'],
                'average_mem_usage': float("{0:.2f}".format(avg_mem_usage[0]['average_mem_usage'])),
                'max_mem_usage': avg_mem_usage[0]['max_mem_usage'],
                'average_cpu_usage': float("{0:.2f}".format(avg_cpu_usage[0]['average_cpu_usage'])),
                'average_nics_count': round(avg_nics_list[0]['average_nics_count']),
                'max_nics_count': round(avg_nics_list[0]['max_nics_count'])

                }

    @staticmethod
    def vm_stats():
        vms_count = mongo.db.vm.find().count()
        mem_size_pipe = [{'$group': {'_id': None, 'average_mem_size': {'$avg': '$mem_size'},
                                     'max_mem_size': {'$max': '$mem_size'}}}]
        os_type_pipe = [{'$group': {'_id': '$os_type', 'count': {'$sum': 1}}}]
        mem_usage_pipe = [{'$group': {'_id': None, 'average_mem_usage': {'$avg': '$mem_usage'}}}]
        cpu_usage_pipe = [{'$group': {'_id': None, 'average_cpu_usage': {'$avg': '$cpu_usage'}}}]
        display_type_pipe = [{'$group': {'_id': '$display_type', 'count': {'$sum': 1}}}]
        num_of_cpus_pipe = [{'$group': {'_id': '$num_of_cpus', 'count': {'$sum': 1}}}]
        nics_pipe = [{'$group': {'_id': None, 'average_nics_count': {'$avg': '$nics_count'},
                                 'max_nics_count': {'$max': '$nics_count'}}}]
        disks_pipe = [{'$group': {'_id': None, 'average_disks_count': {'$avg': '$disks_count'},
                                  'max_disks_count': {'$max': '$disks_count'}}}]

        avg_mem_size = list(mongo.db.vm.aggregate(pipeline=mem_size_pipe))
        os_types_list = list(mongo.db.vm.aggregate(pipeline=os_type_pipe))
        display_types_list = list(mongo.db.vm.aggregate(pipeline=display_type_pipe))
        avg_mem_usage = list(mongo.db.vm.aggregate(pipeline=mem_usage_pipe))
        avg_cpu_usage = list(mongo.db.vm.aggregate(pipeline=cpu_usage_pipe))
        num_of_cpus_list = list(mongo.db.vm.aggregate(pipeline=num_of_cpus_pipe))
        avg_nics_list = list(mongo.db.vm.aggregate(pipeline=nics_pipe))
        avg_disks_list = list(mongo.db.vm.aggregate(pipeline=disks_pipe))

        os_types = GeneralStatisticsResource.get_percentage_dict(os_types_list, vms_count)
        display_types = GeneralStatisticsResource.get_percentage_dict(display_types_list, vms_count)
        num_of_cpus = GeneralStatisticsResource.get_percentage_dict(num_of_cpus_list, vms_count)

        return {'vms_count': vms_count,
                'average_mem_size': round(avg_mem_size[0]['average_mem_size']),
                'max_mem_size': avg_mem_size[0]['max_mem_size'],
                'os_types': os_types,
                'display_types': display_types,
                'num_of_cpus': num_of_cpus,
                'average_mem_usage': float("{0:.2f}".format(avg_mem_usage[0]['average_mem_usage'])),
                'average_cpu_usage': float("{0:.2f}".format(avg_cpu_usage[0]['average_cpu_usage'])),
                'average_nics_count': round(avg_nics_list[0]['average_nics_count']),
                'max_nics_count': avg_nics_list[0]['max_nics_count'],
                'average_disks_count': round(avg_disks_list[0]['average_disks_count']),
                'max_disks_count': avg_disks_list[0]['max_disks_count']
                }

    @staticmethod
    def storage_stats():
        storage_count = mongo.db.storage.find().count()
        storage_type_pipe = [{'$group': {'_id': '$storage_type', 'count': {'$sum': 1}}}]
        disk_usage_pipe = [{'$group': {'_id': None, 'average_disk_usage': {'$avg': '$used_disk'},
                                       'max_disk_usage': {'$max': '$used_disk'}}}]
        storage_types_list = list(mongo.db.storage.aggregate(pipeline=storage_type_pipe))
        avg_disk_usage = list(mongo.db.storage.aggregate(pipeline=disk_usage_pipe))

        storage_types = GeneralStatisticsResource.get_percentage_dict(storage_types_list, storage_count)

        return {'storage_count': storage_count, 'storage_types': storage_types,
                'average_disk_usage': float("{0:.2f}".format(avg_disk_usage[0]['average_disk_usage'])),
                'max_disk_usage': avg_disk_usage[0]['max_disk_usage']}

    @use_args(setup_args)
    def get(self, args):

        stats = {}
        if args['stats_for'] == 'setups':
            stats = self.setup_stats()
        elif args['stats_for'] == 'hosts':
            stats = self.host_stats()
        elif args['stats_for'] == 'datacenters':
            stats = self.dc_stats()
        elif args['stats_for'] == 'clusters':
            stats = self.cluster_stats()
        elif args['stats_for'] == 'vms':
            stats = self.vm_stats()
        elif args['stats_for'] == 'storage':
            stats = self.storage_stats()
        return stats


class SetupStatistics(Resource):
    def get(self):
        return list(mongo.db.setup.find())


# general statistics endpoint
api.add_resource(GeneralStatisticsResource, '/statistics/general')
api.add_resource(SetupStatistics, '/statistics/setups')

if __name__ == '__main__':
    app.run(debug=True)
