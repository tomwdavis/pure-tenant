import time, requests, json
from elasticsearch import Elasticsearch, helpers
import purestorage
from purity_fb import PurityFb, FileSystemResponse, FileSystemPerformanceResponse, BucketPerformanceResponse, rest
import urllib3
from kubernetes import client, config
import base64
from ssl import create_default_context

class PureTenant:

    def __init__(self):
        """ 
        Assets:

            FlashBlade:

                chi-lab-fb01 Mgmt VIP: 10.224.112.90
                chi-lab-fb02 Mgmt VIP: 10.224.112.97

            FlashArray:

                chi-fs-flasharray1 Mgmt VIP: 10.224.112.20
                chi-fs-flasharray2 Mgmt VIP: 10.224.113.20
                chi-fs-remote Mgmt VIP: 10.224.113.30
        """
        # Disable Insecure Request warnings since FlashArray and FlashBlade use self-signed certs
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self.flashblades = {
            'chi-lab-fb01':
            {
                'ip':'10.224.112.90','auth_token':'T-9de7968c-d628-4241-a75d-c390bf90b6ac'
            },
            'chi-lab-fb02':
            {
                'ip':'10.224.112.97','auth_token':'T-4eb776c2-a4e4-46cc-ae96-9dcde5a0d293'
            }
        }

        self.flasharrays = {
            'chi-fs-flasharray1':
            {
                'ip':'10.224.112.20','auth_token':'4f54c6ce-5f6d-575e-bbe3-ce1051541d9d'
            },
            'chi-fs-flasharray2':
            {
                'ip':'10.224.113.20','auth_token':'79494678-19fa-60e4-7d3d-00a3df3338d2'
            },
            'chi-fs-remote':
            {
                'ip':'10.224.113.30','auth_token':'a6e8df02-077b-1a52-8220-6dd9ceab7c1e'
            }
        }

        # Set Elasticsearch Parameters
        # K8s config

        # Prod Settings
        # Get K8s secrets
        config.load_kube_config()
        v1 = client.CoreV1Api()
        # Password
        sec = str(v1.read_namespaced_secret("quickstart-es-elastic-user", "default").data)
        password = base64.b64decode(sec.strip().split()[1]).decode("utf-8")

        # Certificate
        sec = v1.read_namespaced_secret("quickstart-es-http-certs-public", "default").data
        cert = base64.b64decode(sec["tls.crt"]).decode("utf-8")
        context = create_default_context(cadata=cert)
        self.es_params = {
            'host': 'quickstart-es-http',
            'port': '9200',
            'http_auth': ('elastic', password),
            'scheme': 'https',
            'ssl_context': context
        }
        # Test Config
        # self.es_params = {
        #     'host': 'localhost',
        #     'port': '9200'
        # }

    def pull_fa_objects(self, host: str, object_type: str) -> list:
        fa = purestorage.FlashArray(target=self.flasharrays[host]['ip'], api_token=self.flasharrays[host]['auth_token'], verify_https=False)
        if object_type == 'vol':
            try:
                fa_items = fa.list_volumes()      
            except rest.ApiException as e:
                print("Exception: %s\n" % e)
        elif object_type == 'hgroup':
            try:
                fa_items = fa.list_hgroups()      
            except rest.ApiException as e:
                print("Exception: %s\n" % e)
        else:
            raise Exception("Wrong object type specified. Object type should be 'nfs' or 's3'.")

        names = []
        for item in fa_items:
             names.append(item['name'])

        return names

    def pull_fb_objects(self, host: str, object_type: str) -> list:
        fb = PurityFb(self.flashblades[host]['ip'])
        if object_type == 'nfs':
            try:
                fb.login(self.flashblades[host]['auth_token'])
                fb_file_systems = fb.file_systems.list_file_systems()      
            except rest.ApiException as e:
                print("Exception: %s\n" % e)
        elif object_type == 's3':
            try:
                fb.login(self.flashblades[host]['auth_token'])
                fb_file_systems = fb.buckets.list_buckets()      
            except rest.ApiException as e:
                print("Exception: %s\n" % e)
        else:
            raise Exception("Wrong object type specified. Object type should be 'nfs' or 's3'.")

        names = []
        for item in fb_file_systems.items:
            names.append(item.name)

        return names

    def split_file_systems(self, file_systems: list, group_size: int = 5) -> list:
        # Ingest list of file systems, then combine into a list of groups of 5 file systems
        name_list = []
        name_list = [file_systems[i:i + group_size] for i in range(0, len(file_systems), group_size)]
        
        return name_list

    def pull_fa_stats(self, host: str, objects: str, num_days: int = 30) -> list: 
        fa = purestorage.FlashArray(target=self.flasharrays[host]['ip'], api_token=self.flasharrays[host]['auth_token'], verify_https=False)   
        
        try:
            # Need to use the _request() method rather than get_volume() since get_volume() doesn't support multiple volumes
            objects = ','.join(objects)
            path = 'volume?action=monitor&historical='+str(num_days)+'d&names='+str(objects)
            fa_stats = fa._request(method='GET', path=path)      
        except rest.ApiException as e:
            print("Exception: %s\n" % e)
        
        return fa_stats

    def pull_fb_stats(self, host: str, object_type: str, objects: str, num_days: int = 30,interval: int = 86400000) -> BucketPerformanceResponse: 
        # Default num_days is 30, interval time is 1 day in milliseconds
        end = int(time.time() * 1000)   # Convert seconds to milliseconds
        start = int(end - (num_days * interval))
    
        fb = PurityFb(self.flashblades[host]['ip'])
        
        if object_type == 'nfs':
            try:
                fb.login(self.flashblades[host]['auth_token'])
                fb_stats = fb.file_systems.list_file_systems_performance(start_time = start, 
                    end_time = end, resolution = interval, protocol = 'nfs', names = objects)      
            except rest.ApiException as e:
                print("Exception: %s\n" % e)
        elif object_type == 's3':
            try:
                fb.login(self.flashblades[host]['auth_token'])
                fb_stats = fb.buckets.list_buckets_performance(start_time = start, 
                    end_time = end, resolution = interval, names = objects)      
            except rest.ApiException as e:
                print("Exception: %s\n" % e)
        else:
            raise Exception("Wrong object type specified. Object type should be 'nfs' or 's3'.")

        return fb_stats

    def clean_fa_data_for_elasticsearch(self, target_index: str, host: str, stats: list) -> list:
        # Elasticsearch bulk index needs objects in the following format, so we'll add an "_index" key to each 
        # dictionary, rename "time" to "timestamp" and delete the "id" field which could conflict with Elasticsearch's 
        # id representation.
        # {
        #     "_id": 42,
        #     "_routing": 5,
        #     "title": "Hello World!",
        #     "body": "..."
        # }
        
        flasharray_stats = []
        for item in stats:
            flasharray_stats.append({'_index': target_index, 'host': host, 'timestamp': item['time'], 
                'name': item['name'], 'reads_per_sec': item['reads_per_sec'], 'usec_per_read_op': item['usec_per_read_op'],
                'usec_per_write_op': item['usec_per_write_op'], 'writes_per_sec': item['writes_per_sec'], 'input_per_sec': item['input_per_sec'],
                'output_per_sec': item['output_per_sec']})
        
        return flasharray_stats
    
    def clean_fb_data_for_elasticsearch(self, target_index: str, host: str, protocol: str, stats: list) -> list:
        
        flashblade_stats = []
        for item in stats.items:
            flashblade_stats.append({'_index': target_index, 'protocol': protocol, 'host': host, 'timestamp': item.time, 
                'name': item.name, 'bytes_per_op': item.bytes_per_op, 'bytes_per_read': item.bytes_per_read, 
                'bytes_per_write': item.bytes_per_write, 'others_per_sec': item.others_per_sec, 
                'read_bytes_per_sec': item.read_bytes_per_sec, 'reads_per_sec': item.reads_per_sec, 
                'usec_per_other_op': item.usec_per_other_op, 'usec_per_read_op': item.usec_per_read_op,
                'usec_per_write_op': item.usec_per_write_op, 'write_bytes_per_sec': item.write_bytes_per_sec, 
                'writes_per_sec': item.writes_per_sec})
        
        return flashblade_stats

    def create_index(self, target_index: str, target_body: str):
        es = Elasticsearch([self.es_params])
        try:
            es.indices.create(target_index, body=target_body)
        except Exception as e:
            print("Create Index failed: %s\n" % e)

    def debug_to_json_file(self, items, filename: str):
        json_object = json.dumps(items)
        
        with open(filename, "w") as outfile:
            outfile.write(json_object)

    def elasticsearch_bulk_index(self, target_index: str, stats: list):
        # Test HTTP connection to Elasticsearch
        # Test is invalid with ECK due to certificate and authentication requirements
        # try:
        #     res = requests.get('http://' + self.es_params['host'] + ':' + self.es_params['port'])
        # except requests.RequestException as e:
        #     print("Cannot connect to Elasticsearch - exception: %s\n" % e)

        es = Elasticsearch([self.es_params])
        try:
            helpers.bulk(es, stats)
        except Exception as e:
            print("Indexing failed: %s\n" % e)


pt = PureTenant()

# Create FlashBlade index with mappings for milliseconds to timestamp
pt.create_index('flashblade','{"mappings":{"properties":{"timestamp":{"type":"date","format": "epoch_millis"}}}}')

# Process FlashBlade data pull
s3_stats = []
s3_buckets = []
s3_bucket_groups = []
nfs_file_systems = []
nfs_fs_groups = []
nfs_stats = []

for host in pt.flashblades:
    # Bucket and File System objects can only be queried 5 items at a time
    s3_buckets = pt.pull_fb_objects(host, 's3')
    s3_bucket_groups = pt.split_file_systems(s3_buckets)
    for bucket_group in s3_bucket_groups:
        s3_stats = s3_stats + pt.clean_fb_data_for_elasticsearch('flashblade', host, 's3', pt.pull_fb_stats(host, 's3', bucket_group))
    
    nfs_file_systems = pt.pull_fb_objects(host, 'nfs')
    nfs_fs_groups = pt.split_file_systems(nfs_file_systems)
    for group in nfs_fs_groups:
        nfs_stats = nfs_stats + pt.clean_fb_data_for_elasticsearch('flashblade', host, 'nfs', pt.pull_fb_stats(host, 'nfs', group))

# Process FlashArray data pull

vol_stats = []
vols = []
vol_groups = []
hgroups = []

for host in pt.flasharrays:
    vols = pt.pull_fa_objects(host, 'vol')
    # Remove temporary Veeam snaps from list as they are ephemeral
    vols = [x for x in vols if 'VEEAM-ExportLUNSnap' not in x]

    hgroups = pt.pull_fa_objects(host, 'hgroup')
    vol_groups = pt.split_file_systems(vols)
    for group in vol_groups:
        vol_stats = vol_stats + pt.clean_fa_data_for_elasticsearch('flasharray', host, pt.pull_fa_stats(host, group))

# Index data via Elasticsearch
pt.elasticsearch_bulk_index("flashblade", s3_stats)
pt.elasticsearch_bulk_index("flashblade", nfs_stats)
pt.elasticsearch_bulk_index('flasharray', vol_stats)

### Data gathering and indexing is done, data pull and indexing work properly
### NEXT STEPS:
#       Build Kibana dashboards