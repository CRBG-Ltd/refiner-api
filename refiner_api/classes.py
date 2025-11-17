from io import StringIO
from types import NoneType
from lxml.etree import XMLSyntaxError
import pandas as pd
import requests
from log_manager import LogManager

class ApiSetupError(ValueError):
    err_dict = {'api_down': {'name': 'API Service Not Running',
                             'description': 'The API Initialisation call failed\nThe API {} returned the error {}'},
                'api_key': {'name': 'API Response Error',
                            'description': 'Missing data fields: "{}"\n{}'},
                'fld_drop': {'name': 'Blob Error',
                             'description': 'Field delete: "{}" does not exist in {}'},
                'job_id': {'name': 'Job ID Error',
                             'description': 'The folder for Job ID: "{}" does not exist.{}'},
                'sp_log': {'name': 'SharePoint Logging Error',
                             'description': '{} {}'}}

    def __init__(self, err_vars):
        """
Raise this when there's an application error
        :param err_vars:
        """
        if isinstance(err_vars, list):
            if len(err_vars) == 3:
                e = self.err_dict.get(err_vars[0])
                if e:
                    self.error = e['name']
                    self.description = e['description'].format(err_vars[1], err_vars[2])
            else:
                self.error = err_vars.pop(0)
                self.description = '\n'.join(err_vars)
        else:
            self.error = 'Unknown Error'
            self.description = str(err_vars)

        super().__init__(str(self))  # Call the base class constructor

    def __str__(self):
        return '\n'.join(['Refiner Processing Manager', self.error, self.description])


class ApiServer:
    __check_str = 'address1=AFD%20Software%20Ltd,Lezayre,Ramsey,Isle%20of%20Man,IM7%202DZ'
    sleep_timer = 1  # Number of seconds to wait before attempting retry
    sleep_timer_increment = 3  # Number of seconds to increase wait times
    online = True

    def __init__(self, name, host, licence, password, root, api_log, multi_thread_mode=False, refiner_fields=None, frmt='xml'):
        """
A fully configured API Server object that will handle the calls for individual or batch address lookups
        :param name: str - the internal reference for this instance
        :param host: str - the DNS name or IP address for the server
        :param licence: str - the licence ID
        :param password: str - the password for this specific licence
        :param root: str - the URL root string
        :param api_log: str - the URL of the log file or LogManager object (str is deprecated)
        :param multi_thread_mode: bool - True if more than one ApiServer will be used
        :param refiner_fields: list - the fields to be returned in the API call
        :param frmt: str - the output format from the API call
        """
        self.name = name
        self.root = root.format(host, licence, password)
        if not refiner_fields:
            refiner_fields = ['UDPRN', 'UPRN', 'Organisation', 'Property', 'Street', 'Locality', 'Town',
                              'PostalCounty', 'Postcode']
        self.uri_stem = '&'.join([self.root, 'format=' + frmt, 'fields=' + '@'.join(refiner_fields)])
        self.data = None
        self.multi_thread_mode = multi_thread_mode
        if isinstance(api_log, LogManager):
            self.log = api_log
        else:
            self.log = LogManager()
            log_file_url = api_log if isinstance(api_log, str) else api_log['AppInfo']['api_log_file'].format(self.name)
            self.log.start_text_logging(log_file_url, reset_log=False)
        self.init_result = self.check_api(self.root)
        if self.init_result != 'Working':
            self.online = False
            raise ApiSetupError(['API Settings Error', self.init_result])

        self.log.create_entry(['API Server initialised', self.name, 'Running in ' +
                               ('Multi' if self.multi_thread_mode else 'Single') +
                               '-threaded Mode'])

    def __str__(self):
        return self.uri_stem

    def call(self, fields=None, address=None):
        if not address:
            address = self.__check_str
        url = '&'.join([self.root, fields, address]) if fields else '&'.join([self.uri_stem, address])
        response = requests.get(url)
        return response

    def check_api(self, root):
        """
Checks that the Refiner API service is running properly.
        :param root: The URL root string generated from ini.Root, which must include the credentials
        :return: str: The value from result dict, from the key result_code
        """
        result = {'200': 'Working',
                  '-1': 'Test Lookup Address Failed - Check service manually',
                  '-7': f'{self.name} Authentication Error - Check licence (serial) and password',
                  'H1': f'{self.name} Not Responding - Check the host IP address',
                  'H2': f'{self.name} Not Responding - Check the host port number',
                  'S1': f'''You have setup the {self.name} API for XML responses. We can currently only handle JSON. 
Ensure that "frmt='json'" is included in your ApiServer instantiation.''',
                  'NA': f'{self.name} Returned Unexpected Result ([urc])- Check service manually'}
        url = '&'.join([root, 'fields=UDPRN', self.__check_str])
        # print('ApiServer.check_api.response.url', url)
        try:
            response = self.call()
            # print('ApiServer.check_api.response.string', response.text)
            in_dict = response.json()
            result_code = in_dict.get("ResultCode")
            if not result_code:
                result_code = in_dict.get("Result")
            if str(result_code) not in result:
                result['NA'] = result['NA'].replace('[urc]', result_code)
                result_code = 'NA'
        except requests.exceptions.ConnectTimeout:
            result_code = 'H1'
        except requests.exceptions.ConnectionError:
            result_code = 'H2'
        except (requests.exceptions.JSONDecodeError, requests.exceptions.CompatJSONDecodeError):
            result_code = 'S1'
        return result[str(result_code)]


class ApiResponse:
    def __init__(self, in_dict):
        self.result_code = in_dict.get("ResultCode")
        self.closeness = in_dict.get("Closeness")
        self.property = in_dict.get("Property")
        self.street = in_dict.get("Street")
        self.locality = in_dict.get("Locality")
        self.town = in_dict.get("Town")
        self.postal_county = in_dict.get("PostalCounty")
        self.udprn = in_dict.get("UDPRN")
        self.postcode = in_dict.get("Postcode")

    def __repr__(self, delimiter=', '):
        out_str = delimiter.join([self.property, self.street, self.locality, self.town, self.postal_county,
                                  self.udprn, self.postcode])
        return out_str + '\nResult: ' + str(self.result_code) + '\nScore: ' + str(self.closeness)


class ApiCall:
    logs = []
    data = ''
    first_row = []

    def __init__(self, api_servers, api_server_id, refiner_fields, batch_data=None):
        """
Set up an API call to a Refiner API server
        :param api_servers: list - all available Refiner API servers
        :param api_server_id: int - list index of the server to be used
        :param refiner_fields: list
        :param batch_data:
        """
        self.api_servers = api_servers
        self.api_server_id = api_server_id
        self.out_fields = refiner_fields
        if batch_data:
            if isinstance(batch_data, list):
                api_str = batch_data[0]
                self.data = api_str.encode('utf-8')
                self.first_row = api_str.split('\n')[0]
                self.index = batch_data[1]
            else:
                self.data = batch_data.encode('utf-8')
                self.first_row = batch_data.split('\n')[0]

    def get_server(self, switch=False):
        if switch:
            self.api_server_id += 1
            if self.api_server_id >= len(self.api_servers):
                self.api_server_id = 0
        return self.api_servers[self.api_server_id]

    def call_api(self, api_str=None, index=None):
        """
Send batch call to Refiner API
        :return: pd.DataFrame
        """
        api_server = self.get_server()
        if api_str:
            self.data = api_str.encode('utf-8')
            self.first_row = api_str.split('\n')[0]
        if index:
            self.index = index
        batch = pd.DataFrame()
        try:
            response = requests.post(api_server.uri_stem, data=self.data)
            r_out = response.text
            # print('refiner_api.API.call_api.response.text', r_out)
            try:
                batch = pd.read_json(StringIO(r_out)).fillna('').astype(str)  # .iloc[1:]
            except XMLSyntaxError:
                api_server.log.create_entry([api_server.name, self.data, r_out])
            if batch.shape[1] > 2:
                batch['UDPRN'] = batch['UDPRN'].apply(lambda r: ('00000000' + r.split('.')[0])[-8:])
                batch['UPRN'] = batch['UPRN'].apply(lambda r: ('000000000000' + r.split('.')[0])[-12:])
                batch['Closeness'] = batch['Closeness'].apply(lambda r: r.split('.')[0])
            else:
                for f in self.out_fields:
                    batch[f] = ''
                batch['UDPRN'] = '00000000'
                batch['UPRN'] = '000000000000'
                batch['Closeness'] = '0'
            retry_count = 10
        except KeyError:
            raise ApiSetupError(['api_key', batch, self.data])
        # except requests.exceptions.ConnectionError:
        #     api_server.log.create_entry(['Call Failed - ConnectionError', api_server.name, self.first_row])

        if not isinstance(self.index, NoneType):
            # print('**   refiner_api.API.call_api.index', self.index, batch.index)
            batch.index = self.index
        # api_server.log.create_entry([api_server.name, self.first_row, batch.iloc[0]])
        return batch


class Config:
    main_specs = {}
    file_path = {}

    # Immutable attributes (cannot be modified after assignment)
    class ImmutableDict(dict):
        def __setitem__(self, key, value):
            raise TypeError("This dictionary is immutable and cannot be changed.")

        def __delitem__(self, key):
            raise TypeError("This dictionary is immutable and cannot be changed.")

        def clear(self):
            raise TypeError("This dictionary is immutable and cannot be cleared.")

        def update(self, *args, **kwargs):
            raise TypeError("This dictionary is immutable and cannot be updated.")

        def pop(self, key, default=None):
            raise TypeError("This dictionary is immutable and cannot be changed.")

        def popitem(self):
            raise TypeError("This dictionary is immutable and cannot be changed.")

        def setdefault(self, key, default=None):
            raise TypeError("This dictionary is immutable and cannot be changed.")

    class ImmutableList(list):
        def __setitem__(self, index, value):
            raise TypeError("This list is immutable and cannot be changed.")

        def __delitem__(self, index):
            raise TypeError("This list is immutable and cannot be deleted.")

        def append(self, value):
            raise TypeError("This list is immutable and cannot be changed.")

        def extend(self, iterable):
            raise TypeError("This list is immutable and cannot be extended.")

        def insert(self, index, value):
            raise TypeError("This list is immutable and cannot be changed.")

        def remove(self, value):
            raise TypeError("This list is immutable and cannot be changed.")

        def pop(self, index=-1):
            raise TypeError("This list is immutable and cannot be changed.")

        def clear(self):
            raise TypeError("This list is immutable and cannot be cleared.")

    def __init__(self, settings=None, settings_file_url=None):
        """
The Config class is a custom object for setting global variables in a way that is more controllable and therefore more
predictable. Usually instantiated as the *ini* object, Config contains both mutable and immutable objects. Immutable
objects prevent devs inadvertently overwriting the value of list or dictionary item, thus averting the classic problem
with global variables. Although they do offer a limited layer of security, they should not be considered secure.
        """
        if settings_file_url:
            import sys
            import importlib
            import pathlib
            file_path = pathlib.Path(settings_file_url)
            sys.path.insert(0, str(file_path.parent))
            global_app_settings = importlib.import_module(file_path.stem)

            # import everything global_app_settings defines into the current namespace
            app_settings = {}
            for name in dir(global_app_settings):
                if not name.startswith("_"):
                    app_settings[name] = getattr(global_app_settings, name)
            self.__APP_SETTINGS = Config.ImmutableDict(app_settings)
        elif isinstance(settings, dict):
            self.__APP_SETTINGS = Config.ImmutableDict(settings)


        self.app_log = LogManager(application_error_log=self.__APP_SETTINGS.get('ApplicationErrorLog'))
        self.api_log = LogManager(application_error_log=self.__APP_SETTINGS.get('ApplicationErrorLog'))

        # Define immutable attributes here, but remember to add the property, to be able to retrieve the attribute
        # api_servers = self.set_apis(self.__APP_SETTINGS.get('ApiServerInfo'))
        self._API_SERVERS = Config.ImmutableList(self.set_apis(self.__APP_SETTINGS.get('ApiServerInfo')))

        # Define mutable attributes here
        # These can be overwritten at run time

        self.verbose_logging = False
        self.multi_threaded_mode = self.__APP_SETTINGS.get('MultiThreadedMode', False)

        self.file_log = None
        self.fail_over_api_servers = []


    # Properties to access immutable attributes without allowing modification
    @property
    def api_batch_size(self):
        return self.__APP_SETTINGS.get('ApiBatchSize')

    @property
    def api_retries(self):
        return self.__APP_SETTINGS.get('ApiRetryLimit')

    @property
    def api_servers(self):
        return self._API_SERVERS

    @property
    def default_delimiter(self):
        return self.__APP_SETTINGS.get('DefaultDelimiter')

    @property
    def default_output_fields(self):
        return self.__APP_SETTINGS.get('DefaultOutFields')

    @property
    def refiner_postcode_field(self):
        return self.__APP_SETTINGS.get('RefinerPostcodeField')

    @property
    def refiner_output_fields(self):
        return self.__APP_SETTINGS.get('RefinerOutputFields')

    def set_apis(self, api_server_info, refiner_request_fields=None):
        root = r'http://{}/v1/refiner/GBR/clean?serial={}&password={}'
        multi_threaded_mode = (len(api_server_info) > 1)
        api_servers = []
        for si in api_server_info:
            api_server = ApiServer(si['name'], si['host'], si['licence'], si['password'], root, self.api_log,
                                   multi_threaded_mode, refiner_request_fields, frmt='json')
            if api_server.online:
                api_servers.append(api_server)
        print(f"{len(api_servers)} server{'' if len(api_servers) == 1 else 's'} initialised")
        return api_servers


