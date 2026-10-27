from concurrent.futures import ThreadPoolExecutor
import threading
import pandas as pd
import requests
import time
from stop_watch import StopWatch
from .classes import ApiCall, ApiServer


def get_apis(log, api_server_info, refiner_request_fields=None):
    root = r'http://{}/v1/refiner/GBR/clean?serial={}&password={}'
    multi_threaded_mode = (len(api_server_info) > 1)
    api_servers = []
    for si in api_server_info:
        api_server = ApiServer(si['name'], si['host'], si['licence'], si['password'], root, log,
                               multi_threaded_mode, refiner_request_fields, frmt='json')
        if api_server.online:
            api_servers.append(api_server)
    print(f"{len(api_servers)} server{'' if len(api_servers) == 1 else 's'} initialised")
    return api_servers


def check_batch(api):
    if api.stop_event.is_set():
        return None

    retry_count = 0
    calls_failed = 0
    api_server =  api.api_servers[api.api_server_id]
    sleep_timer = api_server.sleep_timer
    result = pd.DataFrame()

    while retry_count < 10:
        # print('refiner_api.check_batch: retry_count', retry_count)
        retry_count += 1
        try:
            result = api.call_api()
            if isinstance(result, pd.DataFrame):
                retry_count = 10
            else:
                print('refiner_api.check_batch: return type is', type(result))
        except requests.exceptions.ConnectionError:
            api_server.log.create_entry(['Call Failed - ConnectionError', retry_count])
            calls_failed += 1
            time.sleep(sleep_timer)
            sleep_timer += api_server.sleep_timer_increment
    return result


def check_addresses(ini, blob):
    """
    Takes a loaded Blob object and checks the addresses in it against the Refiner API.

    :param ini: Config object with global variables (api_log, api_servers, api_batch_size, api_retries,
                job_specs['Internal']['LogUrl'], refiner_output_fields, multi_threaded_mode)
    :param blob: data_asset_manager.Blob object containing address data and metadata, including the address fields to be used in the API call.
    :return: Blob object with API results appended to the data_frame attribute
    """
    def prep_row(row, in_address_fields):
        """
        Concatenates the address fields for parsing in the API call.
        :param: list of address field names
        :return: data row with concatenated address fields in the 'api' column.
        """
        out_addr = []
        for f in in_address_fields:
            txt = row[f].strip()
            if len(txt) > 0:
                out_addr.append(txt)
        return '~'.join(out_addr)

    def check_chunk(chunk_blob):
        """
        Takes a Blob of *FileIteratorChunkSize* addresses and parses it, in batches of *ApiBatchSize* rows, through the
        *ApiServerInfo*. If there is more than one *ApiServerInfo*, and *MultiThreadedMode* is True, batches will be spread
        across all of them.
        :param chunk_blob:
        :return: The updated Blob object containing the API results.
        """

        def process_chunk(**thread_objects):
            """
        Run segment processes in parallel using multiple processors, or in series
            :param thread_objects: **kwargs
            :return: pd.DataFrame with API results
            """
            api_servers = thread_objects.get('apis')
            batch_list = thread_objects.get('address_list')
            refiner_fields = thread_objects.get('refiner_fields')
            batch_index = thread_objects.get('row_ids')
            thread_count = len(batch_list)
            if len(api_servers) > 1 or thread_objects.get('multi_thread_mode'):
                proc_list = []
                while len(batch_list) > 0:
                    s_id = 0  # Selected API Server ID
                    while s_id < len(api_servers) and len(batch_list) > 0:
                        # batch = batch_list.pop(0)
                        api_call = ApiCall(api_servers, s_id, refiner_fields, batch_list.pop(0))
                        proc_list.append(api_call)
                        # proc_list.append([api_servers, s_id, refiner_fields, batch])
                        # proc_list.append(API(api_servers, s_id, batch_list.pop(0), refiner_fields))
                        s_id += 1

                MAX_THREADS = 30
                with ThreadPoolExecutor(max_workers=min(thread_count, MAX_THREADS)) as executor:
                    process_results = list(executor.map(check_batch, proc_list))
            else:
                ini.api_log.create_entry(['Multi-thread disabled'])
                process_results = []
                for batch in batch_list:
                    api_call = ApiCall(api_servers, 0, refiner_fields, batch)
                    # api = [api_servers, 0, refiner_fields, batch]
                    batch_df = check_batch(api_call)
                    process_results.append(batch_df)

            chunk_df = pd.concat(process_results, ignore_index=False)

            return chunk_df

        ts = StopWatch()
        # print('*    subroutines.check_addresses.check_chunk.chunk_blob', chunk_blob.data_frame.index)
        ini.api_log.start_text_logging(ini.job_specs['Internal']['LogUrl'], reset_log=True)

        in_df = chunk_blob.data_frame
        # first_row = in_df.index.min()
        # last_row = in_df.index.max()
        first_row = 0
        last_row = in_df.shape[0]
        api_data = []
        batch_count = 0
        df_slice = pd.DataFrame()

        while first_row < last_row:
            batch_end = first_row + ini.api_batch_size
            df_slice = in_df.iloc[first_row:batch_end]
            # print('*    subroutines.check_addresses.check_chunk.df_slice', df_slice.index)
            api_data.append(['\n'.join(df_slice['api']), df_slice.index])
            first_row += ini.api_batch_size
            batch_count += 1

        df_out = process_chunk(multi_thread_mode=ini.multi_threaded_mode,  # optional
                               apis=ini.api_servers,
                               address_list=api_data,
                               refiner_fields=ini.refiner_output_fields,
                               retry_limit=ini.api_retries)
        ini.api_log.create_entry(['Chunk complete: Batch Count ', batch_count, ' Time taken ', ts.check_lap()])
        return df_out

    blob.data_frame['api'] = blob.data_frame.apply(lambda r: prep_row(r, blob.address_fields), axis=1)
    blob.load_data(check_chunk(blob))

    return blob

def check_data_frame(refiner_settings, df, address_fields):
    """
    :param refiner_settings: dict containing:- root, apis, servers, threaded_mode, return_fields, refiner_postcode_field, api_batch_size, api_chunk_size, api_retries, log_folder, api_log, stop_on_error
    :param df: DataFrame, including the address fields to be used in the API call.
    :param address_fields: list of address field names to be included in API call
    :return: DataFrame with API results appended
    """
    def prep_row(row, in_address_fields):
        """
        Concatenates the address fields for parsing in the API call.
        :param: list of address field names
        :return: data row with concatenated address fields in the 'api' column.
        """
        out_addr = []
        for f in in_address_fields:
            txt = row[f].strip()
            if len(txt) > 0:
                out_addr.append(txt)
        return '~'.join(out_addr)

    def check_df(in_df):
        """
        Takes a Blob of *FileIteratorChunkSize* addresses and parses it, in batches of *ApiBatchSize* rows, through the
        *ApiServerInfo*. If there is more than one *ApiServerInfo*, and *MultiThreadedMode* is True, batches will be spread
        across all of them.
        :param in_df: pd.DataFrame
        :return: The updated Blob object containing the API results.
        """

        def process_data(**thread_objects):
            """
        Run segment processes in parallel using multiple processors, or in series
            :param thread_objects: **kwargs
            :return: pd.DataFrame with API results
            """
            batch_list = thread_objects.get('address_list')
            thread_count = len(batch_list)
            stop_event = threading.Event()
            error_count = 0
            apsc = len(refiner_settings['servers'])
            if apsc > 1 or thread_objects.get('multi_thread_mode'):
                proc_list = []
                while len(batch_list) > 0:
                    s_id = 0  # Selected API Server ID
                    while s_id < apsc and len(batch_list) > 0:
                        api_call = ApiCall(settings=refiner_settings,
                                           server_id=s_id,
                                           batch_data=batch_list.pop(0))
                        api_call.stop_event = stop_event
                        proc_list.append(api_call)
                        s_id += 1

                MAX_THREADS = 30
                process_results = []
                with ThreadPoolExecutor(max_workers=min(thread_count, MAX_THREADS)) as executor:
                    for result in executor.map(check_batch, proc_list):
                        if isinstance(result, Exception):
                            error_count += 1
                            if error_count >= error_limit:
                                stop_event.set()
                                break
                        elif result is not None:
                            process_results.append(result)
            else:
                log.create_entry(['Multi-thread disabled'])
                process_results = []
                for batch in batch_list:

                    if stop_event.is_set():
                        break

                    api_call = ApiCall(settings=refiner_settings,
                                       server_id=0,
                                       batch_data=batch)
                    api_call.stop_event = stop_event

                    result = check_batch(api_call)

                    if isinstance(result, Exception):
                        error_count += 1
                        if error_count >= error_limit:
                            stop_event.set()
                            break
                    elif result is not None:
                        process_results.append(result)

            chunk_df = pd.concat(process_results, ignore_index=False)

            return chunk_df

        time_tracker = StopWatch()
        log.start_text_logging(refiner_settings['log_folder'] + 'api_log.log', reset_log=True)

        first_row = 0
        last_row = in_df.shape[0]
        api_data = []
        batch_count = 0
        df_slice = pd.DataFrame()

        while first_row < last_row:
            batch_end = first_row + refiner_settings['api_batch_size']
            df_slice = in_df.iloc[first_row:batch_end]
            # Concatenate all the concatenated addresses from the batches, using a linefeed as a delimiter.
            api_data.append(['\n'.join(df_slice['api']), df_slice.index, first_row, error_limit])
            first_row += refiner_settings['api_batch_size']
            batch_count += 1

        df_returned = process_data(
            settings=refiner_settings,
            address_list=api_data
        )
        return df_returned

    error_limit = refiner_settings.get('stop_on_error', 1)
    log = refiner_settings['api_log']
    df['api'] = df.apply(lambda r: prep_row(r, address_fields), axis=1)
    df_out = check_df(df)
    if not isinstance(df_out, pd.DataFrame):
        log.create_entry(['API call failed', df_out])
        df_out = pd.DataFrame(columns=refiner_settings['return_fields'])

    return df_out
