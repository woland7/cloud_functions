#!/usr/bin/python3

import logging
import os
import json
import typing as t
import yaml
import socket
from google.cloud import firestore
from google.cloud import storage
from google.oauth2 import service_account
import google.auth
from google.cloud import storage

_type = t.NewType("_type", t.Union[t.MutableMapping[t.Any, t.Any], t.MutableSequence[t.Any]])
_FORMAT = '%(asctime)s %(host)-15s %(user)-8s %(message)s'

_logger_data = d = {'host': socket.gethostname(), 'user': "<google cloud functions engine> |=>"}
logging.basicConfig(format=_FORMAT)
logger = logging.getLogger('Constellations components Processing')
logger.setLevel(logging.INFO)


def _components_parsing(data: _type) -> t.Iterator:
    """
    :param data:
    :return:
    """
    _keys = ("name", "artifactId", "version", "description", "parameters")
    for item in data["products"]:
        for component in item["components"]:
            yield {i: k for i, k in zip(_keys, \
                                        (component["artifactId"] + "-" + component.get("version", "noversion"),
                                         component["artifactId"],
                                         component.get("version", "noversion"), "", {item["name"]: \
                                            {
                                                key: value for key, value \
                                                in item.items() if key != "name"} \
                                             for item in component["parameters"] \
                                             for key, value in item.items()
                                         } \
                                             if "parameters" in component else {}

                                         ))}


def _components_reads(bucket_name: str = "constellations_bucket", directory: str = "devconst") -> t.Iterator:
    """
    :param bucket_name:
    :param directory:
    :return:
    """
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your new GCS object
    # blob_name = "storage-object-name"
    credentials, project_id = google.auth.default()
    storage_client = storage.Client(project=project_id, credentials=credentials)
    logger.info(f"constellations components would be read from within directory {directory} of bucket {bucket_name}",
                extra=_logger_data)

    bucket = storage_client.bucket(bucket_name)

    for blob in list(bucket.list_blobs(prefix=directory)):
        with blob.open('r') as f:
            logger.info(f"now blob {blob.name} is being processed", extra=_logger_data)
            yield from _components_parsing(json.loads(f.read()))


def components_explosion(bucket_name: str = "constellations_bucket", directory: str = "devconst") -> _type:
    """
    :param bucket_name:
    :param directory:
    :return:
    """
    _input = [json.dumps(a) for a in _components_reads(bucket_name=bucket_name, directory=directory)]
    logger.info(f"process started with {str(len(_input))} constellations components to be parsed",
                extra=_logger_data)
    _data = [json.loads(_) for _ in
             set(_input)]
    logger.info(f"{str(len(_data))} distinct components' paths have been generated", extra=_logger_data)

    return _data


def plho_reference(bucket_name: str = "constellations_bucket", directory: str = "plho_origin",
                   plho_file: str = "") -> _type:
    """
    :param bucket_name:
    :param directory:
    :param plho_file:
    :return:
    """
    credentials, project_id = google.auth.default()
    _client = storage.Client(project=project_id, credentials=credentials)
    with _client.get_bucket(bucket_name).blob(plho_file).open('r') as _input:
        _data = json.loads(_input.read())
    _client.close()
    return _data


def firestore_operations(components_collection: _type, plho_info: t.Optional[_type] = None, target_key_path:t.Optional[str]=None) -> None:
    """
    :param components_collection:
    :param plho_info:
    :return:
    """
    credentials, project_id = google.auth.default()
    if not target_key_path:
        _client = firestore.Client(project=project_id, credentials=credentials)
    else:
        gcp_json_credentials_dict = json.loads(os.environ[target_key_path])
        credentials = service_account.Credentials.from_service_account_info(gcp_json_credentials_dict)
        _client = firestore.Client(project=gcp_json_credentials_dict['project_id'], credentials=credentials)

    components = _client.collection('components')
    for comp in components_collection:
        logger.info(f"component processed is {comp['name']}", extra=_logger_data)
        comp_doc_ref = components.document(comp['name'])
        for p_key, p_value in comp['parameters'].items():
            if plho_info:
                if 'origin' in plho_info[p_key].keys():
                    p_key['origin'] = plho_info[p_key]['origin']
        comp_doc_ref.set(comp)
        logger.info(f"{comp['name']}'s data have been inserted", extra=_logger_data)
    _client.close()
