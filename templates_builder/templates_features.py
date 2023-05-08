#!/usr/bin/python3

import logging
import os
import json
import typing as t
import yaml
import socket
import google.auth
from google.cloud import firestore
from google.cloud import storage
from google.cloud import pubsub_v1
from google.oauth2 import service_account

_type = t.NewType("_type", t.Union[t.MutableMapping[t.Any, t.Any], t.MutableSequence[t.Any]])
_FORMAT = '%(asctime)s %(host)-15s %(user)-8s %(message)s'

_logger_data = d = {'host': socket.gethostname(), 'user': "<google cloud functions engine> |=>"}
logging.basicConfig(format=_FORMAT)
logger = logging.getLogger('Templates Processing')
logger.setLevel("INFO", )


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


def _templates_parsing(data: _type, blob_name: str,
                       firestore_client: t.Optional[google.cloud.firestore.Client] = None) -> t.Iterator:
    """
    :param data:
    :param firestore_client:
    :param blob_name:
    :return:
    """
    if firestore_client:
        _constellations = firestore_client.collection('constellations')
        _constellation_key = blob_name.split('.')[0].split('/')[1] + '-' + data['specification-version']
        logger.info(f"constellations collection now would be updated with new document\
        <{_constellation_key}>", extra=_logger_data)
        doc_ref = _constellations.document(_constellation_key)
        doc_ref.set(data)
        logger.info(f"constellation document has been correctly inserted", extra=_logger_data)

    _components_keys = _keys = ("name", "version", "artifactId", "description",)

    _lambda = lambda items_: [
        {i: k for i, k in zip(_components_keys, (data_["artifactId"] + "-" + data_.get("version", "noversion"),
                                                 data_.get("version", "noversion"),
                                                 data_["artifactId"],
                                                 "" if "alias" not in data_ else data_["alias"]
                                                 )
                              ) \
         } for data_ in items_ \
        ]

    _final_structure = {}
    _final_structure["name"] = blob_name.split(".")[0].split('/')[1]
    _final_structure["version"] = data["specification-version"]
    _products_keys = ("name", "version", "artifactId", "description", "components")
    _final_structure["products"] = [
        {i: k for i, k in zip(_products_keys, (item["artifactId"] + "-" + item.get("version", "noversion"),
                                               item.get("version", "noversion"),
                                               item["artifactId"],
                                               "" if "alias" not in item else item["alias"],
                                               _lambda(item["components"])))}
        for item in data["products"]]
    yield _final_structure


def components_reads(bucket_name: str = "constellations_bucket", directory: str = "devconst") -> t.Iterator:
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

    bucket = storage_client.bucket(bucket_name)
    for blob in list(bucket.list_blobs(prefix=directory)):
        with blob.open('r') as f:
            yield from _components_parsing(json.loads(f.read()))

    # # Mode can be specified as wb/rb for bytes mode.
    # # See: https://docs.python.org/3/library/io.html


def _templates_reads(bucket_name: str = "constellations_bucket",
                     directory: str = "constellations-templates",
                     target_key_path: t.Optional[str] = "ACCESS_KEY") -> t.Iterator:
    """
    :param bucket_name:
    :param directory:
    :param target_key_path:
    :return:
    """
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your new GCS object
    # blob_name = "storage-object-name"

    credentials, project_id = google.auth.default()
    storage_client = storage.Client(project=project_id, credentials=credentials)

    bucket = storage_client.bucket(bucket_name)
    logger.info(f"constellations templates would be read from within directory {directory} of bucket {bucket_name}",
                extra=_logger_data)

    for blob in list(bucket.list_blobs(prefix=directory)):
        with blob.open('r') as f:
            logger.info(f"now blob {blob.name} is being processed", extra=_logger_data)
            logger.info(f"constellation collection would be updated with data coming from blob {blob.name}",
                        extra=_logger_data)
            if target_key_path:
                # gcp_json_credentials_dict = json.load(open("citric-goal-384817-75bfb5ae3622.json"))
                gcp_json_credentials_dict = json.loads(os.environ[target_key_path])
                credentials = service_account.Credentials.from_service_account_info(gcp_json_credentials_dict)
                _firestore_client = firestore.Client(project=gcp_json_credentials_dict['project_id'],
                                                     credentials=credentials)
                yield from _templates_parsing(yaml.safe_load(f.read()), blob_name=blob.name,
                                              firestore_client=_firestore_client)
            else:
                yield from _templates_parsing(yaml.safe_load(f.read()), blob_name=blob.name, )

    # # Mode can be specified as wb/rb for bytes mode.
    # # See: https://docs.python.org/3/library/io.html


def templates_extraction(bucket_name: str = "constellations_bucket",
                         directory: str = "constellations-templates",
                         target_key_path: t.Optional[str] = None, ) -> _type:
    """
    :param bucket_name:
    :param directory:
    :param target_key_path:
    :return:
    """
    return [yaml.safe_load(_) for _ in
            set([yaml.safe_dump(_, sort_keys=False) for _ in
                 _templates_reads(bucket_name=bucket_name, directory=directory, target_key_path=target_key_path)])]


def templates_collect(client: firestore.Client, templates: _type, ) -> \
        t.MutableSequence[t.Any]:
    """
    :param client:
    :param templates:
    :return:
    """

    _components = client.collection('components')

    for data in templates:
        for product in data["products"]:

            for component in product["components"]:
                doc = _components.document(component["name"]).get()
                if not doc.exists:
                    component["parameters"] = {}
                else:
                    if "parameters" in doc.to_dict():
                        component["parameters"] = doc.to_dict()["parameters"]
    return templates


def put_templates(collection: _type,
                  target_key_path: str = "ACCESS_KEY", pubsub_topic: t.Optional[str] = "citric-goal-384817",
                  pubsub_path: t.Optional[str] = "templates") -> t.MutableSequence[str]:
    """
    :param collection:
    :param target_key_path:
    :param pubsub_topic:
    :param pubsub_path:
    :return:
    """
    gcp_json_credentials_dict = json.loads(os.environ[target_key_path])
    credentials = service_account.Credentials.from_service_account_info(gcp_json_credentials_dict)
    _client = firestore.Client(project=gcp_json_credentials_dict['project_id'], credentials=credentials)
    logger.info("firestore object correctly received as input...", extra=_logger_data)
    _templates = _client.collection('templates')

    publisher = pubsub_v1.PublisherClient(credentials=credentials)
    topic_path = publisher.topic_path(pubsub_topic, pubsub_path)
    _final_list = []
    for item in templates_collect(_client, collection):
        logger.info(f"processed item is {item['name']}", extra=_logger_data)
        logger.info(f"now a new document called <{item['name'] + '-' + item['version']}> would be inserted",
                    extra=_logger_data)
        doc_ref = _templates.document(item['name'] + "-" + item['version'])
        doc_ref.set(item)

        logger.info(f"all data of item {item['name'] + '-' + item['version']} have been processed and inserted",
                    extra=_logger_data)
        #
        logger.info(f"now pub-sub service will be triggered for item  {item['name'] + '-' + item['version']}...",
                    extra=_logger_data)

        future = publisher.publish(topic_path, bytes(
            json.dumps({"name": item["name"], "version": item["version"]}), 'utf-8'))
        _results = future.result()
        _final_list.append(_results)
        logger.info(
            f"message with id <{_results}> correctly published and delivered to path {topic_path}",
            extra=_logger_data)
    return _final_list


def put_components(data: _type):
    """
    :param data:
    :return:
    """
    pass


if __name__ == "__main__":
    pass
