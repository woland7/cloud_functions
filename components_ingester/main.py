#!/usr/bin/python3

import flask
from flask import escape
import functions_framework
from google.cloud import storage
from components_features import components_explosion, plho_reference, firestore_operations


@functions_framework.http
def components_ingester(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    ##
    if request_json and 'bucket' in request_json:
        bucket = request_json['bucket']
    elif request_args and 'bucket' in request_args:
        bucket = request_args['bucket']
    else:
        bucket = 'constellations_bucket'
    ##
    if request_json and 'directory' in request_json:
        directory = request_json['directory']
    elif request_args and 'directory' in request_args:
        directory = request_args['directory']
    else:
        directory = 'devconst'
    ##
    if request_json and 'plho_dir' in request_json:
        plhos_dir = request_json['plho_dir']
    elif request_args and 'plho_dir' in request_args:
        plhos_dir = request_args['plho_dir']
    else:
        plhos_dir = 'plho_origin'

    ##
    if request_json and 'plho_file' in request_json:
        plho_file = request_json['plho_file']
    elif request_args and 'plho_file' in request_args:
        plho_file = request_args['plho_file']
    else:
        plho_file = None

    components_collection = components_explosion(bucket_name=bucket, directory=directory)
    ##
    if plho_file:
        plhos_data = plho_reference(directory=plhos_dir, plho_file=plho_file)
    else:
        plho_data = None

    ##
    if request_json and 'target_key_path' in request_json:
        target_key_path = request_json['target_key_path']
    elif request_args and 'target_key_path' in request_args:
        target_key_path = request_args['target_key_path']
    else:
        target_key_path = None
    firestore_operations(components_collection=components_collection, plho_info=plho_data,target_key_path=target_key_path)

    return 'OK'


# [END]


if __name__ == "__main__":
    pass

