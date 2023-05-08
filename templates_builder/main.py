#!/usr/bin/python3


from flask import escape
import functions_framework
from google.cloud import storage
from templates_features import put_templates, templates_extraction



# [START functions]
@functions_framework.http
def templates_builder(request):
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

    if request_json and 'bucket' in request_json:
        bucket = request_json['bucket']
    elif request_args and 'bucket' in request_args:
        bucket = request_args['bucket']
    else:
        bucket = 'constellations_bucket'
    if request_json and 'directory' in request_json:
        directory = request_json['directory']
    elif request_args and 'directory' in request_args:
        directory = request_args['directory']
    else:
        directory = 'constellations-templates'
    templates_collection = templates_extraction(bucket_name=bucket, directory=directory)

    _messages_ids = put_templates(templates_collection)
    return 'OK'


# [END]


if __name__ == "__main__":
    pass
