import re
import os
import json
from typing import List, Tuple

from google.cloud import documentai_v1 as documentai
from google.cloud import storage
from google.api_core.operation import Operation

# Reading environment variables
gcs_output_uri_prefix = os.environ.get('GCS_OUTPUT_URI_PREFIX')
project_id = os.environ.get('GCP_PROJECT')
location = os.environ.get('PARSER_LOCATION')
processor_id = os.environ.get('PROCESSOR_ID')
timeout = int(os.environ.get('TIMEOUT'))

client_options = {
    "api_endpoint": f"{location}-documentai.googleapis.com"
}

docai_client = documentai.DocumentProcessorServiceClient(
    client_options=client_options)

storage_client = storage.Client()

ACCEPTED_MIME_TYPES = set(['application/pdf', 'image/jpeg',
                           'image/png', 'image/tiff', 'image/gif'])

def create_json(json_object, bucketname, filename):
    '''
    this function will create json object in
    google cloud storage
    '''
    print("[INFO] Stared: create_json")
    source_bucket = storage_client.bucket(bucketname)
    # create a blob
    blob = source_bucket.blob(filename)
    # upload the blob 
    blob.upload_from_string(
        data=json.dumps(json_object),
        content_type='application/json'
        )
    result = filename + ' upload complete'
    return {'response' : result}

def trim_text(text: str):
    """
    Remove extra space characters from text (blank, newline, tab, etc.)
    """
    return text.strip().replace("\n", " ")

def extract_document_entities(document: documentai.Document) -> dict:
    """
    Get all entities from a document and output as a dictionary
    Flattens nested entities/properties
    Format: entity.type_: entity.mention_text OR entity.normalized_value.text
    """
    document_entities = {}

    def extract_document_entity(entity: documentai.Document.Entity):
        """
        Extract Single Entity and Add to Entity Dictionary
        """
        entity_key = entity.type_.replace('/', '_')
        normalized_value = getattr(entity, "normalized_value", None)

        new_entity_value = normalized_value.text if normalized_value else entity.mention_text

        existing_entity = document_entities.get(entity_key)

        # For entities that can have multiple (e.g. line_item)
        if existing_entity:
            # Change Entity Type to a List
            if not isinstance(existing_entity, list):
                existing_entity = list([existing_entity])

            existing_entity.append(new_entity_value)
            document_entities[entity_key] = existing_entity
        else:
            document_entities.update({
                entity_key: new_entity_value
            })

    for entity in document.entities:
        # Fields detected. For a full list of fields for each processor see
        # the processor documentation:
        # https://cloud.google.com/document-ai/docs/processors-list
        extract_document_entity(entity)

        # Properties are Sub-Entities
        for prop in entity.properties:
            extract_document_entity(prop)

    return document_entities


def _batch_process_documents(
    project_id: str,
    location: str,
    processor_id: str,
    gcs_input_uri: str,
    gcs_output_uri: str,
) -> Operation:
    """
    Constructs a request to process a document using the Document AI
    Batch Method.
    """

    # The full resource name of the processor, e.g.:
    # projects/project-id/locations/location/processor/processor-id
    # You must create new processors in the Cloud Console first
    resource_name = docai_client.processor_path(
        project_id, location, processor_id)

    # Load GCS Input URI Prefix into Input Config Object
    input_config = documentai.BatchDocumentsInputConfig(
        gcs_prefix=documentai.GcsPrefix(
            gcs_uri_prefix=gcs_input_uri
        )
    )

    # Cloud Storage URI for Output directory
    gcs_output_config = documentai.DocumentOutputConfig.GcsOutputConfig(
        gcs_uri=gcs_output_uri
    )

    # Load GCS Output URI into Output Config Object
    output_config = documentai.DocumentOutputConfig(
        gcs_output_config=gcs_output_config)

    # Configure Process Request
    request = documentai.BatchProcessRequest(
        name=resource_name,
        input_documents=input_config,
        document_output_config=output_config
    )

    # Future for long-running operations returned from Google Cloud APIs.
    operation = docai_client.batch_process_documents(request)

    return operation

def get_document_protos_from_gcs(output_bucket: str, output_directory: str) -> List[documentai.Document]:
    """
    Download document proto output from GCS. (Directory)
    """
    print("Executing get_document_protos_from_gcs")
    # List of all of the files in the directory `gs://gcs_output_uri/operation_id`
    blob_list = list(storage_client.list_blobs(
        output_bucket, prefix=output_directory))
    document_protos = []
    for blob in blob_list:
        # Document AI should only output JSON files to GCS
        if ".json" in blob.name:
            print("Fetching from " + blob.name)
            document_proto = documentai.types.Document.from_json(
                blob.download_as_bytes())
            print("Document Proto Found:", document_proto)
            document_protos.append(document_proto)
        else:
            print(f"Skipping non-supported file type {blob.name}")
    print("Document Proto Count:", len(document_protos))
    return document_protos

def keys_exists(element, *keys):
    '''
    Check if *keys (nested) exists in `element` (dict).
    '''
    if not isinstance(element, dict):
        raise AttributeError('keys_exists() expects dict as first argument.')
    if len(keys) == 0:
        raise AttributeError('keys_exists() expects at least two arguments, one given.')

    _element = element
    for key in keys:
        try:
            _element = _element[key]
        except KeyError:
            return False
    return True

def get_document_json_from_gcs(output_bucket: str, output_directory: str):
    """
    Download document json output from GCS. (Directory)
    """
    print("Executing get_document_json_from_gcs")
    # List of all of the files in the directory `gs://gcs_output_uri/operation_id`
    blob_list = list(storage_client.list_blobs(
        output_bucket, prefix=output_directory))
    json_objects = []
    for blob in blob_list:
        # Document AI should only output JSON files to GCS
        if ".json" in blob.name:
            print("Fetching from " + blob.name)
            json_data_string = blob.download_as_string()
            print("json sting type", type(json_data_string))
            print("JSON String Data:", json_data_string)
            json_data = json.loads(json_data_string)
            print("json data type", type(json_data))
            print("JSON Data:", json_data)
            print("JSON Pages:", json_data["pages"])
            for page in json_data["pages"]:
                if keys_exists(page, "formFields"):
                    for field in page["formFields"]:
                        if keys_exists(field, "fieldName", "textAnchor", "content") and keys_exists(field, "fieldName", "confidence") and keys_exists(field, "fieldValue", "textAnchor", "content") and keys_exists(field, "fieldValue", "confidence"):
                            fieldName = trim_text(field["fieldName"]["textAnchor"]["content"])
                            fieldNameConfidence = trim_text(str(field["fieldName"]["confidence"]))
                            fieldValue = trim_text(field["fieldValue"]["textAnchor"]["content"])
                            fieldValueConfidence = trim_text(str(field["fieldValue"]["confidence"]))
                            fieldItem = {'fieldName': fieldName, 'fieldNameConfidence': fieldNameConfidence, 'fieldValue': fieldValue, 'fieldValueConfidence': fieldValueConfidence}
                            print("Field Item:", fieldItem)
                            json_objects.append(fieldItem)
            print("Json Objects:", json_objects)
        else:
            print(f"Skipping non-supported file type {blob.name}")
    return json_objects

def process_invoice(event, context):
    """
    Extract Invoice Entities and Save to BQ
    """
    print("[INFO] Started: process_invoice")
    input_bucket = event.get("bucket")
    input_filename = event.get("name")
    mime_type = event.get("contentType")

    if not input_bucket or not input_filename:
        print("No bucket or filename provided")
        return

    if mime_type not in ACCEPTED_MIME_TYPES:
        print('Cannot parse the file type: ' + mime_type)
        return

    print('Mime Type: ' + mime_type)

    gcs_input_uri = f'gs://{input_bucket}/{input_filename}'

    print("Input File: " + gcs_input_uri)

    destination_uri = f"gs://{input_bucket}/{gcs_output_uri_prefix}/"

    operation = _batch_process_documents(
        project_id, location, processor_id, gcs_input_uri, destination_uri)

    print("Document Processing Operation: " + operation.operation.name)

    # Wait for the operation to finish
    operation.result(timeout=timeout)

    # The output files will be in a new subdirectory with the Operation ID as the name
    operation_id = re.search(
        r"operations\/(\d+)", operation.operation.name, re.IGNORECASE).group(1)

    output_directory = f"{gcs_output_uri_prefix}/{operation_id}"
    print(f"New Output Path: gs://{input_bucket}/{output_directory}")

    entities = get_document_json_from_gcs(input_bucket, output_directory)
    print("Entities:", entities)

    extract_filename = f"extractedjson/{input_filename}.json"
    print(create_json(entities, input_bucket, extract_filename))
    """
    output_document_protos = get_document_protos_from_gcs(input_bucket, output_directory)

    print("Completed: get_document_protos_from_gcs")
    print(output_document_protos)
    print("output_document_protos Printed")

    idx = 0
    for document_proto in output_document_protos:
        print("Document proto Item:")
        print(document_proto)
        entities = extract_document_entities(document_proto)
        print("Entities:", entities)

        extract_filename = f"extractedjson/{idx}/input_filename.json"
        print(create_json(entities, input_bucket, extract_filename))
        idx = idx + 1
    """

    """
        entities["input_file_name"] = input_filename

        print("Entities:", entities)
        print("Writing DocAI Entities to BQ")

        # Add Entities to DocAI Extracted Entities Table
        result = write_to_bq(dataset_name, entities_table_name, entities)
        print(result)

        # Send Address Data to PubSub
        for address_field in address_fields:
            if address_field in entities:
                process_address(
                    address_field, entities[address_field], input_filename)

    cleanup_gcs(input_bucket, input_filename, gcs_output_bucket,
                output_directory, gcs_archive_bucket_name)
    """
    return
