import os
import json
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

class JsonCoder(object):
  """A JSON coder interpreting each line as a JSON string."""
  def encode(self, x):
    #print('Inside encode')
    #print(x)
    return json.dumps(x)

  def decode(self, x):
    #print('Inside decode')
    #print(x)
    return json.loads(x)

def run():    
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file',           
                      dest='input_file',
                      required=True,
                      help='Input file to process.')
    parser.add_argument('--bq_table',
                      dest='bq_table',
                      required=True,
                      help='Load to bq table.')

    path_args, pipeline_args = parser.parse_known_args()   

    file_location = path_args.input_file
    bq_target_tbl = path_args.bq_table

    #Set variables
    project_id = 'slalom-de'
    bq_dataset = 'slalom'
    #bq requires scratch disk space to push any errors or load files
    bq_scarch_disk = 'gs://slalom-de/temp'

    table_schema = {
        'fields': [
            {'name' : 'Longitude', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'Latitude', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'Business_State', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'Business_City', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'Business_Address', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'Business_Id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'Business_Name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'alias_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'day_of_the_week', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'close', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'open', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    }
    options = PipelineOptions(pipeline_args)
    p = beam.Pipeline(options=options)
    #with beam.Pipeline() as p1:
    composition = (
        p
        |'Read txt file' >> beam.io.ReadFromText(file_location,coder=JsonCoder())
        |'load to bq' >> beam.io.WriteToBigQuery(bq_target_tbl,dataset=bq_dataset, project=project_id, schema=table_schema, 
                        create_disposition='CREATE_IF_NEEDED', write_disposition='WRITE_APPEND', custom_gcs_temp_location=bq_scarch_disk)
    )

if __name__ == '__main__':
 run()