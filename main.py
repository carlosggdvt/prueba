# Copyright 2020 Google, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START cloudrun_helloworld_service]
# [START run_helloworld_service]
from google.cloud import bigquery
from google.cloud import pubsub_v1
import pandas as pd
import json
import os
import datetime
from flask import Flask, request,jsonify


app = Flask(__name__)


@app.route("/",methods=['POST'])
def http():
    pub_client = pubsub_v1.PublisherClient()
    bq_client = bigquery.Client()
    #PROJECT_ID = os.environ.get("GCP_PROJECT")
    #geocode_request_topicname = os.environ.get("GEOCODE_REQUEST_TOPICNAME")
    dataset_name = 'invoice_parser_results'
    scoring_table_name='score'
    # Every call to publish() returns an instance of Future
    geocode_futures = []

    # Setting variables
    address_fields = [
        "receiver_address",
        "remit_to_address",
        "ship_from_address",
        "ship_to_address",
        "supplier_address",
    ]
    def write_to_bq_score(dataset_name, table_name, score_dict):
        """
        Write Data to BigQuery
        """
        dataset_ref = bq_client.dataset(dataset_name)
        table_ref = dataset_ref.table(table_name)

        json_score=[score_dict]

        json_data = json.dumps(json_score, sort_keys=False,default=str)
        # Convert to a JSON Object
        json_object = json.loads(json_data)


        schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION]
        source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

        job_config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField('ID', 'STRING', 'NULLABLE'),bigquery.SchemaField('timestamp', 'TIMESTAMP', 'NULLABLE'),bigquery.SchemaField('redFlag', 'BOOLEAN', 'NULLABLE'),bigquery.SchemaField('score', 'INTEGER', 'NULLABLE')],
            schema_update_options=schema_update_options,
            source_format=source_format
        )

        job = bq_client.load_table_from_json(
            json_object, table_ref, job_config=job_config)
        return job.result()  # Waits for table load to complete.
    
  
    query = 'SELECT * FROM `carlos-gomez-sandbox-01.invoice_parser_results.view_query` WHERE ID ="{}"'.format(request.json["ID"])
    #print(query)
    query_job = bq_client.query(query)
    results = query_job.result()
    query_dict=dict()
    result=dict()
    score=0
    redFlag=False
    for row in results:
        query_dict["ID"]=row[0]
        query_dict["receiver_name"]=row[1]
        query_dict["receiver_address"]=row[2]
        query_dict["receiver_email"]=row[3]
        query_dict["receiver_phone"]=row[4]
        query_dict["receiver_tax_id"]=row[5]
        query_dict["supplier_name"]=row[6]
        query_dict["supplier_address"]=row[7]
        query_dict["supplier_email"]=row[8]
        query_dict["supplier_phone"]=row[9]
        query_dict["supplier_tax_id"]=row[10]
        query_dict["invoice_date"]=row[11]
        query_dict["invoice_id"]=row[12]
        query_dict["line_item"]=row[13]
        query_dict["line_item_description"]=row[14]
        query_dict["line_item_quantity"]=row[15]
        query_dict["line_item_product_code"]=row[16]
        query_dict["line_item_unit"]=row[17]
        query_dict["line_item_unit_price"]=row[18]
        query_dict["total_amount"]=row[19]
        query_dict["total_tax_amount"]=row[20]
        query_dict["ship_from_name"]=row[21]
        query_dict["ship_to_name"]=row[22]
        query_dict["ship_to_address"]=row[23]
        query_dict["supplier_iban"]=row[24]
        query_dict["remit_to_adress"]=row[26]
        #query_dict["plastiks_type"]=row[27]
    
    if query_dict["invoice_id"] is None \
    or query_dict["invoice_date"] is None \
    or query_dict["supplier_name"] is None \
    or query_dict["supplier_address"] is None \
    or query_dict["line_item_quantity"] is None:
        redFlag=True

    features=["invoice_id","invoice_date","supplier_name","supplier_address","line_item_quantity","supplier_tax_id","receiver_tax_id",
    "receiver_name","ship_to_address","supplier_iban"]
    scoring=[10,10,10,10,10,10,10,5,10,5]

    df=pd.DataFrame({'features':features,'scoring':scoring})

    for i in range(df.shape[0]):
        if query_dict[df.iloc[i,0]] is not None:
            score +=df.iloc[i,1]

    result_json={'ID':str(query_dict["ID"]),'timestamp':datetime.datetime.now(),'redFlag':redFlag,'score':score}
    result['score']=str(score)
    result['redFlag']=str(int(redFlag))
    write_to_bq_score(dataset_name, scoring_table_name,result_json)

    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
# [END run_helloworld_service]
# [END cloudrun_helloworld_service]
