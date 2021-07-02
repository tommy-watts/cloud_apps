from flask import Flask, request, render_template, redirect, url_for, jsonify
import pandas as pd
import os
from datetime import datetime
import logging
from google.cloud import storage, bigquery

logging.getLogger().setLevel(logging.INFO)

app = Flask(__name__)

FIELDS = {
        'country' : [
            'CHF',
            'FR',
            'DE',
            'NL',
            'ES',
            'UK'
    ],
        'provider' : [
            'paypal',
            'be2bill',
            'stripe',
            'klarna ',
            'bacs'
    ]
}

class InvalidUsage(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv

def get_last_month():
    now = datetime.now()
    return f"{now.year}{('0' + str(now.month-1)) if now.month else '12'}"

def check_last_month(dates):
    last_month = int(get_last_month()[4:])
    return (dates.dt.month == last_month).all()

def check_field(field):
    return [i for i in field if i not in FIELDS[field.name]]

def parse_refunds_data(path): 
    try:
        df = pd.concat(pd.read_excel(path, sheet_name=None)).\
                            reset_index().drop(['level_1'], axis=1).\
                            rename(columns={"level_0": "provider"})
        if not all([item in df.columns for item in ['order_id', 'provider', 'date', 'amount', 'country']]):
            raise InvalidUsage("Expected fields order_id, provider, date, amount, country are not present.")
        df.date = pd.to_datetime(df.date, format='%Y%m%d')
        if not check_last_month(df.date):
            raise InvalidUsage("Data needs to contain refunds only for last month.")
        if any(df.date.isna()):
            raise InvalidUsage("There are null dates in the data provided.")
        df.amount = df.amount * 100
        df.provider = df.provider.str.lower()
        for field in [df.provider, df.country]:
            unknown_values = check_field(field)
            if unknown_values:
                raise InvalidUsage(f"There are unknown {field.name} {set(unknown_values)} in the data provided.")
        return df
    except InvalidUsage as e:
        logging.error(e)
        raise e


def upload_gcs(bucket_name, uploaded_file, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(
        uploaded_file,
        content_type='text/csv'
    )
    logging.info("File uploaded to {}.".format(destination_blob_name))

def delete_tmp(bucket_name, path):
    storage_client = storage.Client()
    for blob in storage_client.list_blobs(bucket_name, prefix=path):
        blob.delete()
        logging.info("Blob {} deleted.".format(str(blob)))

def move_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_file_name)
    new_blob = bucket.rename_blob(blob, destination_blob_name)
    logging.info("Blob {} has been renamed to {}".format(blob.name, new_blob.name))

def blob_to_bq(bucket, filename, table_id):
    
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("provider", "STRING"),
            bigquery.SchemaField("order_id", "STRING"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("amount", "FLOAT"),
            bigquery.SchemaField("country", "STRING"),
        ],
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
    )
    
    uri = f"gs://{bucket}/{filename}"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  

    load_job.result()
    logging.info("File {} written to {}".format(uri, table_id))


@app.route('/')
@app.route('/upload')
def index():
    return render_template('index.html')

@app.route("/preview", methods=['POST'])
def preview():
    if request.method == 'POST':
        uploaded_file = request.files.get('file')
        filename = request.form.get('filename')
        if uploaded_file:
            filename = uploaded_file.filename
            if not '.xls' in filename:
                raise InvalidUsage('Uploaded file is not of type .xlsx or .xls', 400)
            df = parse_refunds_data(uploaded_file)
            upload_gcs(os.environ['BUCKET'], df.to_csv(index=False), f"tmp/refund_payments_{get_last_month()}.csv")
            return render_template('preview.html', table=df.head().to_html(classes='data'), filename=filename)
        elif filename:
            return redirect(url_for('sent', filename=filename))

@app.route('/sent', methods=['GET', 'POST'])
def sent():
    try:
        filename = f"refund_payments_{get_last_month()}.csv"
        move_blob(os.environ['BUCKET'], f'tmp/{filename}', filename)
        table_id = f"{os.environ['BIGQUERY_PROJECT']}.archive.{os.path.splitext(filename)[0]}"
        blob_to_bq(os.environ['BUCKET'], filename, table_id)
    except InvalidUsage as e:
        print(e)
    return render_template('sent.html')

@app.route('/cancel', methods=['GET', 'POST'])
def cancel():
    delete_tmp('madecom-dev-tommy-watts-sandbox', 'tmp/')
    return render_template('index.html')

@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
