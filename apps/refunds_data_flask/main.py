from flask import Flask, request, render_template, redirect, url_for, flash
import pandas as pd
import os
import base64
import io
import logging
from google.cloud import storage
import time

os.environ['GOOGLE_APPLICATION_CREDENTIALS']="access_token.json"

app = Flask(__name__)

def parse_refunds_data(path): 
    df = pd.concat(pd.read_excel(path, sheet_name=None)).\
                        reset_index().drop(['level_1'], axis=1).\
                        rename(columns={"level_0": "provider"})
    df.amount = df.amount * 100
    df.date = pd.to_datetime(df.date, format='%Y%m%d')
    df.provider = df.provider.str.lower()
    if any(df.date.isna()):
        raise Exception("There are null dates in the data provided.")
    if not all([item in df.columns for item in ['order_id', 'provider', 'date', 'amount', 'country']]):
        raise Exception("Expected fields order_id, provider, date, amount, country.")
    return df


def upload_gcs(bucket_name, df, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(df.to_csv(index=index), 'text/csv')

    logging.warning(
        "File {} uploaded to {}.".format(
            'file', destination_blob_name
        )
    )
    
def move_gcs(bucket_name, source_file_name, destination_blob_name):
    """Renames a blob."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"
    # new_name = "new-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_file_name)

    new_blob = bucket.rename_blob(blob, destination_blob_name)

    logging.warning("Blob {} has been renamed to {}".format(blob.name, new_blob.name))


@app.route('/')
@app.route('/upload')
def index():
    return render_template('index.html')

@app.route("/preview", methods=['GET', 'POST'])
def preview():
    if request.method == 'POST':

        uploaded_file = request.files.get('file')
        gcs_filename = request.form.get('gcs_filename')
        timestr = time.strftime("%Y%m%d-%H%M%S")

        # if not uploaded_file:
        #     return 'No file uploaded.', 400
        if uploaded_file:
            filename = uploaded_file.filename
            try:
                if 'csv' in filename:
                    df = pd.read_csv(uploaded_file)
                elif 'xls' in filename:
                    df = parse_refunds_data(uploaded_file)
                gcs_filename =  'upload_' + timestr
                upload_gcs('madecom-dev-dan-kruse-sandbox', df, 'refund_uploads/draft/'+ gcs_filename)
            except Exception as e:
                print(e)
            return render_template('preview.html', table=df.head().to_html(classes='data'), gcs_filename=gcs_filename)
                
        elif gcs_filename:
            move_gcs('madecom-dev-dan-kruse-sandbox', 'refund_uploads/draft/'+ gcs_filename, 'refund_uploads/confirmed/'+ gcs_filename)
            return render_template('sent.html')


@app.route('/sent', methods=['GET', 'POST'])
def test():
    return render_template('sent.html')

@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
