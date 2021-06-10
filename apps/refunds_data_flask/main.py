from flask import Flask, request, render_template, redirect, url_for, flash
import pandas as pd
import os
import base64
import io
import logging
from google.cloud import storage

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


def upload_gcs(bucket_name, uploaded_file, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(
        uploaded_file.read(),
        content_type=uploaded_file.content_type
    )
    logging.warning("File {} uploaded to {}.".format(uploaded_file.filename, destination_blob_name))

def delete_blob(bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()
    logging.warning("Blob {} deleted.".format(blob_name))

def move_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_file_name)
    new_blob = bucket.rename_blob(blob, destination_blob_name)
    logging.warning("Blob {} has been renamed to {}".format(blob.name, new_blob.name))


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
            try:
                if 'csv' in filename:
                    df = pd.read_csv(uploaded_file)
                elif 'xls' in filename:
                    df = parse_refunds_data(uploaded_file)
            except Exception as e:
                logging.info(e)
            upload_gcs('madecom-dev-tommy-watts-sandbox', uploaded_file, f"tmp/{filename}")
            return render_template('preview.html', table=df.head().to_html(classes='data'), filename=filename)
        elif filename:
            logging.warning(f"@@{filename}")
            return redirect(url_for('sent', filename=filename))

@app.route('/sent/<filename>', methods=['GET', 'POST'])
def sent(filename):
    try:
        move_blob('madecom-dev-tommy-watts-sandbox', f"tmp/{filename}", filename)
    except Exception as e:
                print(e)
    return render_template('sent.html')

@app.route('/cancel/', methods=['POST'])
def cancel():
    delete_blob('madecom-dev-tommy-watts-sandbox', 'tmp')
    return render_template('index.html')

@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
