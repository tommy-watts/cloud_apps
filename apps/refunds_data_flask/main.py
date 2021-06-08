from flask import Flask, request, render_template, redirect, url_for, flash
import pandas as pd
import os
import base64
import io
import logging
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS']="/access_token.json"

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


def upload_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )


@app.route('/')
@app.route('/upload')
def index():
    return render_template('index.html')

@app.route("/preview", methods=['GET', 'POST'])
def preview():
    if request.method == 'POST':
        
        uploaded_file = request.files.get('file')
        if not uploaded_file:
            return 'No file uploaded.', 400

        filename = uploaded_file.filename
        if filename != '':
            try:
                if 'csv' in filename:
                    df = pd.read_csv(uploaded_file)
                elif 'xls' in filename:
                    df = parse_refunds_data(uploaded_file)

                if request.form.get('submit_button'):

                    upload_gcs('madecom-dev-tommy-watts-sandbox', uploaded_file, os.path.basename(uploaded_file))

                    render_template('sent.html')

            except Exception as e:
                print(e)

        return render_template('preview.html', table=df.head().to_html(classes='data'))
    else:
        return render_template('index.html')

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
