#%%
import json
import numpy as np
import pandas as pd
from ibm_watson import ToneAnalyzerV3
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch
from datetime import datetime
#%%
app = Flask(__name__)
PATH = './data/7282_1.csv'
df = pd.read_csv(PATH, delimiter=',')
df.drop(columns=['reviews.date'], inplace=True)
df.fillna('', inplace=True)
df = df[df.categories.str.contains('^Hotels$',case=True, regex=True)]#.dropna(subset=['reviews.text']
reviews = list(df['reviews.text'])
text = '\n'.join(reviews)

#%%
@app.route('/tones')
def get_tones():
    agg_avg = tone_analysis(text)
    return jsonify(agg_avg)

@app.route('/indexing')
def data_index():
    for i in range(df.shape[0]):
        es = Elasticsearch()
        endpoint = 'http://localhost:9200/hotels/hotel/{}'.format(i)
        data = df.iloc[i,:].to_dict()
        es.index(index='hotels',doc_type='hotel', id=i, body=data)
    return jsonify('Hotels data has been indexed successfully.!')

#%%
def tone_analysis(text):
    API_KEY = '' # Put your api here
    authenticator = IAMAuthenticator(API_KEY)
    tone_analyzer = ToneAnalyzerV3(
        version='2017-09-21',
        authenticator=authenticator
    )

    tone_analyzer.set_service_url('https://gateway-lon.watsonplatform.net/tone-analyzer/api')
    tone_analysis = tone_analyzer.tone(
        {'text': text} , # {'text': text}
        content_type='application/json'
    ).get_result()
    try:
        scores_list = []
        for i in range(len(tone_analysis['sentences_tone'])):
            if len(tone_analysis['sentences_tone'][i]['tones'])>0:
                tone_id = tone_analysis['sentences_tone'][i]['tones'][0]['tone_id']
                score = tone_analysis['sentences_tone'][i]['tones'][0]['score']
                scores_list.append([tone_id, score])
        df = pd.DataFrame(scores_list, columns=['tone_id', 'score'])
        agg_results = df.pivot_table(index = ['tone_id'], values = 'score', aggfunc = np.mean)['score'].to_dict()

        return agg_results
    except:
        agg_results = pd.DataFrame.from_dict(tone_analysis['document_tone']['tones'])[['tone_id', 'score']].to_dict()
        return agg_results

if __name__ == "__main__":
    app.run(debug=True)
