import pickle
import numpy as np
from flask import Flask, render_template,request 
import requests
from bs4 import BeautifulSoup
import pandas as pd
import pickle
import numpy as np
from flow import *

app= Flask(__name__)
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload():
    #print(request.files)
    
    d = {0:'Normal', 1:'BMF'  , 2:'BML', 3:'BMLO' , 4:'BMS' ,5: 'MMF',6:'MML',7:'MMLO',8:'MMS',9:'TMF',10:'TML',11:'TMLO',
     12:'TMRA',13:'TMRB',14:'TMS',15:'BNF',16:'BNL',17:'BNS',18:'MNF',19:'MNL',20:'MNLO',21:'MNS',22:'TNF',23:'TNL',24:'TNLO',
     25:'TNRA',26:'TNRB',27:'TNS'
    }
    if 'file' not in request.files:
        return 'No file part'
    file = request.files['file']
    if file.filename == '':
        return 'No selected file'
    # Handle the file, for example, save it to disk
    file.save(file.filename)
    Flow_Feature_Generator_(file.filename)
    pfile = "processed_"+ file.filename
    df = pd.read_csv(pfile)
    #print(df)

    #data = pd.read_csv("sample.csv")
    #print(data)
    first_row_array = df.iloc[0].values
    print(np.array([first_row_array]))
    

    pickled_model = pickle.load(open('model_opt.pkl', 'rb'))
    x = pickled_model.predict(np.array([first_row_array]))
    print(x)
    if(d[x[0]]=="Normal"):
        return render_template('index.html',bot= "No botnet detetcted")
    return render_template('index.html',bot= "Botnet detetcted, belonging to the class: "+d[x[0]])


    
    
if __name__ == '__main__':
    app.run(debug=True)

