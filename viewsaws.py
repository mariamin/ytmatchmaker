from flask import Flask
app = Flask(__name__)

from flask import render_template, request
import pickle
import os
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import scoring_algo_v3 as alg3
import sys
sys.path.append('/home/ubuntu/anaconda/lib/python2.7/site-packages/')
import youtubeapi_pullinfo as yt

@app.route('/')
def home_fun():
    return render_template("intro_pulldown.html")

@app.route('/graph/<input_id>')
def graph(input_id):
    return render_template('graph_d3.html', input_id=input_id)

@app.route('/output')
def case1_output():
    #pull 'ID' from input field and store it
    inputUrl = request.args.get('inputID')
    #print "inputUrl is", inputUrl
    list1 = inputUrl.split("/")
    #print "list1", list1[:]
    if list1[len(list1)-2]=='channel':
        inputcID=str(list1[-1])
    else:
        inputname=str(list1[-1])
        print "inputname",inputname
        o1=yt.inopts(inputname,'channel','relevance',1) #below is debug
        #print "created inopts"
        [s,vids, vtitles,vdescs,vdates,vchanids,vchants,nextp,rperpage,totr]=yt.youtube_search(o1,-1)
        #print "pulled channel info"
        inputcID=str(vchanids[0])
    print "inputcID from channel url", inputcID
    if not os.path.exists('./static/data/'+ inputcID + '/' + inputcID + '_collfilter_list_freqtauto2_comm.p'):
        print "collect the data"
        subids = alg3.collect_data(inputcID)
        print "run comm detection"
        nodesf=alg3.run_comm(inputcID,subids)
        print "run collective filtering"
        alg3.run_colfil(inputcID,nodesf,subids)
    [sorted1,input_info] = pickle.load(open('./static/data/'+ inputcID + '/' + inputcID + '_collfilter_list_freqtauto2_comm.p', 'rb'))
    input_info[2]=int(input_info[2])
    if input_info[3]>10**6:
        input_info[3]=str(int(input_info[3]/10**6))+'M'
    elif input_info[3]>10**3:
        input_info[3]=str(int(input_info[3]/10**3))+'K'
    else:
        input_info[3]=str(int(input_info[3]))
    if input_info[4]>10**6:
        input_info[4]=str(int(input_info[4]/10**6))+'M'
    elif input_info[4]>10**3:
        input_info[4]=str(int(input_info[4]/10**3))+'K'
    else:
        input_info[4]=str(int(input_info[4]))
    #print input_info[7]
    input_info[7]=str(input_info[7]).strip('{u\'url\': u\'\'}') #data for some channels was not cleaned
    #print "after strip = ", input_info[7]
    matches = []
    for result in sorted1[0:20]:
        urli='http://www.youtube.com/channel/' + result[0]
        matches.append(dict(cid=urli, title=result[1], uploads=result[6], score=result[8]))
    #the_result = ''
    return render_template("output_d3.html", matches = matches, input_info=input_info,  inputUrl= inputUrl)
    #return render_template("freelance.html")

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=80,debug=True)
