import os
import numpy as np
import math
import pandas as pd
import youtubeapi_pullinfo as yt
import pickle
import networkx as nx
from networkx.readwrite import json_graph
import json
import sys
sys.path.append('/home/ubuntu/anaconda/lib/python2.7/site-packages/')
import scipy
import pymysql as mdb
from sqlalchemy import create_engine
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_context('talk')
sns.set_style('darkgrid') 
plt.rcParams['figure.figsize'] = 12, 8  # plotsize
plt.rcParams['font.size'] = 22
from apiclient.errors import HttpError
import itertools

def collect_data(inputcID):
    engine = create_engine('mysql+pymysql://X:X@localhost/X?unix_socket=X')
    #print "created engine"
    con = mdb.connect(user= 'X', passwd='X', db='X',unix_socket='X',charset='utf8')
    #print "mdb connected"
    if not os.path.exists('./static/data/'+inputcID):
        os.makedirs('./static/data/'+inputcID)
    if not os.path.exists('./static/img/'+inputcID):
        os.makedirs('./static/img/'+inputcID)
    with con:
        cur = con.cursor()
        cur.execute("SELECT count(distinct cID) FROM ChannelInfo WHERE cID = %s", (inputcID))
        infofound = int(np.array(cur.fetchall()).flatten())
        cur.close()
    if infofound==0: #retrive channel stats and uploads playlist
        df_r=yt.channelinfo(inputcID)
        df_r.to_sql('ChannelInfo', engine, if_exists='append', index=False)
        uploadsid = str(df_r.UploadsID.values).strip('u[u\'\'])\',')
        print "Pulled Uploads playlist ID:",uploadsid
    else:
        print "Congrats! Input channel info is already in the database"
        with con: 
            cur = con.cursor()
            cur.execute("SELECT distinct UploadsID FROM ChannelInfo WHERE cID = %s", (inputcID))
            uploadsid = str(np.array(cur.fetchall()).flatten()).strip('u[u\'\'])\',')
            cur.close()
    vids=[]
    with con:
        cur = con.cursor()
        cur.execute("SELECT distinct vID FROM VideoInfo WHERE cID = %s", (inputcID))
        vids = np.array(cur.fetchall()).flatten()
        cur.close()
    if len(vids)<30: 
        print"retrieve  uploads info for playlist",uploadsid
        df_v=yt.pull_uploads(uploadsid)
        print "pulled uploads info"
        df_v.to_sql('VideoInfo', engine, if_exists='append', index=False)
        con = mdb.connect(user= 'X', passwd='X', db='X',unix_socket='X',charset='utf8')
        with con:
            cur = con.cursor()
            cur.execute("SELECT distinct vID FROM VideoInfo WHERE cID = %s", (inputcID))
            vids = np.array(cur.fetchall()).flatten()
            cur.close()
    else:
        print "Congrats! Video info for the input channel is already in the database"
    cpervid=[]
    with con:
        cur = con.cursor()
        cur.execute("SELECT distinct vID FROM CommentLinks WHERE cID = %s", (inputcID))
        vidsfound = np.array(cur.fetchall()).flatten()
        cur.close()
    if len(vidsfound)<30: #retrive comments for a given video
        vidstopull = set(vids) - set(vidsfound)
        for vID in list(vidstopull):
            print "Have to pull comments for vID",vID
            try:
                [nextp,rperpage,totr,ccount,df_c]=yt.pull_comments(vID,-1)
                df_c.to_sql('CommentLinks', engine, if_exists='append', index=False)
                reqtot = 1
                ccount_i=ccount
                while nextp!='None' or reqtot<4: #load no more than 3 pages to most recent comments
                    [nextp,rperpage,totr,ccount,df_c]=yt.pull_comments(vID,nextp)
                    ccount_i=ccount_i+ccount
                    reqtot = reqtot + 1     
                    df_c.to_sql('CommentLinks', engine, if_exists='append', index=False)
                cpervid.append(ccount_i)
            except:
                df_c = pd.DataFrame({ 'sID' : pd.Series('NotShared'),
                                     'cID' : pd.Series(inputcID),
                                     'vID' : pd.Series(vID)
                                    })
                df_c.to_sql('CommentLinks', engine, if_exists='append', index=False)
                pass
    else:
        print "Congrats! Comment info for recent 30 vids for the channel cID", inputcID, "is already in the database"
    counti=0
    df10 = pd.DataFrame(columns=('sID','vID'))
    subids=[]
    con = mdb.connect(user= 'X', passwd='X', db='X',unix_socket='X',charset='utf8')
    with con:
        cur = con.cursor()
        cur.execute("SELECT sID,vID FROM CommentLinks WHERE cID= %s AND sID<>'NotShared'",inputcID)
        subids = np.array(cur.fetchall())
        cur.close()
    for info_i in subids:
        df10.loc[counti] = info_i
        counti=counti+1
    df10=df10.drop_duplicates()
    df10['sIDfreq']=df10.groupby('sID').transform('count') #add a column that shows the number of time this commenter sID appeared in the comment threads
    sidnumt=[]
    freqt=0
    freqset=0
    for freqt_i in np.arange(10):
        df11=df10[df10['sIDfreq']>=freqt_i]
        subidsf=len(df11.sID.drop_duplicates())
        sidnumt.append(subidsf)
        #print "Pulling only sIDs with freq >",freqt_i," reduces # of records from",len(df10.sID.drop_duplicates()),"to",subidsf
        if freqset==0:
            if float(sidnumt[freqt_i])/sidnumt[0]<=0.03 or subidsf<=45:
                freqset=1
                freqt=freqt_i
                df12=df10[df10['sIDfreq']>=freqt]
            print "Pulling only sIDs with freq >",freqt_i," reduces # of records from",sidnumt[0],"to",sidnumt[freqt_i],"ratio is",float(sidnumt[freqt_i])/sidnumt[0]
    plt.figure()
    h1=df10.sIDfreq.hist(color='mediumaquamarine')
    h2=plt.axvline(df10.sIDfreq.mean(), 0, 1,color='navy',linewidth=10)
    h3=plt.axvline(freqt, 0, 1,color='coral',linewidth=10)
    plt.title('Viewer engagement (comments per user)',fontsize=26)
    plt.xlabel("Number of comments per user",fontsize=30)
    plt.ylabel('Frequency',fontsize=30)
    plt.legend((h2,h3),('average engagement = '+str(round(df10.sIDfreq.mean(),2)),'engagement threshold = '+str(freqt)),fontsize=22)
    plt.savefig('./static/img/'+inputcID+'/'+inputcID+'_commenter_ID_freq_hist.png', dpi=300, format='png')

    plt.figure()
    h1=plt.plot(sidnumt,'o-',linewidth=10.0,color='steelblue',markersize=20)
    h2=plt.axvline(freqt,0,1, color='coral',linewidth=15)
    plt.legend((h1,h2),('','engagement threshold'),fontsize=22)
    plt.xlabel('Number of comments per user',fontsize=30)
    plt.ylabel("Thresholded user count",fontsize=30)
    plt.savefig('./static/img/'+inputcID+'/'+inputcID+"_commenterIds_thresholding.png", dpi=300, format='png')
    
    subids=set(df12.sID.values)
    print "After dropping duplicates and leaving only sIDs of freqt =", freqt," reduced # of records from ", sidnumt[0], "to", len(subids)
    nopull=0;
    subnumber=[]
    res_pulled=0;
    count_i=0
    sidstopull=[]
    numdownloads=1
    res_pulled=0
    con = mdb.connect(user= 'X', passwd='X', db='X',unix_socket='X',charset='utf8')
    with con: #get all sIDs for which SubsfromComments have been already pulled from YT
        cur = con.cursor()
        cur.execute("SELECT distinct cID FROM CommenterSubs WHERE inputID=%s", (inputcID))
        sIDexist = np.array(cur.fetchall()).flatten()
        cur.close()
    if (len(subids) - len(sIDexist))>0: #pullagain==1:
        sidstopull= set(subids) - set(sIDexist)
        print "Have subscription info for", len(sIDexist)," gotta pull subscriptions for ", len(sidstopull),"more"
        for sID in sidstopull:
            print "Gotta pull subscriptions for sID", sID
            try:
                [sublist,subdates,nextp,rperpage,totr,resread]=yt.pull_subscriptions(sID,-1)
                subnumber.append(int(totr))
                res_pulled = resread+res_pulled
                sIDs = [sID for s in subdates]
                inputcIDs=[inputcID for s in subdates]
                reqtot = 1
                numdownloads=math.ceil(float(totr)/rperpage)
                dfi = pd.DataFrame({ 'inputID': pd.Series(inputcIDs),
                                    'DateAdded' : pd.Series(subdates),
                                    'cID' : pd.Series(sIDs),
                                    'tocID' : pd.Series(sublist),    
                                   })     
                dfi.to_sql('CommenterSubs', engine, if_exists='append', index=False)
                while reqtot < numdownloads:
                    [sublist,subdates,nextp,rperpage,totr,resread]=yt.pull_subscriptions(sID,nextp)
                    res_pulled = res_pulled + resread
                    reqtot = reqtot + 1
                    dfi = pd.DataFrame({ 'inputID': pd.Series(inputcIDs),
                                        'DateAdded' : pd.Series(subdates),
                                        'cID' : pd.Series(sIDs),
                                        'tocID' : pd.Series(sublist),    
                                       })     
                    dfi.to_sql('CommenterSubs', engine, if_exists='append', index=False)
            except:
                dfi = pd.DataFrame({'inputID': inputcID,
                                    'DateAdded' : "None",
                                    'cID' : sID,
                                    'tocID' : pd.Series("NotShared"),
                                   })
                dfi.to_sql('CommenterSubs', engine, if_exists='append', index=False)
                #print "Channel ", sID, 'does not share its subscriptions'
                nopull=nopull+1
                pass
        #plt.figure()
        #plt.hist(subnumber,color='sandybrown')
        #plt.title('Average number of subscriptions per user: %.d (std = %.d)' % (int(np.average(subnumber)), int(np.std(subnumber))),fontsize=30) #%08.2f (std = %.2f)
        #plt.xlabel('Number of channels',fontsize=22)
        #plt.ylabel('Frequency (normalized)',fontsize=22)
        #plt.savefig('./static/img/'+inputcID+'/'+inputcID+'_numsub_per_user.png', dpi=300, format='png')#,transparent=True)
    else:
        print "Congrats! Subscriptions for all Commenters are already in the database!"
    return subids
        
def run_comm(inputcID,subids):
    con = mdb.connect(user= 'X', passwd='X', db='X',unix_socket='X',charset='utf8')
    with con:
        cur = con.cursor()
        cur.execute("SELECT cID,tocID FROM CommenterSubs WHERE inputID=%s and tocID<>'NotShared'",(inputcID))# (tocID<>'None' AND cID<>'None')")# limit 1000")
        edges = cur.fetchall()
        cur.close()
    edges_pulled = [list(x) for x in set(tuple(x) for x in edges)]
    print "Number of edges pulled from the database", len(edges_pulled)
    uedges=[]
    for i,edge_i in enumerate(edges_pulled):
        if edge_i[0] in subids:
            uedges.append(edge_i)
    print "Number of relevant edges for the inputcID",len(uedges)
    G=nx.Graph()
    for edge in uedges:
        G.add_edge(edge[0],edge[1],weight=0.5)
        #print edge[0],edge[1]
    print "Number of edges", G.number_of_edges(),", number of nodes",G.number_of_nodes()
    
    import community as comm
    #dendo = comm.generate_dendogram(G) #takes a long time and unnecessary 
    part = comm.best_partition(G)
    modularity=comm.modularity(part, G) 
    print "Number of communities found",max(part.values())+1, ", modularity:",modularity
    count = 0.
    commf=0; #community to which input channel belongs
    nodesf=[] #nodes of the community to which input channel belongs
    nodepcom=[]
    label_prep=["" for x in range(len(part.values()))]
    for com in set(part.values()) :
        count = count + 1.
        list_nodes = [nodes for nodes in part.keys() if part[nodes] == com]
        nodepcom.append(len(list_nodes))
        label_prep[int(com)]=str(len(list_nodes))
        if inputcID in list_nodes:
            print "Input Channel is in the community #", com
            commf=com
            nodesf=list_nodes
            label_prep[int(com)]="Target community: "+str(len(list_nodes))
    labs=dict(zip(set(part.values()), label_prep))
    plt.figure()
    h1=plt.hist(nodepcom,bins=20,normed=False,color='steelblue')
    h2=plt.axvline(int(np.average(nodepcom)),0,1,color='navy',linewidth=10, label='average community size = '+str(int(np.average(nodepcom))))
    plt.legend(fontsize=22)
    plt.title('Network modularity Q = %.4f ' % (modularity),fontsize=30) 
    plt.xlabel('Number of channels per community', fontsize=26)
    plt.ylabel('Frequency', fontsize=26)
    plt.savefig('./static/img/'+inputcID+'/'+inputcID+'_com_size_distrib_1.png', dpi=300, format='png')#,transparent=True)
    com1=comm.induced_graph(part, G)
    # plt.figure()
    # pos = nx.spring_layout(com1)
    # nx.draw_networkx_edges(com1,pos,width=1.0, edge_color='g', style='solid', alpha=0.2)
    # nx.draw_networkx_labels(com1, pos, labels=labs, font_size=12, font_color='r', font_family='sans-serif', font_weight='normal', alpha=1.0)
    # plt.draw()
    # plt.savefig('./static/img/'+inputcID+'/'+inputcID + '_v3_com_plot_all_1.png', dpi=300, format='png',transparent=True)
    # #export to json for d3 graph plot
    i=0
    comfin=com1
    for node in com1.nodes():
        comfin.node[i]['group'] = i
        comfin.node[i]['label'] = label_prep[i]
        i=i+1
    comfin.nodes(data=True)
    nld=json_graph.node_link_data(comfin)
    json.dump(nld,open('./static/img/'+inputcID+'/'+inputcID+'_community_graph_comsub2.json','w'))
    return nodesf

def run_colfil(inputcID,nodesf,subids):
    inputID=inputcID
    engine = create_engine('mysql+pymysql://X:X@localhost/X?unix_socket=X')
    con = mdb.connect(user= 'X', passwd='X', db='X',unix_socket='X',charset='utf8')
    with con:
        cur = con.cursor()
        cur.execute("SELECT distinct cID FROM ChannelInfo")# limit 1000")
        response = cur.fetchall()
        cur.close()
    existIDs = set(np.array(response).flatten())
    print "Database contains info about",len(existIDs),"YT channels"
    #determine for which channels in the target community we are stil missing data
    pullcids=list(set(nodesf)-set(existIDs))
    print "Have to pull Channel info for",len(pullcids),"channels that are listed in the target community"
    counti=0
    for cID in pullcids:
        try:
            counti=counti+1
            df_r=yt.channelinfo(cID)
            #print 'Info pulled for channel ', df_r.cTitle.values,  ': pulled'
            df_r.to_sql('ChannelInfo', engine, if_exists='append', index=False)
        except HttpError:
            print "Can not pull channel",cID,"info, going to create a NotChannel record"
            df_r = pd.DataFrame({ 'cID' : pd.Series(cID),
                                 'DateAdded' : pd.Series('None'),
                                 'cTitle' : pd.Series('NotChannel'),
                                 'cDescr' : pd.Series('None'),
                                 'UploadsID' : pd.Series('None'),
                                 'SubCount': 0,
                                 'ViewCount': 0,
                                 'picUrl' : pd.Series('None'),
                                })
            df_r.to_sql('ChannelInfo', engine, if_exists='append', index=False)
            pass
    #print "pulled channel info for new matches, connect to database and load all channel info"
    con = mdb.connect(user= 'X', passwd='X', db='X',unix_socket='X',charset='utf8')
    sqlstr='SELECT cID,cTitle,VideoCount,SubCount,ViewCount,cDescr,UploadsID,picUrl FROM ChannelInfo WHERE cID IN (%s)' 
    in_p = ', '.join(itertools.repeat('%s', len(nodesf)))
    sqlstr = sqlstr % in_p
    with con:
        cur = con.cursor()
        cur.execute(sqlstr, nodesf)
        response = cur.fetchall()
        cur.close()
    print "pulled info about", np.array(response).shape, " channels in target community"
    dfi = pd.DataFrame(columns=('cID','cTitle','VideoCount','SubCount','ViewCount','cDescr','UploadsID','picUrl'))
    counti=0
    for channel in np.array(response):
        if channel[0] in nodesf:
            #print "iter ",counti,"add channel info to dataframe"
            dfi.loc[counti] = channel
            counti=counti+1
    print "added channel info to a dataframe for",counti,"channels"
    dfu=dfi.drop_duplicates()
    print "dropped duplicates"
    if inputcID in list(dfu.cID.values):
        print "inputID survived droping duplicates"
    keywords='beauty|haul|fashion|makeup|shopping|skin|hair|nail|polish|clothes|style|shopaholic'
    dff1=dfu[dfu['cDescr'].str.contains(keywords, case=False)]
    if inputcID in list(dff1.cID.values):
        print "inputID survived keywords filtering"
    #pickle.dump((dff1), open('./static/data/'+inputID+'/'+inputID+'_debug_1.p', 'wb')) #debug
    dff1[['SubCount', 'VideoCount','ViewCount']]=dff1[['SubCount', 'VideoCount','ViewCount']].astype(float)
    dff2=dff1[dff1['VideoCount']>10]
    if inputcID in list(dff2.cID.values):
        print "inputID survived video count filtering"
    dff=dff2[dff2['SubCount']>2000]
    if inputcID in list(dff.cID.values):
        print "inputID survived sub count filtering"
    print "Filters reduced # of matches from", len(dfu.index),"to",len(dff.index)
    plt.figure()
    dff['SubCount'].hist(bins=200, color='mediumpurple')
    plt.xlim([0,5*10**5])
    plt.xlabel('Number of subscribers per channel',fontsize=22)
    plt.ylabel('Frequency',fontsize=22)
    plt.title('Average number of subscribers %dK (std = %dK)' % (dff['SubCount'].mean()/1000, dff['SubCount'].mean()/1000),fontsize=30) 
    plt.savefig('./static/img/'+inputID+'/'+inputID+'_commf_subcount_hist_3.png', dpi=400, format='png')
    #print "create figure of ave num of vids"
    plt.figure()
    dff['VideoCount'].hist(bins=50,color='coral')
    plt.xlim([0,1500])
    plt.xlabel('Number of videos per channel',fontsize=22)
    plt.ylabel('Frequency',fontsize=22)
    plt.title('Average number of videos %d (std = %d)' % (dff['VideoCount'].mean(), dff['VideoCount'].mean()),fontsize=35) 
    plt.savefig('./static/img/'+inputID+'/'+inputID+'_commf_videocount_hist_1.png', dpi=400, format='png')
    viewpv=np.array(dff['ViewCount'].values/dff['VideoCount'].values)

    con = mdb.connect(user= 'X', passwd='X', db='X',unix_socket='X',charset='utf8')
    sublists=[]
    sublist_i=[]
    normsub=1
    print "pull subslists_ij"
    for i,cIDi in enumerate(dff['cID']):
        with con:
            cur = con.cursor()
            cur.execute("SELECT distinct cID FROM CommenterSubs WHERE tocID= %s", (cIDi));
            response = cur.fetchall()
            sublist_ii = [node[0] for node in np.array(response)]
            cur.close()
        sublist_i=list(set(sublist_ii) & set(subids))
        sublists.append(np.array(sublist_i))
        if cIDi==inputcID:
            normsub=len(sublist_i)
            indinputID=i
            print "! indinputID is",indinputID
    #print "create zero-ed similarity matrix"
    Sij=np.zeros((len(dff.index),len(dff.index)))
    Sijn=np.zeros((len(dff.index),len(dff.index)))
    for i,cIDi in enumerate(dff['cID']):
        sublist_i = set(sublists[i])
        for j,cIDj in enumerate(dff['cID']):
            if (j>i):
                sublist_j = set(sublists[j])
                counti=len(sublist_i & sublist_j)
                Sij[i][j]= float(counti)
                Sijn[i][j]= float(counti)/normsub
            elif i==j:
                Sij[i][j]=0
                Sijn[i][j]=0
            else:
                Sij[i][j]=Sij[j][i]
                Sijn[i][j]=Sijn[j][i]
    #print "create figure of the similarity matrix"
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    ax.set_aspect('equal')
    plt.imshow(Sijn, interpolation='nearest', cmap=plt.cm.rainbow)
    plt.colorbar()
    plt.savefig('./static/img/'+inputID+'/'+'case1_v2_Sijn_plot_2.png', dpi=300, format='png')#,transparent=True)
    corrmat=np.zeros((len(dff.index),len(dff.index)))
    corrmatw=np.zeros((len(dff.index),len(dff.index)))
    cc2=[]
    #print "about to make corr matrix"
    for i,cIDi in enumerate(dff['cID']):
        cc2.append(np.corrcoef(Sijn[:][indinputID],Sijn[:][i])[0,1])
        for j,cIDj in enumerate(dff['cID']):
            corrmat[i][j]=np.corrcoef(Sijn[:][i],Sijn[:][j])[0,1]
    totsimv=np.zeros(len(dff.index))
    totsim=[]
    for i,cIDi in enumerate(dff['cID']): #compute correlation coef for i channel with the input channel (weight for col. fil.)
            totsimv[i]=sum([corrmat[i][j]*cc2[i] for j,cIDj in enumerate(dff['cID'])])/sum(cc2)
            totsim.append([cIDi, dff['cTitle'].values[i], totsimv[i]])
            corrmatw[:][i]=corrmat[:][i]*cc2
    totsimvn=sorted(totsimv,reverse=True)
    totsimn=sorted(totsim, key=lambda totsim: totsim[2],reverse=True) 
    sort_index = np.argsort(totsimv)
    print "scored everything"
    #collab filter
    dff['score'] = pd.Series(totsimv, index=dff.index)
    dffin=dff.sort(columns='score', axis=0, ascending=False, inplace=False, kind='quicksort', na_position='last')
    IDpic=dffin[dffin['cID']==inputID]
    dffin = dffin[dffin.cID != inputID]
    final_results=dffin.values.tolist()
    input_info=IDpic.values.tolist()[0]
    print "about to dump results to pickle"
    pickle.dump((final_results,input_info), open('./static/data/'+inputID+'/'+inputID+'_collfilter_list_freqtauto2_comm.p', 'wb'))

    


    
    
