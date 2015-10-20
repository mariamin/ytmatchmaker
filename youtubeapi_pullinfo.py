import sys
sys.path.append('/home/ubuntu/anaconda/lib/python2.7/site-packages/')
from apiclient.discovery import build
from apiclient.errors import HttpError
import numpy as np
import math
import pandas as pd
import scipy
import unicodedata


DEVELOPER_KEY = "X"  # browser key
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

class inopts: 
    def __init__(self, q, rtype,order,max_results):
        self.q = q
        self.max_results = max_results
        self.rtype=rtype
        self.order=order

def youtube_search(options,pullp):
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
    developerKey=DEVELOPER_KEY)
    # Call the search.list method to retrieve results matching the specified query term.
    if pullp==-1:
        search_response = youtube.search().list(
            q=options.q,
            order=options.order,
            type = options.rtype, 
            relevanceLanguage="en",
            part="id,snippet",
            regionCode="US",
            maxResults=options.max_results    
        ).execute()
    else:
        search_response = youtube.search().list(
            q=options.q,
            order=options.order,
            type = options.rtype, 
            relevanceLanguage="en",
            part="id,snippet",
            regionCode="US",
            maxResults=options.max_results, 
            pageToken=pullp
        ).execute()
    vids = []
    vtitles=[]
    vdescs=[]
    vdates=[]
    vchanids=[]
    vchants=[]
    vurls=[]  
    # Add each result to the appropriate list, and then display the lists of matching videos, channels, and playlists.
    nextp=search_response.get("nextPageToken")
    pageinfo=search_response.get("pageInfo",[])#.resultsPerPage", [])
    rperpage=pageinfo["resultsPerPage"]
    totr=pageinfo["totalResults"]
    for search_result in search_response.get("items", []):
        if search_result["id"]["kind"] == "youtube#video":
            vtitles.append("%s" % (search_result["snippet"]["title"]))
            vchanids.append("%s" % (search_result["snippet"]["channelId"]))
            vdates.append("%s" % (search_result["snippet"]["publishedAt"]))
            vchants.append("%s" % (search_result["snippet"]["channelTitle"]))
            vids.append("%s" % (search_result["id"]["videoId"]))
            vdescs.append("%s" % (search_result["snippet"]["description"]))
            print "Returned ", rperpage, "video results and", np.around(totr/rperpage),"pages"
        if search_result["id"]["kind"] == "youtube#channel":
            vtitles.append("%s" % (search_result["snippet"]["title"]))
            vchanids.append("%s" % (search_result["snippet"]["channelId"]))
            vdates.append("%s" % (search_result["snippet"]["publishedAt"]))
            vchants.append("%s" % (search_result["snippet"]["channelTitle"]))
            vdescs.append("%s" % (search_result["snippet"]["description"]))
            print "Channel ID for the title", options.q, "is", vchanids[0] 
    #print "Next page token", nextp
    return search_result,vids, vtitles,vdescs,vdates,vchanids,vchants,nextp,rperpage,totr

def channelinfo(sID):
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
    developerKey=DEVELOPER_KEY)  
    response = youtube.channels().list(
        id=sID,
        part="snippet,contentDetails,statistics",
    ).execute()
    
    if len(response["items"])>0:
        res_i=response["items"][0]
        title=res_i["snippet"]["title"]
        title=unicodedata.normalize('NFKD', unicode(res_i["snippet"]["title"])).encode('ascii','ignore')
        title=title.decode('unicode_escape',errors='ignore').encode('ascii',errors='ignore')
        desc=res_i["snippet"]["description"]
        desc=unicodedata.normalize('NFKD', unicode(res_i["snippet"]["description"])).encode('ascii','ignore')
        desc=desc.decode('unicode_escape',errors='ignore').encode('ascii',errors='ignore')
        picurl=str(res_i["snippet"]["thumbnails"]["high"]).strip("[ \"{u\'url\': u\'\'}\"]")
        if "publishedAt" in res_i["snippet"]:
            dateadded = res_i["snippet"]["publishedAt"]
        else:
            dateadded = "NotShared"
        df_r = pd.DataFrame({ 'cID' : pd.Series(sID),
                             'DateAdded' : pd.Series(dateadded),
                             'cTitle' : pd.Series(title),
                             'cDescr' : pd.Series(desc),
                             #'LikesID' : pd.Series(res_i["contentDetails"]["relatedPlaylists"]["likes"]), #too much data to pull live
                             'UploadsID' : pd.Series(res_i["contentDetails"]["relatedPlaylists"]["uploads"]), 
                             #'WatchHistoryID': pd.Series(res_i["contentDetails"]["relatedPlaylists"]["watchHistory"]), #too much data to pull live
                             'SubCount': int(res_i["statistics"]["subscriberCount"]),
                             'ViewCount': int(res_i["statistics"]["viewCount"]),
                             'PicURL' : picurl,
                             'VideoCount' : int(res_i["statistics"]["videoCount"]),
                          }) 
        print 'Info pulled for channel ', df_r.cTitle.values,  ':',df_r.SubCount.values
    else:
        df_r = pd.DataFrame({ 'cID' : pd.Series(sID),
                             'DateAdded' : pd.Series(''),
                             'cTitle' : pd.Series('NotChannel'),
                             'cDescr' : pd.Series('NotChannel'),
                             #'LikesID' : pd.Series(res_i["contentDetails"]["relatedPlaylists"]["likes"]),
                             'UploadsID' : pd.Series(''),
                             #'WatchHistoryID': pd.Series(res_i["contentDetails"]["relatedPlaylists"]["watchHistory"]),
                             'SubCount': 0,
                             'ViewCount': 0,
                             'PicURL' : '',
                             'VideoCount' : 0,
                          })
    return df_r

def pull_uploads(pID):
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
    developerKey=DEVELOPER_KEY)
    maxResults=30
    response = youtube.playlistItems().list(
        playlistId=pID,
        part="id,snippet",
        maxResults=30    
    ).execute()
    print "got response from api"
    vids=[]
    dad=[]
    vt=[]
    vdes=[]
    cid=[]
    for res_i in response.get("items", []):
        vids.append(res_i["snippet"]["resourceId"]["videoId"])
        dad.append(res_i["snippet"]["publishedAt"])
        title=unicodedata.normalize('NFKD', unicode(res_i["snippet"]["title"])).encode('ascii','ignore')
        title=title.decode('unicode_escape',errors='ignore').encode('ascii',errors='ignore')
        desc=unicodedata.normalize('NFKD', unicode(res_i["snippet"]["description"])).encode('ascii','ignore')
        desc=desc.decode('unicode_escape',errors='ignore').encode('ascii',errors='ignore')
        vt.append(title)
        vdes.append(desc)
        cid.append(res_i["snippet"]["channelId"])
    df_r = pd.DataFrame({ 'vID' : pd.Series(vids),
                 'DateAdded' : pd.Series(dad),
                 'vTitle' : pd.Series(vt),
                 'vDescr' : pd.Series(vdes),
                 'cID' : pd.Series(cid),
              })
    print "Loaded", maxResults,"videos info for channelID  ",cid[0]
    return df_r

def pull_comments(vID,pullp):
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
    developerKey=DEVELOPER_KEY)
    maxResults=100
    if pullp==-1:
        response = youtube.commentThreads().list(
            videoId=vID,
            part="replies,snippet",
            maxResults=maxResults, 
            order="time"
        ).execute()
    else:
        response = youtube.commentThreads().list(
            videoId=vID,
            part="replies,snippet",
            maxResults=maxResults, 
            order="time",
            pageToken=pullp
        ).execute()
    nextp=str(response.get("nextPageToken"))
    print "Nextp",nextp
    pageinfo=response.get("pageInfo",[])
    rperpage=pageinfo["resultsPerPage"]
    totr=pageinfo["totalResults"]
    sids=[]
    cids=[]
    vids=[]
    for res_i in response.get("items", []):
        sids.append(str(res_i["snippet"]["topLevelComment"]["snippet"]["authorChannelId"]["value"]))
        cids.append(str(res_i["snippet"]["channelId"]))
        vids.append(str(res_i["snippet"]["videoId"]))
        if "replies" in res_i:
            replies=res_i["replies"].get("comments",[])
            for res_j in replies:
                sids.append(str(res_j["snippet"]["authorChannelId"]["value"]))
                cids.append(str(res_i["snippet"]["channelId"]))
                vids.append(str(res_i["snippet"]["videoId"]))
    df_r = pd.DataFrame({ 'sID' : pd.Series(sids),
                         'cID' : pd.Series(cids),
                         'vID' : pd.Series(vids)
                        })
    print "Page",pullp,": Loaded", len(sids),"comments info out of",totr,"threads total"
    return nextp,rperpage,totr,len(sids),df_r

def pull_subscriptions(sID,pullp):
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
    developerKey=DEVELOPER_KEY)
    if pullp==-1:   
        response = youtube.subscriptions().list(
            channelId=sID,
            part="id,snippet",
            maxResults=50 #max possible
        ).execute()
    else:
        response = youtube.subscriptions().list(
            channelId=sID,
            part="id,snippet",
            maxResults=50, #max possible
            pageToken=pullp
        ).execute()
    sublist=[]
    subdates=[]
    pageinfo=response.get("pageInfo",[])
    nextp=str(response.get("nextPageToken"))
    rperpage=pageinfo["resultsPerPage"]
    totr=pageinfo["totalResults"]
    for res_i in response.get("items", []):
            sublist.append(str(res_i["snippet"]["resourceId"]["channelId"]))
            subdates.append((res_i["snippet"]["publishedAt"]))
    print 'Page ',pullp,  ': pulled',len(subdates), ' subscriptions out of ', totr,' total'
    resread = len(subdates)
    return sublist,subdates,nextp,rperpage,totr,resread
