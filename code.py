api_key="AIzaSyCHKkTB1-rfP_wr1SxpFbYrN7UO2ls_aGs"
import googleapiclient.discovery
import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from pprint import pprint
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)
# scrapping channel details
def channeldetails(idofchannel):
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=idofchannel
        )
    response = request.execute()
    for i in response['items']:
        a = dict(channel_name =i['snippet']['title'],
                channel_id =i['id'],
                descrpition =i['snippet']['description'],
                subscribers= i['statistics']['subscriberCount'],
                totalvideos = i['statistics']['videoCount'],
                views = i['statistics']['viewCount'],
                playlist_id=i['contentDetails']['relatedPlaylists']['uploads'])
    return a
#scraping video id details
def detailsofvideo(idofchannel):
    vids=[]
    request = youtube.channels().list(
                part="contentDetails",
                id=idofchannel)
    response=request.execute()
    pid=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                    part="snippet",
                    playlistId=pid,
                    maxResults=50,
                    pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            vids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return vids


#getting video info using videoids
#getting video info
def videoinfo(videoids):
    videodata=[]
    for video_id in videoids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()
        for item in response['items']:
            data=dict(channelname=item['snippet']['channelTitle'],
                     channelid=item['snippet']['channelId'],
                     videoid=item['id'],
                     videotitle=item['snippet']['title'],
                     thumbnails=item['snippet']['thumbnails']['default']['url'],
                     discription=item['snippet'].get('description'),
                     dateofupload=item['snippet']['publishedAt'],
                     duration=item['contentDetails']['duration'],
                     views=item['statistics'].get('viewCount'),
                     comments=item['statistics'].get('commentCount'),
                     favouritecount=item['statistics']['favoriteCount'],
                     defenition=item['contentDetails']['definition'],
                     captions=item['contentDetails']['caption'],
                     likes=item['statistics'].get('likeCount')
                     )
            videodata.append(data)
    return videodata


#getting comment info
def commentinfo(video_ids):
    commentdata=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    maxResults=50
                )
            response=request.execute()

            for item in response['items']:
                 data=dict(commentid=item['snippet']['topLevelComment']['id'],
                           videoid=item['snippet']['topLevelComment']['snippet'][ 'videoId'],
                           commenttext=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                           commentersname=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                           commentposteddate=item['snippet']['topLevelComment']['snippet']['publishedAt'])
            commentdata.append(data)
    except:
        pass
    return commentdata


#getting playlist info

def playlistinfo(idofchannel):
    nextpagetoken=None
    playlistdata=[]
    while True:
        request=youtube.playlists().list(
                part='snippet,contentDetails',
                channelId=idofchannel,
                maxResults=50,
                pageToken=nextpagetoken
                )
        response=request.execute()

        for item in response['items']:
            data=dict(playlistid=item['id'],
                      titleofplaylist=item['snippet']['title'],
                      channelid=item['snippet']['channelId'],
                      channelname=item['snippet']['channelTitle'],
                      publisheddate=item['snippet']['publishedAt'],
                      videocount=item['contentDetails']['itemCount'])
            playlistdata.append(data)

        nextpagetoken=response.get('nextPageToken')
        if nextpagetoken is None:
            break
    return playlistdata


#connect with mongodb
client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["youtubedata"]

def channelinfo(idofchannel):
    chdetails=channeldetails(idofchannel)
    pldetails=playlistinfo(idofchannel)
    viiddetails=detailsofvideo(idofchannel)
    videtails=videoinfo(viiddetails)
    cmtdetails=commentinfo(viiddetails)
    
    coll1=db['channeldetails']
    coll1.insert_one({"channel_information": chdetails,"playlist_info":pldetails,
                    "video_details":videtails,"comment_details":cmtdetails})
    
    return "upload completed"


#table creation for channeldeatils in sql
def channels_table(channelnames):
    mydb=psycopg2.connect(host="127.0.0.1",
                        user="postgres",
                        password="hari",
                        database="youtube",
                        port="5432")
    cursor=mydb.cursor()


    try:
        create_query='''create table if not exists channels(channel_name varchar(200),
                                                            channel_id varchar(200) primary key,
                                                            descrpition text,
                                                            subscribers bigint,
                                                            totalvideos int,
                                                            views bigint,
                                                            playlist_id varchar(100))'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print('channels already table created')
        
    # getting data from mongodb and converting into dataframe
    single_channel_detail=[]
    db=client["youtubedata"]
    coll1=db['channeldetails']
    for ch_data in coll1.find({"channel_information.channel_name":channelnames},{"_id":0}):
        single_channel_detail.append(ch_data["channel_information"])
    df_singlechannelinfo=pd.DataFrame(single_channel_detail)  #convert into dataframe   

    for index,row in df_singlechannelinfo.iterrows():
            insert_query='''insert into channels(channel_name,
                                            channel_id,
                                            descrpition,
                                            subscribers,
                                            totalvideos,
                                            views,
                                            playlist_id)
                                            
                                            
                            values(%s,%s,%s,%s,%s,%s,%s)'''
                                            
            values=(row['channel_name'],
                    row['channel_id'],
                    row['descrpition'],
                    row['subscribers'],
                    row['totalvideos'],
                    row['views'],
                    row['playlist_id'])
            
                    
            try:
                    cursor.execute(insert_query,values)
                    mydb.commit()

            except Exception as e:
                    print("Error:", e)  # Printing the error message for debugging:
                    print("channeldetails are already inserted")   
        
        
#table creation for playlistsdetails in sql
def playlist_table(channelnames):
        mydb=psycopg2.connect(host="127.0.0.1",
                user="postgres",
                password="hari",
                database="youtube",
                port="5432")
        cursor=mydb.cursor()

        try:
                create_query='''create table if not exists playlists(playlistid varchar(200) primary key,
                                                        titleofplaylist varchar(200),
                                                        channelid varchar(100),
                                                        channelname varchar(100),
                                                        publisheddate timestamp,
                                                        videocount bigint
                                                        )'''
                cursor.execute(create_query)
                mydb.commit()

        except Exception as e:
                print("Error:", e)  # Printing the error message for debugging:
                print("playlistdetails are already inserted") 

        # getting data from mongodb and converting into dataframe
        single_playlist_detail=[]
        db=client["youtubedata"]
        coll1=db['channeldetails']
        for ch_data in coll1.find({"channel_information.channel_name":channelnames},{"_id":0}):
            single_playlist_detail.append(ch_data["playlist_info"])
        df_singleplaylistinfo=pd.DataFrame(single_playlist_detail[0])           

        for index,row in df_singleplaylistinfo.iterrows():
                insert_query='''insert into playlists(playlistid,
                                                titleofplaylist,
                                                channelid,
                                                channelname,
                                                publisheddate,
                                                videocount)
                                                
                                values(%s,%s,%s,%s,%s,%s)'''
                                                
                values=(row['playlistid'],
                        row['titleofplaylist'],
                        row['channelid'],
                        row['channelname'],
                        row['publisheddate'],
                        row['videocount'])
                
                        
                try:
                        cursor.execute(insert_query,values)
                        mydb.commit()
                
                except Exception as e:
                        print("Error:", e)  # Printing the error message for debugging:
                        print("playlistdetails are already inserted") 

# table creation for video details
def video_table(channelnames):
    mydb=psycopg2.connect(host="127.0.0.1",
                user="postgres",
                password="hari",
                database="youtube",
                port="5432")
    cursor=mydb.cursor()


    try:
        create_query='''create table if not exists videos(channelname varchar[100],
                        channelid varchar(100),
                        videoid varchar(100) primary key,
                        videotitle varchar(200),
                        thumbnails varchar(300),
                        discription text,
                        dateofupload timestamp,
                        duration interval, 
                        views int,
                        comments int,
                        favouritecount varchar(100),
                        defenition varchar(100),
                        captions varchar(100),
                        likes bigint
                        )'''
        cursor.execute(create_query)
        mydb.commit()

    except Exception as e:
                print("Error:", e)  # Printing the error message for debugging:
                print("video details are already inserted") 

    # getting data from mongodb and converting into dataframe
    single_video_detail=[]
    db=client["youtubedata"]
    coll1=db['channeldetails']
    for ch_data in coll1.find_one({"channel_information.channel_name":channelnames},{"_id":0})['video_details']:
                            single_video_detail.append(ch_data)
        
    videolist=[]
    for i in single_video_detail:
        videolist.append(tuple(i.values()))

    for row in videolist:
                            insert_query='''insert into videos(channelname,
                                                            channelid,
                                                            videoid,
                                                            videotitle,
                                                            thumbnails,
                                                            discription,
                                                            dateofupload,
                                                            duration, 
                                                            views,
                                                            comments,
                                                            favouritecount,
                                                            defenition,
                                                            captions,
                                                            likes)
                                                            
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                                                            
                            
                                    
                            try:
                                            cursor.execute(insert_query,row)
                                            mydb.commit()
                                    
                            except Exception as e:
                                            print("Error:", e)  # Printing the error message for debugging:
                                            print("videodetails are already inserted") 

#table creation for comment details
def comment_table(channelnames):
        mydb=psycopg2.connect(host="127.0.0.1",
                user="postgres",
                password="hari",
                database="youtube",
                port="5432")
        cursor=mydb.cursor()


        try:
                create_query='''create table if not exists comments(commentid varchar(200) primary key,
                                        videoid varchar(100),
                                        commenttext text,
                                        commentersname varchar(100),
                                        commentposteddate timestamp)'''
                cursor.execute(create_query)
                mydb.commit()

        except Exception as e:
                print("Error:", e)  # Printing the error message for debugging:
                print("video details are already inserted") 

        single_comment_detail=[]
        db=client["youtubedata"]
        coll1=db['channeldetails']
        for ch_data in coll1.find({"channel_information.channel_name":channelnames},{"_id":0}):
                single_comment_detail.append(ch_data["comment_details"])
        df_singlecommentinfo=pd.DataFrame(single_comment_detail[0])           

        for index,row in df_singlecommentinfo.iterrows():
                insert_query='''insert into comments(commentid,
                                videoid,
                                commenttext,
                                commentersname,
                                commentposteddate)
                                                
                                values(%s,%s,%s,%s,%s)'''
                                                
                values=(row['commentid'],
                        row['videoid'],
                        row['commenttext'],
                        row['commentersname'],
                        row['commentposteddate']
                        )
                
                        
                try:
                        cursor.execute(insert_query,values)
                        mydb.commit()
                
                except Exception as e:
                        print("Error:", e)  # Printing the error message for debugging:
                        print("comment details are already inserted") 

def tables(channelnames):
    channels_table(channelnames)
    playlist_table(channelnames)
    video_table(channelnames)
    comment_table(channelnames)
    
    return "Tables created"
    

def show_channeltable():
    ch_list=[]
    db=client["youtubedata"]
    coll1=db['channeldetails']
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)  #convert into dataframe   
    
    return df

def show_playlisttable():
        pl_list=[]
        db=client["youtubedata"]
        coll1=db['channeldetails']
        for pl_data in coll1.find({},{"_id":0,"playlist_info":2}):
                for i in range(len(pl_data["playlist_info"])): #has n no of playlists so use nested for
                        pl_list.append(pl_data["playlist_info"][i])
        df1=st.dataframe(pl_list)  #convert into dataframe 
        
        return df1 
    
def show_videostable():
        video_list=[]
        db=client["youtubedata"]
        coll1=db['channeldetails']
        for video_data in coll1.find({},{"_id":0,"video_details":3}):
                for i in range(len(video_data["video_details"])): #has n no of playlists so use nested for
                        video_list.append(video_data["video_details"][i])
        df2=st.dataframe(video_list)  #convert into dataframe 
        
        return df2 

def show_commenttable():
        cmt_list=[]
        db=client["youtubedata"]
        coll1=db['channeldetails']
        for cmt_data in coll1.find({},{"_id":0,"comment_details":4}):
                for i in range(len(cmt_data["comment_details"])): #has n no of playlists so use nested for
                        cmt_list.append(cmt_data["comment_details"][i])
        df3=st.dataframe(cmt_list)  #convert into dataframe 
        
        return df3  

# stream lit code

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Takeaway")
    st.caption("Python Scrpiting")
    st.caption("Data Collection")
    st.caption("MongoDb")
    st.caption("API intergration")
    st.caption("Data Management using MongoDb and SQL")

idofchannel=st.text_input("Enter the channel id")

if st.button("Collect and Store Data"):
    ch_ids=[]
    db=client["youtubedata"]
    coll1=db["channeldetails"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["channel_id"])
    
    
    if idofchannel in ch_ids:
        st.success("Channel Details of the given id are already existed")
        
    else:
        insert=channelinfo(idofchannel)
        st.success("Channel data inserted succesfully")
        
allchannel=[]
db=client["youtubedata"]
coll1=db['channeldetails']
for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
    allchannel.append(ch_data["channel_information"]["channel_name"]) 
        
unique_channel=st.selectbox("Choose the channel",allchannel)
    
if st.button("Migrate to Sql"):
        Table=tables(unique_channel)
        st.success(Table)
        
show_table=st.radio("Select the Table for viewing",("Channels","Playlists","Videos","Comments"))
    
if show_table=="Channels":
    show_channeltable()
    
elif show_table=="Playlists":
    show_playlisttable()
        
elif show_table=="Videos":
    show_videostable()
        
elif show_table=="Comments":
    show_commenttable()
    

#sql connection

mydb=psycopg2.connect(host="127.0.0.1",
            user="postgres",
            password="hari",
            database="youtube",
            port="5432")
cursor=mydb.cursor()

question=st.selectbox("select the question",("1. All the videos and channel name",
                                            "2. channels with most number of videos",
                                            "3. 10 most viewed videos",
                                            "4. comments in each videos",
                                            "5. videos with highest likes",
                                            "6. likes of all the videos",
                                            "7. views of each channel",
                                            "8. videos published in the year of 2022",
                                            "9. average duration of all videos in each channel",
                                            "10. videos with highest number of comments"))

if question=="1. All the videos and channel name":
    query1='''select videotitle as videos_title,channelname as channel_name from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["videos_title","channel_name"])
    st.write(df)

elif question=="2. channels with most number of videos":
    query2='''select channel_name as channelname,totalvideos as no_videos from channels 
                order by totalvideos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

elif question=="3. 10 most viewed videos":
    query3='''select views as views,channelname as channel_name,videotitle as video_title from videos 
            where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channelname","videotitle"])
    st.write(df3)
    
elif question=="4. comments in each videos":
    query4='''select comments as no_comments,videotitle as video_title from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no_comments","video_title"])
    st.write(df4)

elif question=="5. videos with highest likes":
    query5='''select videotitle as video_title,channelname as channel_name,likes as likecount
                from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=="6. likes of all the videos":
    query6='''select likes as likecount,videotitle as video_title from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","video_title"])
    st.write(df6)


elif question=="7. views of each channel":
    query7='''select channel_name as channelname ,views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channelname","totalviews"])
    st.write(df7)
    

elif question=="8. videos published in the year of 2022":
    query8='''select videotitle as video_title,dateofupload as videorelease,channelname as channel_name from videos
                where extract(year from dateofupload)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","videorelease","channel_name"])
    st.write(df8)

elif question=="9. average duration of all videos in each channel":
    query9='''select channelname as channel_name,AVG(duration) as averageduration from videos group by channelname'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channel_name","averageduration"])

    T9=[]
    for index,row in df9.iterrows():
        channel_titles=row["channel_name"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitles=channel_titles,avgduration=average_duration))
    df1=pd.DataFrame(T9)
    st.write(df1)


elif question=="10. videos with highest number of comments":
    query10='''select videotitle as video_title, channelname as channel_name,comments as comments from videos where comments is
            not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video_title","channel_name","comments"])
    st.write(df10)
