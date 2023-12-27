from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#API key connection
def Api_connection():
    Api_Id="AIzaSyAvmwvRPoBWFQ7N9ty7N21YfFvarvuNxdM"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=Api_Id)
    return youtube
youtube=Api_connection()


def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id
        )
    response=request.execute()

    for i in response['items']:
            
        data=dict(channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i["statistics"]["viewCount"],
                Views=i["statistics"]["viewCount"],
                Total_videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]['description'],
                Playlist_ID=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data
                              
              

def get_Videos_Ids(channel_id):
    video_ids=[]
    
    response=youtube.channels().list(id=channel_id,
                                  part='contentDetails').execute()
    
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    next_page_token=None

    while True:

        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'] )
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids
    

# GET VIDEO DETAILS METHOD
def get_detailvideo_info(video_ids):

    video_data=[]
    for videid in video_ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=videid
        )
        response=request.execute()

        for j in response["items"]:
            data=dict(Channel_Name=j['snippet']['channelTitle'],
                    Channel_Id=j['snippet']['channelId'],
                    video_ID=j['id'],
                    Title=j['snippet']['title'],
                    Tags=j['snippet'].get('tags'),
                    Tumbnail=j['snippet']['thumbnails']['default']['url'],
                    Description=j['snippet'].get('description'),
                    Published_Date=j['snippet']['publishedAt'],
                    Duration=j['contentDetails']['duration'],
                    Viewcount=j['statistics'].get('viewCount'),
                    Likes=j['statistics'].get('likeCount'),
                    Comments=j['statistics'].get('commentCount'),
                    FavoriteCount=j['statistics']['favoriteCount'],
                    Definition=j['contentDetails']['definition'],
                    CaptionStatus=j['contentDetails']['caption']
                    )
            video_data.append(data) 
    return video_data        



#GET COMMAND INFORMATION
def get_info_cmd(cmdvideo):
    cmd_list=[]
    try:
        for id_video in cmdvideo:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=id_video,
                maxResults=50
            )
            response=request.execute()

            for k in response['items']:
                cmd_data=dict(Command_Id=k['snippet']['topLevelComment']['id'],
                            Video_ID=k['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_text=k['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=k['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published=k['snippet']['topLevelComment']['snippet']['publishedAt'])
                cmd_list.append(cmd_data)
    except:
        pass  
    return cmd_list    


def playlist_get_details(chanId):
    next_page_Token=None

    All_playlist=[]
    while True:
            request=youtube.playlists().list(
                part='snippet,contentDetails',
                channelId=chanId,
                maxResults=50,
                pageToken=next_page_Token
                    
                )
            response=request.execute()

            for l in response['items']:
                data2=dict(playlists_Id=l['id'],
                        Title=l['snippet']['title'],
                        Channel_Id=l['snippet']['channelId'],
                        Channel_Name=l['snippet']['channelTitle'],
                        Published_At=l['snippet']['publishedAt'],
                        video_Count=l['contentDetails']['itemCount'])
                All_playlist.append(data2)
            next_page_Token=response.get('nextPageToken')
            if next_page_Token is None:
                    break
    return All_playlist     



client=pymongo.MongoClient("mongodb+srv://lakshmiram:srmukhi@cluster0.j5exy2n.mongodb.net/?retryWrites=true&w=majority")
db=client["doc_data"]

def details_channel(chaid):
    details_of_channel= get_channel_info(chaid)
    details_of_playlist=playlist_get_details(chaid)
    details_of_video_ids=get_Videos_Ids(chaid)
    details_of_videodetails=get_detailvideo_info(details_of_video_ids)
    details_of_comment=get_info_cmd(details_of_video_ids)
    

    coll1=db["details_channel"]
    coll1.insert_one({"channel_information":details_of_channel,
                       "vidieo_ID_details":details_of_videodetails,
                       "comment details":details_of_comment,
                       "playlist_information":details_of_playlist,})
    
    return "Upload database completely succesfully"



#Table creation for channels 

def channels_table():    
    my_db=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="srmukhi15",
                        database="youtube_details",
                        port="5432")
    cursor=my_db.cursor()

    drop_query='''drop table  if exists channels'''
    cursor.execute(drop_query)
    my_db.commit()

    try:
        
        create_query='''create table if not exists channels(channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_videos int,
                                                            Channel_Description text,
                                                            Playlist_ID  varchar(80))'''
        cursor.execute(create_query)    
        my_db.commit()  
    except:                                                 
        print("channel tables already created") 
        
    ch_list=[]
    db=client["doc_data"] #database
    coll1=db["details_channel"] #collection
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)    

    for index,row in df.iterrows():
        insert_query='''insert into channels(channel_Name ,
                                            Channel_Id,
                                            Subscribers,
                                            Views,
                                            Total_videos,
                                            Channel_Description,
                                            Playlist_ID)
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_videos'],
                row['Channel_Description'],
                row['Playlist_ID'])
        try:
            
            cursor.execute(insert_query,values)    
            my_db.commit()  
            
        except:                                                 
            print("channel tables already created")                                                              




    #playlist table:
def playlist_table():
    my_db=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="srmukhi15",
                            database="youtube_details",
                            port="5432")
    cursor=my_db.cursor()

    drop_query='''drop table  if exists playlists'''
    cursor.execute(drop_query)
    my_db.commit()

        
    create_query='''create table if not exists playlists(playlists_Id varchar(100) primary key,
                                                        Title varchar(100),
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        Published_At timestamp,
                                                        video_Count int
                                                        )'''
    cursor.execute(create_query)                          
    my_db.commit()  
    
    #playlist fetch the mongo db

    pl_list=[]
    db=client["doc_data"] #database
    coll1=db["details_channel"] #collection
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):                                                          #print(pl_data)    
            pl_list.append(pl_data["playlist_information"][i])
            #df=pd.DataFrame(pl_list)  
        # print(pl_data["playlist_information"][i]) 
    df1=pd.DataFrame(pl_list)      
    
    cursor=my_db.cursor()

    for index,row in df1.iterrows():
            insert_query='''insert into playlists(playlists_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                Published_At,
                                                video_Count
                                                )      
                                                values(%s,%s,%s,%s,%s,%s)'''
            values=(row['playlists_Id'],
                    row['Title'],
                    row['Channel_Id'],
                    row['Channel_Name'],
                    row['Published_At'],
                    row['video_Count'])

                
            cursor.execute(insert_query,values)    
            my_db.commit()                                                           
                                                
                                                
 #video table

def videos_table():
    my_db=psycopg2.connect(host="localhost",
                                user="postgres",
                                password="srmukhi15",
                                database="youtube_details",
                                port="5432")
    cursor=my_db.cursor()

    drop_query='''drop table  if exists videos'''
    cursor.execute(drop_query)
    my_db.commit()

        
    create_query='''create table if not exists videos(Channel_Name  varchar(100),
                                                    Channel_Id varchar(100),
                                                    video_ID varchar(30) primary key,
                                                    Title varchar(150),
                                                    Tags text,
                                                    Tumbnail varchar(200),
                                                    Description text,
                                                    Published_Date timestamp,
                                                    Duration interval,
                                                    Viewcount bigint,
                                                    Likes bigint ,
                                                    Comments int,
                                                    FavoriteCount int,
                                                    Definition varchar(10),
                                                    CaptionStatus varchar(50)
                                                        )'''
    cursor.execute(create_query)                          
    my_db.commit() 
     
    vi_list=[]
    db=client["doc_data"] #database
    coll1=db["details_channel"] #collection
    for vi_data in coll1.find({},{"_id":0,"vidieo_ID_details":1}):
        for i in range(len(vi_data["vidieo_ID_details"])):                                                          #print(pl_data)    
            vi_list.append(vi_data["vidieo_ID_details"][i])
            #df=pd.DataFrame(pl_list)  
        # print(pl_data["playlist_information"][i]) 
    df2=pd.DataFrame(vi_list)      

    for index,row in df2.iterrows():
            insert_query='''insert into videos(Channel_Name,
                                                Channel_ID,
                                                video_ID,
                                                Title,
                                                Tags,
                                                Tumbnail,
                                                Description,
                                                Published_Date,
                                                Duration,
                                                Viewcount,
                                                Likes,
                                                Comments,
                                                FavoriteCount,
                                                Definition,
                                                CaptionStatus
                                            )    
                                                    
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                
            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['video_ID'],
                    row['Title'],
                    row['Tags'],
                    row['Tumbnail'],
                    row['Description'],
                    row['Published_Date'],
                    row['Duration'],
                    row['Viewcount'],
                    row['Likes'],
                    row['Comments'],
                    row['FavoriteCount'],
                    row['Definition'],
                    row['CaptionStatus']
                    )
                    
            cursor.execute(insert_query,values)    
            my_db.commit()                                                           
      
                                               
            #Comment details table creation
def comment_table():
    my_db=psycopg2.connect(host="localhost",
                                    user="postgres",
                                    password="srmukhi15",
                                    database="youtube_details",
                                    port="5432")
    cursor=my_db.cursor()

    drop_query='''drop table  if exists comments'''
    cursor.execute(drop_query)
    my_db.commit()

        
    create_query='''create table if not exists comments(Command_Id varchar(100) primary key,
                                                        Video_ID varchar(100),
                                                        Comment_text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp
                                                        )'''
    cursor.execute(create_query)                          
    my_db.commit()  

    ct_list=[]
    db=client["doc_data"] #database
    coll1=db["details_channel"] #collection
    for ct_data in coll1.find({},{"_id":0,"comment details":1}):
        for i in range(len(ct_data["comment details"])):                                                          #print(pl_data)    
            ct_list.append(ct_data["comment details"][i])
            #df=pd.DataFrame(pl_list)  
        # print(pl_data["playlist_information"][i]) 
    df3=pd.DataFrame(ct_list)   

    for index,row in df3.iterrows():
                insert_query='''insert into comments(Command_Id,
                                                    Video_ID,
                                                    Comment_text,
                                                    Comment_Author,
                                                    Comment_Published
                                                )    
                                                        
                                                values(%s,%s,%s,%s,%s)'''
                    
                values=(row['Command_Id'],
                        row['Video_ID'],
                        row['Comment_text'],
                        row['Comment_Author'],
                        row['Comment_Published']
                        )
                        
                cursor.execute(insert_query,values)    
                my_db.commit()                                                           
        


def tables():
    channels_table()
    playlist_table()
    videos_table()
    comment_table()
    
    return "Tables created succesfully"
        
        
def view_chantable():
    ch_list=[]
    db=client["doc_data"] #database
    coll1=db["details_channel"] #collection
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)  
    
    return df  
                       
                       
def view_playtable():
    pl_list=[]
    db=client["doc_data"] #database
    coll1=db["details_channel"] #collection
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):                                                          #print(pl_data)    
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)   
    
    return df1   
                      
                       
                                          
def view_vidtable():
    vi_list=[]
    db=client["doc_data"] #database
    coll1=db["details_channel"] #collection
    for vi_data in coll1.find({},{"_id":0,"vidieo_ID_details":1}):
        for i in range(len(vi_data["vidieo_ID_details"])):                                                          #print(pl_data)    
            vi_list.append(vi_data["vidieo_ID_details"][i])
            
    df2=st.dataframe(vi_list)
    
    return df2      

def view_comtable():
    ct_list=[]
    db=client["doc_data"] #database
    coll1=db["details_channel"] #collection
    for ct_data in coll1.find({},{"_id":0,"comment details":1}):
        for i in range(len(ct_data["comment details"])):                                                          #print(pl_data)    
            ct_list.append(ct_data["comment details"][i])
    df3=st.dataframe(ct_list)   
    
    return df3


# streamlit process

with st.sidebar:
    st.title(":red[YOUTUBE HARVESTING AND DATA WAREHOUSING]")
    st.header("Skill Takeaway")
    st.caption("python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("DataManagement using MongoDB and SQL")

channel_id=st.text_input("Enter the channel ID")   

if st.button("collected store data"):
    ch_ids=[]
    db=client['doc_data']
    coll1=db['details_channel']
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        
    if channel_id  in ch_ids:
        st.success("Channel Details of the given channel is already existed")
        
    else:
        insert=details_channel(channel_id)
        st.success(insert)
    
        
if st.button("Migrate to SQL"):
    Table=tables()
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    view_chantable()
    
elif show_table=="PLAYLISTS":
    view_playtable()
    
elif show_table=="VIDEOS":
    view_vidtable()

elif show_table=="COMMENTS":
    view_comtable()          
        
                                                                                                                                                                   
my_db=psycopg2.connect(host="localhost",
                                    user="postgres",
                                    password="srmukhi15",
                                    database="youtube_details",
                                    port="5432")
cursor=my_db.cursor()

question=st.selectbox("Select your Question",("1. All the videos and the channel name",
                                            "2. Channels with most number of videos",
                                            "3. 10 Most viewed videos",
                                            "4. Comments in each videos",
                                            "5. Videos with highest likes",
                                            "6. Likes of all videos",
                                            "7. Views of each channel",
                                            "8. Videos published in the year of 2022",
                                            "9. Average Duration of all videos in each channel",
                                            "10.Videos with highest number of comment"))
                                                                                            

my_db=psycopg2.connect(host="localhost",
                                    user="postgres",
                                    password="srmukhi15",
                                    database="youtube_details",
                                    port="5432")
cursor=my_db.cursor()

if question=="1. All the videos and the channel name":

    query1='''select title as videos,channel_name as channelname from videos'''

    cursor.execute(query1)
    my_db.commit()

    t1=cursor.fetchall()

    df=pd.DataFrame(t1,columns=["video title","channel name"])

    st.write(df)


elif question=="2. Channels with most number of videos":

    query2='''select channel_name as channelname,total_videos from channels
                order by total_videos desc'''
        
    cursor.execute(query2)
    my_db.commit()

    t2=cursor.fetchall()

    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])

    st.write(df2)


elif question=="3. 10 Most viewed videos":
    
    query3='''select viewcount as views,channel_name as channelname,title as videotitle from videos 
                where viewcount is not null order by views desc limit 10'''
        
    cursor.execute(query3)
    my_db.commit()

    t3=cursor.fetchall()

    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    
    st.write(df3)
    
elif question=="4. Comments in each videos":
    
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
        
    cursor.execute(query4)
    my_db.commit()

    t4=cursor.fetchall()

    df4=pd.DataFrame(t4,columns=["No of comments","videotitle"])
    
    st.write(df4)

elif question=="5. Videos with highest likes":
    
    query5='''select title as videotitle,channel_name as channelname ,likes as likecount
                from videos where likes is not null order by likes desc'''
        
    cursor.execute(query5)
    my_db.commit()

    t5=cursor.fetchall()

    df5=pd.DataFrame(t5,columns=["videotitle","channel name","likecount"])
    
    st.write(df5)
    
elif question=="6. Likes of all videos":
    
    query6='''select likes as likecount ,title as videotitle from videos'''
        
    cursor.execute(query6)
    my_db.commit()

    t6=cursor.fetchall()

    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])

    st.write(df6)
    
elif question=="7. Views of each channel":
    
    query7='''select channel_name as channelname,views as totalviews from channels '''
        
    cursor.execute(query7)
    my_db.commit()

    t7=cursor.fetchall()

    df7=pd.DataFrame(t7,columns=["channel name","totalviews"])

    st.write(df7)
    
elif question=="8. Videos published in the year of 2022":
    
    query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022 '''
        
    cursor.execute(query8)
    my_db.commit()

    t8=cursor.fetchall()

    df8=pd.DataFrame(t8,columns=["video title","published date","channel name"])

    st.write(df8)
    
    
elif question=="9. Average Duration of all videos in each channel":
    
    query9='''select channel_name as channalname,AVG(duration) as averageduration from videos group by channel_name'''
        
    cursor.execute(query9)
    my_db.commit()

    t9=cursor.fetchall()

    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])

    df9

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    dft=pd.DataFrame(T9)
    st.write(dft)
    
    
elif question=="10.Videos with highest number of comment":
    
    query10='''select title as videotitle,channel_name as channelname,comments as comments from videos 
                where comments is not null order by comments desc '''
        
    cursor.execute(query10)
    my_db.commit()

    t10=cursor.fetchall()

    df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])

    st.write(df10)








    
    




    
                


