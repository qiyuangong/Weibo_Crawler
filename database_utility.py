import json
import sqlite3
# SQL for creat sina_weibo.db
"""
CREATE TABLE [USERS] (
[id] TEXT  PRIMARY KEY NULL,
[screen_name] TEXT  NULL,
[name] TEXT  NULL,
[domain] TEXT  NULL,
[gender] TEXT  NULL,
[city] INTEGER  NULL,
[province] INTEGER  NULL,
[location] TEXT  NULL,
[description] TEXT  NULL,
[url] TEXT  NULL,
[statuses_count] INTEGER  NULL,
[created_at] TEXT  NULL,
[all] TEXT  NULL
)

CREATE TABLE [PLACES] (
[pid] TEXT  PRIMARY KEY NULL,
[title] TEXT  NULL,
[address] TEXT  NULL,
[city] TEXT  NULL,
[province] TEXT  NULL,
[telephone] TEXT  NULL,
[checkin_num] INTEGER  NULL,
[checkin_user_num] INTEGER  NULL,
[all] TEXT  NULL
)

CREATE TABLE [U_QUEUE] (
[id] INTEGER  PRIMARY KEY AUTOINCREMENT NOT NULL,
[uid] INTEGER  NULL
)

CREATE TABLE [P_QUEUE] (
[id] INTEGER  PRIMARY KEY AUTOINCREMENT NOT NULL,
[pid] TEXT  NULL
)

CREATE TABLE [FOLLOWERS] (
[id] INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
[uid] INTEGER  NULL,
[fid] INTEGER  NULL
)

CREATE TABLE [MESSAGES] (
[id] INTEGER  NULL PRIMARY KEY,
[uid] INTEGER  NULL,
[created_at] TEXT  NULL,
[text] TEXT  NULL,
[source] TEXT  NULL,
[favorited] TEXT  NULL,
[truncated] TEXT  NULL,
[in_reply_to_status_id] TEXT  NULL,
[in_reply_to_user_id] TEXT  NULL,
[in_reply_to_screen_name] TEXT  NULL,
[geo] TEXT  NULL,
[mid] TEXT  NULL,
[reposts_count] INTEGER  NULL,
[conmments_count] INTEGER  NULL,
[annotations] INTEGER  NULL
)

CREATE TABLE [FRIENDS] (
[id] INTEGER  PRIMARY KEY AUTOINCREMENT NOT NULL,
[uid] INTEGER  NULL,
[fid] INTEGER  NULL
)

"""

class Database_Utility(object):
    #variables for ssqlite db 

    def __init__(self, dbname="sina_weibo.db"):
        """init Database_Utility with dabase path and name"""
        self.con = sqlite3.connect(dbname)
        self.cu = self.con.cursor()

    def close(self):
        """close database connection"""
        if self.con:
            self.con.close()
            self.con = None

    def check_user(self, uid):
        """check if uid in USERS, return true if exist"""
        self.cu.execute('select * from USERS where id=?', (uid,))
        if self.cu.fetchone():
            return False
        return True

    def check_place(self, pid):
        """check if uid in PLACES, return true if exist"""
        self.cu.execute('select * from PLACES where pid=?', (pid,))
        if self.cu.fetchone():
            return False
        return True

    def remove_P_queue(self, pid):
        """remove pid from P_QUEUE list"""
        self.cu.execute('delete from P_QUEUE where pid=?', (pid,))
        self.con.commit()

    def remove_U_queue(self, uid):
        """remove uid from U_QUEUE list"""
        self.cu.execute('delete from U_QUEUE where uid=?', (uid,))
        self.con.commit()

    def decode_place(self, st):
        """decode place json dict and inset into PLACES"""
        if not 'checkin_num' in st:
            st.checkin_num = 0
        poi = [(st.poiid, st.title, st.address, st.city, st.province, \
            st.phone, st.checkin_num, st.checkin_user_num, json.dumps(st).decode('unicode-escape'))]
        self.cu.executemany('insert into PLACES values(?,?,?,?,?,?,?,?,?)', poi)
        self.cu.executemany('insert into P_QUEUE values(NULL,?)', [(st.poiid,)])
        self.con.commit()

    def decode_user(self, st):
        """decode user json and insert into USERS"""
        user = [(st.id, st.screen_name, st.name, st.domain, st.gender, \
            st.city, st.province, st.location, st.description, st.url, st.statuses_count, st.created_at, json.dumps(st).decode('unicode-escape'))]
        self.cu.executemany('insert into USERS values(?,?,?,?,?,?,?,?,?,?,?,?,?)', user)
        self.cu.executemany('insert into U_QUEUE values(NULL,?)', [(st.id,)])
        self.con.commit()

    def makecheckin(self, pid, uid, ctime):
        """record checkin statuses of users"""
        checkin = [(pid, uid, ctime,)]
        self.cu.executemany('insert into CHECKIN values(NULL,?,?,?)', checkin)
        self.con.commit()
        return

    def fetch_from_U_queue(self, num=10):
        """fetch num users form U_QUEUE"""
        self.cu.execute('select * from U_QUEUE order by id limit %d' % num)
        users = self.cu.fetchall()
        return users

    def fetch_from_P_queue(self, num=5):
        """fetch num places form U_QUEUE"""
        self.cu.execute('select * from P_QUEUE order by id limit %d' % num)
        places = self.cu.fetchall()
        return places
