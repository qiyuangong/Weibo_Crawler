import json
import sqlite3

#global variables for ssqlite db 
gl_con = sqlite3.connect("sina_weibo.db")
gl_cu = gl_con.cursor()


def check_user(uid):
    "check if uid in USERS, return true if exist"
    gl_cu.execute('select * from USERS where id=?', (uid,))
    if gl_cu.fetchone():
        return False
    return True


def check_place(pid):
    "check if uid in PLACES, return true if exist"
    gl_cu.execute('select * from PLACES where pid=?', (pid,))
    if gl_cu.fetchone():
        return False
    return True


def remove_P_queue(pid):
    "remove pid from P_QUEUE list"
    gl_cu.execute('delete from P_QUEUE where pid=?', (pid,))
    gl_con.commit()


def remove_U_queue(uid):
    "remove uid from U_QUEUE list"
    gl_cu.execute('delete from U_QUEUE where id=?', (uid,))
    gl_con.commit()


def decode_place(st):
    "decode place json dict and inset into PLACES"
    if not 'checkin_num' in st:
        st.checkin_num = 0
    poi = [(st.poiid, st.title, st.address, st.city, st.province, \
        st.phone, st.checkin_num, st.checkin_user_num, json.dumps(st).decode('unicode-escape'))]
    gl_cu.executemany('insert into PLACES values(?,?,?,?,?,?,?,?,?)', poi)
    gl_cu.executemany('insert into P_QUEUE values(NULL,?)', [(st.poiid,)])
    gl_con.commit()


def decode_user(st):
    "decode user json and insert into USERS"
    user = [(st.id, st.screen_name, st.name, st.domain, st.gender, \
        st.city, st.province, st.location, st.description, st.url, st.statuses_count, st.created_at, json.dumps(st).decode('unicode-escape'))]
    gl_cu.executemany('insert into USERS values(?,?,?,?,?,?,?,?,?,?,?,?,?)', user)
    gl_cu.executemany('insert into U_QUEUE values(NULL,?)', [(st.id,)])
    gl_con.commit()


def makecheckin(pid, uid, ctime):
    "record checkin statuses of users"
    checkin = [(pid, uid, ctime,)]
    gl_cu.executemany('insert into CHECKIN values(NULL,?,?,?)', checkin)
    gl_con.commit()
    return


def fetch_from_U_queue(num=10):
    """fetch num users form U_QUEUE"""
    gl_cu.execute('select * from U_QUEUE order by id limit %d' % num)
    users = gl_cu.fetchall()
    return users


def fetch_from_P_queue(num=5):
    """fetch num places form U_QUEUE"""
    gl_cu.execute('select * from P_QUEUE order by id limit %d' % num)
    places = gl_cu.fetchall()
    return places