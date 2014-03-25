#!/usr/bin/env python
#coding=utf-8

import time
import sys
import pdb
import urllib
import urllib2
from datetime import datetime
from weibo import APIClient
from database_utility import *
from key import gl_USERID, gl_PASSWD, \
                gl_APP_KEY, gl_APP_SECRET, gl_ACCESS_TOKEN, gl_EXPIRE_IN, gl_CALLBACK_URL, \
				gl_key_num, gl_client

__DEBUG = False


#open database
db = Database_Utility("sina_weibo.db")

########################
#key and access_token
########################
def swit_app_key():
    global gl_client, gl_key_num
    gl_key_num += 1
    if gl_key_num >= len(gl_APP_KEY):
        gl_key_num = 0
    
    APP_KEY = gl_APP_KEY[gl_key_num]
    APP_SECRET = gl_APP_SECRET[gl_key_num]
    
    gl_client = APIClient(app_key=APP_KEY, app_secret=APP_SECRET, redirect_uri=gl_CALLBACK_URL)
    # get access_token and expires_in with APP_KEY, APP_SECRET, CALLBACK_URL
    resp = get_access_token(APP_KEY, APP_SECRET, gl_CALLBACK_URL)
    gl_client.set_access_token(resp.access_token, resp.expires_in)


def get_access_token(APP_KEY, APP_SECRET, CALLBACK_URL):
    "get access_token and expires_in with APP_KEY, APP_SECRET, CALLBACK_URL "
    AUTH_URL = 'https://api.weibo.com/oauth2/authorize'
    # client = APIClient(app_key=APP_KEY, app_secret=APP_SECRET, redirect_uri=CALLBACK_URL)
    referer_url = gl_client.get_authorize_url()
    if __DEBUG:
        print "referer url is : %s" % referer_url
    cookies = urllib2.HTTPCookieProcessor()
    opener = urllib2.build_opener(cookies)
    urllib2.install_opener(opener)

    postdata = {"client_id": APP_KEY, 
                "redirect_uri": CALLBACK_URL,
                "userId": gl_USERID,
                "passwd": gl_PASSWD,
                "isLoginSina": "0",
                "action": "submit",
                "response_type": "code",
                }

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; rv:11.0) Gecko/20100101 Firefox/11.0",
                "Host": "api.weibo.com",
                "Referer": referer_url
            }

    req  = urllib2.Request(
                            url = AUTH_URL,
                            data = urllib.urlencode(postdata),
                            headers = headers
                        )
    try:
        resp = urllib2.urlopen(req)
        if __DEBUG:
            print "callback url is : %s" % resp.geturl()
        code = "%s" % resp.geturl()[-32:]
    #code = "code=" + code
    except Exception, e:
        print e
    if __DEBUG:
        print "code is : %s"  % code
    resp = gl_client.request_access_token(code)
    return resp


########################
#Crawler fuctions
########################
def get_user_from_place(pid):
    pn = pMAX = 1
    total = 0
    while pn <= pMAX:
        try:
            user_checkin = gl_client.place.pois.users.get(poiid=pid, count=50, page=pn)
            # pdb.set_trace()
            for sc in user_checkin.users:
                if db.check_user(sc.id):
                    db.makecheckin(pid, sc.id, sc.checkin_at)
                    db.decode_user(sc)
            if pn == 1:
                total = user_checkin.total_number
                pMAX = (total - 1) / 50 + 1 
                print '%d' % total + " person checkin on poiid= " + pid
        except:
            print "There is an error for getting API attribute: page=%d, total=%d" %  (pn,total)
        pn = pn + 1
        time.sleep(2)
    db.remove_P_queue(pid)


def get_place_from_user(uid):
    pn = pMAX = 1
    total = 0
    while pn <= pMAX:
        place_checkin = gl_client.place.users.checkins.get(uid=uid, count=50, page=pn)
        for sc in place_checkin.pois:
            if db.check_place(sc.poiid):
                db.decode_place(sc)
        if pn == 1:
            total = place_checkin.total_number
            total = int(total)
            pMAX = (total - 1) / 50 + 1 
            print " user " + str(uid) + "checkin on " + '%d' % total + " places"
        # except:
        #   print "There is an error for getting API attribute: page=%d, total=%d" %  (pn,total)
        pn = pn + 1
        time.sleep(2)
    db.remove_U_queue(uid)


def begin_for_SEU():
    swit_app_key()
    print "Reading SEU POIID..."
    seu_location = gl_client.place.pois.search.get(city="0025", keyword="东南大学")
    for st in seu_location.pois:
        if db.check_place(st.poiid):
            db.decode_place(st)
    return


def double_queue_crawler():
    place_list = []
    user_list = []
    swit_app_key()
    
    users = db.fetch_from_U_queue(10)
    for sc in users:
        get_place_from_user(sc[1])

    places = db.fetch_from_P_queue(5)
    for sc in places:
        get_user_from_place(sc[1])

if __name__ == '__main__':
    print "Bein Weibo_Crawler!"
    # begin_for_SEU()
    double_queue_crawler()
    db.close()
