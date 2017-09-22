#!/usr/bin/env python
from auth import app_id, user_login, user_password, my_id
import vk
import time
import os
import pandas as pd

class ApiConnection:
    
    def __init__(self,
            _app_id=app_id,
            _user_login=user_login,
            _user_password=user_password
        ):
        self._app_id = _app_id
        self._user_login = _user_login
        self._user_password = _user_password
        
    def get_api(self):       
        session = vk.AuthSession(
                self._app_id, 
                self._user_login, 
                self._user_password, 
                scope='messages'
        ) 
        return vk.API(session)

def messages_generator(dialog_id, step=100):
    def get_hist():
        return vk_api.messages.getHistory(
                peer_id=str(dialog_id),
                user_id=my_id,
                count=step,
                offset=offset,
                v='5.68'
        )
    offset = 0
    hist = get_hist()
    while len(hist['items']) > 0:
        for item in hist['items']:  
            yield item  
        offset += step
        hist = get_hist()

def dialogs_generator(step=100):
    def get_diags():
        return vk_api.messages.getDialogs(
                offset=offset,
                count=step,
                v='5.68'
        )
    offset = 0
    diags = get_diags()
    while len(diags['items']) > 0:
        for item in diags['items']:  
            try:
                yield 2000000000 + item['message']['chat_id']
            except KeyError as e:
                if 'chat_id' in e.args:
                    yield item['message']['user_id']
                else:
                    raise
            except:
                raise
        offset += step
        diags = get_diags()
def get_dialogs_number(step=200):
    offset = 0
    counter = 0
    curr_len = len(vk_api.messages.getDialogs(offset=offset, count=step, v='5.68')['items'])
    while  curr_len > 0:
        time.sleep(0.5)
        counter += curr_len
        curr_len = len(vk_api.messages.getDialogs(offset=offset, count=step, v='5.68')['items'])
        offset += step

    return counter
        

finished = False

while not finished:
    try:
        vk_api = ApiConnection().get_api()
        print ("API obtained")
        processed_chats = [int(f.split('_')[0]) for f in os.listdir('data')]
        print ("Processed things obtained")
        counter = 0
        diags_number = get_dialogs_number()
        print("Diags number obtained")
        for dialog_id in dialogs_generator(200):
            counter += 1
            if dialog_id in processed_chats:
                continue
            print("Dialog {} now".format(dialog_id))
            df = pd.DataFrame()
            for message in messages_generator(dialog_id=dialog_id, step=200):
                df = pd.concat([df, pd.DataFrame(
                    {k: (lambda x: str(x) if type(x) == type([]) else x)(v) for k, v in message.items()}
                    , index=[0])], ignore_index=True)
            df.to_csv(os.path.join("data", str(dialog_id) + "_chat.csv"))
            print("\rProceed: {} of {} ".format(counter, diags_number), end='')
            
        finished = True
    except Exception as e:
        print ("Error was: ", e)
    
print()

