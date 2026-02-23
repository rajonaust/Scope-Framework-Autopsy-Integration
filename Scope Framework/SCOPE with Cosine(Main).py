#!/usr/bin/env python
# coding: utf-8

import sys
import json
import os
import re
import pandas as pd
import copy
import spacy
import datetime
import csv
import time
from keybert import KeyBERT
import math
import timeit
import json
from gensim import corpora, models
from datetime import datetime, timedelta
from spellchecker import SpellChecker
from nltk.stem import WordNetLemmatizer
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

def run_SCOPE(input_json):

    bertModel = SentenceTransformer('all-MiniLM-L12-v2')
    collections = []
    collections_embedding = []
    def word_embedding(w):
        if w not in collections:
            collections.append(w)
            collections_embedding.append(bertModel.encode(w))
        return collections_embedding[collections.index(w)]
    
    
    global_words = []
    global_similarity = []
    def find_similarity(w1, w2):
        w1 = w1.lower()
        w2 = w2.lower()
        if [w1, w2] in global_words:
            idx = global_words.index([w1, w2])
        elif [w2, w1] in global_words:
            idx = global_words.index([w2, w1])
        else:
            idx = len(global_words)
            global_words.append([w1, w2])
            global_similarity.append(
                cosine_similarity([word_embedding(w1)],
                                    [word_embedding(w2)])[0][0])
        return global_similarity[idx]
    
    
    
    already_calculated_set = []
    already_calculated_probability = []
    def find_probability(word_list):
        if word_list in already_calculated_set:
            return already_calculated_probability[already_calculated_set.index(word_list)]
        pro_list = [0 for _ in range(len(topic))]
        topic_probability = []
        for t in range(len(topic)):
            pro_list[t] = find_similarity(' '.join(word_list), topic[t])
        # Softmax Function
        total_sum = 0
        pro_list = pro_list - max(pro_list)
        for t in range(len(topic)):
            total_sum = total_sum + math.exp(pro_list[t])
        for t in range(len(topic)):
            pro_list[t] = (math.exp(pro_list[t]) / total_sum)
        already_calculated_set.append(word_list)
        already_calculated_probability.append(pro_list)
        return pro_list
    
    
    with open(input_json, 'r', encoding='utf-8') as f:
        messages_data = json.load(f)
    df = pd.DataFrame(messages_data)
    new_column_order = ["Chatroom", "Sender", "Timestamp", "Text", "Prompt"]
    df = df[new_column_order]
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s')
    
    
    dataset = df
    dataset['Timestamp'] = pd.to_datetime(dataset['Timestamp'])
    dataset['Date'] = dataset['Timestamp'].dt.date.astype(str)
    # # Dataframe to List
    
    
    
    sentences = []
    for index, row in dataset.iterrows():
        sentences.append([row['Chatroom'],
                          row['Sender'],
                          str(datetime.strptime(str(row['Timestamp']), '%Y-%m-%d %H:%M:%S').date()),
                          str(datetime.strptime(str(row['Timestamp']), '%Y-%m-%d %H:%M:%S').time()),
                          str(row['Text']),
                          str(row['Prompt'])])
    
    
    spell = SpellChecker()
    lemmatizer = WordNetLemmatizer()
    nlp = spacy.load("en_core_web_sm")
    stop_words_spacy = spacy.lang.en.stop_words.STOP_WORDS
    process_sentences = copy.deepcopy(sentences)
    def text_cleaning(text):
        text = re.sub('[^A-Za-z]',' ',text)
        if text is None:
            text = 'nothing'
        text = text.lower()
        text = text.split()
        text = [word for word in text if not word in stop_words_spacy]
        for i in range(len(text)):
            if text[i] is not None:
                text[i] = spell.correction(text[i])
            if text[i] is not None:
                text[i] = lemmatizer.lemmatize(text[i])
            if text[i] is None:
                text[i] = 'nothing'
        text = [word for word in text if not word in stop_words_spacy]
        return text
    for s in range(len(process_sentences)):
        process_sentences[s][4] = text_cleaning(process_sentences[s][4])
    
    
    topic = ['News', 'Research', 'Technology', 'Travel', 'Personal', 'Education', 'Career', 'Health', 'Sports',
            'Vacation', 'Movie', 'Entertainment', 'Book', 'Event', 'Food', 'Politics', 'Finance', 'Relationships',
            'Religion', 'Immigration', 'Fantasy']
    
    start_date = dataset['Timestamp'].min().strftime('%Y-%m-%d')
    end_date = dataset['Timestamp'].max().strftime('%Y-%m-%d')
    
    _start_date = datetime.strptime(start_date, '%Y-%m-%d')
    _end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    date_list = []
    while _start_date <= _end_date:
        date_list.append(_start_date.strftime('%Y-%m-%d'))
        _start_date += timedelta(days=1)

    
    
    target_topic = ['News', 'Research', 'Technology', 'Travel', 'Personal', 'Education', 'Career', 'Health', 'Sports',
            'Vacation', 'Movie', 'Entertainment', 'Book', 'Event', 'Food', 'Politics', 'Finance', 'Relationships',
            'Religion', 'Immigration', 'Fantasy']
    target_topic_index = [-1 for _ in range(len(target_topic))]
    for tp in range(len(target_topic)):
        for mtp in range(len(topic)):
            if topic[mtp] == target_topic[tp]:
                target_topic_index[tp] = mtp
                break
    
    
    preprocess_sentences = []
    preprocess_sentences_full_info = []
    for s in range(len(process_sentences)):
        if process_sentences[s][2] in date_list:
            preprocess_sentences.append(process_sentences[s])
            preprocess_sentences_full_info.append(sentences[s])
    
    
    
    all_data = []
    user_list = []
    user_topic = []
    user_hourly_text = []
    user_hourly_text_full_info = []
    for s in preprocess_sentences:
        if s[1] not in user_list:
            user_list.append(s[1])
    
    
    
    def clear():
        collections.clear()
        collections_embedding.clear()
        global_words.clear()
        global_similarity.clear()
        already_calculated_set.clear()
        already_calculated_probability.clear()
    
    standard_probability = 0.05
    wall_time = 0
    def vector_to_word_list(vector, hourly_text):
        word_list = []
        for v in vector:
            word_list = word_list + hourly_text[v]
        return word_list
    def my_algorithm_for_user(user):
        # Hourly Text
        hourly_text = [[]for h in range(24*len(date_list))]
        hourly_text_full_info = [[]for h in range(24*len(date_list))]
        for s in range(len(preprocess_sentences)):
            if preprocess_sentences[s][1] == user:
                _h = date_list.index(preprocess_sentences[s][2])*24+datetime.strptime(preprocess_sentences[s][3], '%H:%M:%S').hour
                hourly_text[_h] = hourly_text[_h] + preprocess_sentences[s][4]
                hourly_text_full_info[_h].append(preprocess_sentences_full_info[s])
        # My Algorithm
        topic_list = [[]for _ in range(len(target_topic))]
        for tp in range(len(target_topic)):
            cur_point = -1
            Set = []
            PP = []
            for h1 in range(len(hourly_text)):
                if len(hourly_text[h1]) == 0:
                        continue
                Set.append(h1)
                if find_probability(vector_to_word_list([h1], hourly_text))[target_topic_index[tp]] < standard_probability:
                    continue
                PP.append(h1)
            curr = []
            for l in range(len(Set)):
                if Set[l] in PP:
                    if curr == []:
                        curr.append(Set[l])
                    else:
                        temp = copy.deepcopy(curr)
                        temp.append(Set[l])
                        pro = find_probability(vector_to_word_list(temp, hourly_text))[target_topic_index[tp]]
                        if pro >= standard_probability:
                            curr = copy.deepcopy(temp)
                        else:
                            topic_list[tp].append(copy.deepcopy(curr))
                            curr = []
                            curr.append(Set[l])
                elif curr != []:
                    topic_list[tp].append(copy.deepcopy(curr))
                    curr = []
            if curr != []:
                topic_list[tp].append(copy.deepcopy(curr))
        user_topic.append(topic_list)
        user_hourly_text.append(hourly_text)
        user_hourly_text_full_info.append(hourly_text_full_info)
    wall_time = timeit.timeit(lambda: (clear(), [my_algorithm_for_user(user) for user in user_list]), number=1)
    
    
    
    for user in range(len(user_list)):
        print(user_list[user])
        for tp in range(len(target_topic)):
            print(target_topic[tp], user_topic[user][tp])
    
    
    
    number_of_extracted_segments = 0
    total_message_captured = 0
    average_topic_relevance_score = 0
    for user in range(len(user_list)):
        for tp in range(len(target_topic)):
            number_of_extracted_segments = number_of_extracted_segments + len(user_topic[user][tp])
            for seq in user_topic[user][tp]:
                average_topic_relevance_score = average_topic_relevance_score + find_probability(vector_to_word_list(seq, user_hourly_text[user]))[target_topic_index[tp]]
                for s in seq:
                    total_message_captured = total_message_captured + len(user_hourly_text_full_info[user][s])
    print('Number of Extracted Segments : ', number_of_extracted_segments)
    print('Average Length of Segments : ', total_message_captured/max(1,number_of_extracted_segments))
    print('Total Message Captured : ', total_message_captured)
    print('Wall Time : ', wall_time)
    print('Average Topic Relevance Score : ', average_topic_relevance_score/max(1,number_of_extracted_segments))
    
    
    
    def start_time(hour, user):
        user_index = user_list.index(user)
        date_find = int(hour/24)
        _hour = '23:59:59'
        for s in range(len(user_hourly_text_full_info[user_index][hour])):
            if hour%24 == datetime.strptime(user_hourly_text_full_info[user_index][hour][s][3], '%H:%M:%S').hour and date_list[date_find] == user_hourly_text_full_info[user_index][hour][s][2]:
                if datetime.strptime(_hour, '%H:%M:%S') > datetime.strptime(user_hourly_text_full_info[user_index][hour][s][3], '%H:%M:%S'):
                    _hour = user_hourly_text_full_info[user_index][hour][s][3]
        return _hour
    def end_time(hour, user):
        user_index = user_list.index(user)
        date_find = int(hour/24)
        _hour = '00:00:00'
        for s in range(len(user_hourly_text_full_info[user_index][hour])):
            if hour%24 == datetime.strptime(user_hourly_text_full_info[user_index][hour][s][3], '%H:%M:%S').hour and date_list[date_find] == user_hourly_text_full_info[user_index][hour][s][2]:
                if datetime.strptime(_hour, '%H:%M:%S') < datetime.strptime(user_hourly_text_full_info[user_index][hour][s][3], '%H:%M:%S'):
                    _hour = user_hourly_text_full_info[user_index][hour][s][3]
        return _hour
    def time_difference(t1, t2, user):
        time1 = date_list[int(t1/24)] + ' ' + start_time(t1, user)
        time2 = date_list[int(t2/24)] + ' ' + end_time(t2, user)
        time_diff = datetime.strptime(time2, '%Y-%m-%d %H:%M:%S') - datetime.strptime(time1, '%Y-%m-%d %H:%M:%S')
        return time_diff
    def chat_summarization(seq_set, user):
        user_index = user_list.index(user)
        chats = ''
        for seq in seq_set:
            for s in range(len(user_hourly_text_full_info[user_index][seq])):
                chats = chats + '\n' + user + ' :: ' + user_hourly_text_full_info[user_index][seq][s][0] + ' [' + user_hourly_text_full_info[user_index][seq][s][2] + ' ' + user_hourly_text_full_info[user_index][seq][s][3] + ']: ' + user_hourly_text_full_info[user_index][seq][s][4]
        return chats
    for user in range(len(user_list)):
        for tp in range(len(target_topic)):
            for l in range(len(user_topic[user][tp])):
                all_data.append({"User":user_list[user],
                             "Start Date":date_list[int(user_topic[user][tp][l][0]/24)],
                             "Start Time":start_time(user_topic[user][tp][l][0],user_list[user]),
                             "End Date": date_list[int(user_topic[user][tp][l][len(user_topic[user][tp][l])-1]/24)],
                             "End Time" : end_time(user_topic[user][tp][l][len(user_topic[user][tp][l])-1],user_list[user]),
                             "Time Duration": str(time_difference(user_topic[user][tp][l][0], user_topic[user][tp][l][len(user_topic[user][tp][l])-1], user_list[user])),
                             "Topic" : target_topic[tp],
                             "Probability": str(find_probability(vector_to_word_list(user_topic[user][tp][l], user_hourly_text[user]))[target_topic_index[tp]]),
                             "Chat Summary": chat_summarization(user_topic[user][tp][l], user_list[user])
                                })
    
    overlap_df = pd.DataFrame(all_data)
    if len(overlap_df) != 0:
        overlap_df["Start"] = pd.to_datetime(overlap_df["Start Date"] + " " + overlap_df["Start Time"])
        overlap_df["End"]   = pd.to_datetime(overlap_df["End Date"] + " " + overlap_df["End Time"])
        overlap_df["Covered"] = False
        overlap_df = overlap_df.sort_values(
            by = ["User", "Start", "End"],
                ascending = [True, True, False]
            ).reset_index(drop=True)
        cur_user = ""
        cur_index = -1
        for df_ittr in range(len(overlap_df)):
            if overlap_df.iloc[df_ittr]["User"] != cur_user:
                cur_user = overlap_df.iloc[df_ittr]["User"]
                cur_index = df_ittr
            for df_overlap in range(cur_index, len(overlap_df)):
                if overlap_df.iloc[df_ittr]['User'] != overlap_df.iloc[df_overlap]['User']:
                    break
                if df_ittr != df_overlap and \
                overlap_df.iloc[df_ittr]['Start'] >= overlap_df.iloc[df_overlap]['Start'] and \
                overlap_df.iloc[df_ittr]['End']  <= overlap_df.iloc[df_overlap]['End'] and \
                float(overlap_df.iloc[df_ittr]['Probability']) <= float(overlap_df.iloc[df_overlap]['Probability']) :
                    overlap_df.loc[df_ittr, "Covered"] = True
                    break
        final_df = overlap_df[overlap_df["Covered"] != True]
    json_output = final_df.to_json(orient='records', indent=2)
    return json_output
    #with open('SCOPE_output.json', 'w', encoding='utf-8') as f:
        #f.write(json_output)
    #pd.DataFrame(all_data).to_csv('Ouput_by.csv')

def main():
    if len(sys.argv) < 2:
        print("Usage: python SCOPE.py <input_json>")
        sys.exit(1)

    input_json = sys.argv[1]
    
    if not os.path.exists(input_json):
        print("Input JSON not found")
        sys.exit(1)
    
    json_output = run_SCOPE(input_json)
    
    #with open(input_json, "r", encoding="utf-8") as f:
        #messages = json.load(f)

    # ---- YOUR ANALYSIS CODE HERE ----
    #print("Messages received:", len(messages))

    #Example output
    #output = {
        #"total_messages": len(messages),
        #"status": "processed"
    #}

    output_path = input_json.replace(".json", "_output.json")

    with open(output_path, "w", encoding="utf-8") as f:
        #json.dump(output, f, indent=2)
        f.write(json_output)

    print("SCOPE analysis completed")

if __name__ == "__main__":
    main()



