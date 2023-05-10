# -*- coding: utf-8 -*-

import json 
import ipdb 
import requests
from transformers import AutoTokenizer, AutoModelForTokenClassification, pipeline


def run_baseline():
    sent_en, sent_de, ans = [], [], []
    with open("../coypu-questions-all.json") as f:
        qs = json.load(f)
        for i in qs["questions"]:
            sent_en.append(i["question"][0]['string'])
            sent_de.append(i["question"][1]['string'])
        for i in qs["questions"]:
            ans.append(i["answers"])
        assert len(sent_en)==len(sent_de)
    '''
        for i in range(len(sent_en)-1):
            print(sent_en[i])
            print(sent_de[i])
            print(ans[i])
            
    '''


    # baseline run on vicuna
    import json
    import requests
    name = '''
    #COMPANY NAME
    '''
    details = '''
    #COMPANY DETAILS
    '''
    system = '''
    You are a German supply chain-GPT, which knows about supply chain, risk management and supplier information for manufacturing companies.
    You understand German but you only speak English
    You receive a cquestion about this fields. Answer in English precisely with information about global supply chains.
    #If unsure, suggest a reliable source to check.
    '''
    user = f'''
    #The company name is: """{name}""". 
    #Further information about the company: """{details}""".
    #The list is: 
    '''
    for i in range(len(sent_en)-1):
        payload = {
            "model": "vicuna-13b-v1.1",
            "key": "M7ZQL9ELMSDXXE86",
            "temperature": 0,
            "max_tokens": 512,
            "messages": [{"role": "system", "content": system}, {"role": "user", "content": sent_de[i]}],
        }
        headers = {'Content-Type': 'application/json'}
        response = requests.post('https://turbo.skynet.coypu.org', headers=headers, json=payload)
        print(sent_de[i])
        print(ans[i])
        print(json.loads(response.text)[0]['choices'][0]['message']['content'])
        print('\n\n\n\n')

    return 


def extract_mention(dataset):
    sent_en, sent_de, ans = [], [], []
    with open(dataset) as f:
        qs = json.load(f)
        for i in qs["questions"]:
            sent_en.append(i["question"][0]['string'])
            sent_de.append(i["question"][1]['string'])
        for i in qs["questions"]:
            ans.append(i["answers"])
        assert len(sent_en)==len(sent_de)==len(ans)
    model = AutoModelForTokenClassification.from_pretrained("imvladikon/t5-english-ner", trust_remote_code=True)
    tokenizer = AutoTokenizer.from_pretrained("imvladikon/t5-english-ner", trust_remote_code=True)
    T5tagger = pipeline('ner', model=model, tokenizer=tokenizer, aggregation_strategy="simple")

    tokenizer = AutoTokenizer.from_pretrained("Jean-Baptiste/roberta-large-ner-english")
    model = AutoModelForTokenClassification.from_pretrained("Jean-Baptiste/roberta-large-ner-english")
    RobertaTagger = pipeline('ner', model=model, tokenizer=tokenizer, aggregation_strategy="simple")

    res = list()
    for txt in sent_en:
        res.append(get_ent(txt, T5tagger, RobertaTagger))
    
    return sent_en, sent_de, res

    # merging policy: if the string is the same and their types are the same, them merge.
    # roberta has 4 entity types MISC PER ORG and LOC
    # while in T5 tagger there's DATE, LOCATION, ORGANIZATION, OTHER, PERSON, QUANTITY, 
    
def get_ent(txt, T5tagger, RobertaTagger): 
    labelMap = {"PER":"PERSON", "ORG":"ORGANIZATION", "LOC":"LOCATION", "MISC":"OTHER"}

    ents = dict()
    for ent in T5tagger(txt):
        ents[ent["word"].strip()] = ent["entity_group"]
    for ent in RobertaTagger(txt):
        if ent["word"].strip() in ents:
            if ent["entity_group"] in labelMap:
                if labelMap[ent["entity_group"]] == ents[ent["word"].strip()]:
                    continue
        else:
            ents[ent["word"].strip()] = ent["entity_group"]

    return ents



if __name__ == '__main__':
    sents, _, entities = extract_mention("../coypu-questions-all.json")
    for sent, ent in zip(sents, entities):
        print(sent, ent)

