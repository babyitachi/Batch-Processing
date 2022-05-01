from os import listdir
from os.path import isfile, join
import csv
from celery import Celery
import json,time
import random
import os
import shutil

app=Celery('tasks', backend='redis://localhost', broker='pyamqp://guest@localhost//')

def readCsvData(filepath):
	try:
		rows=[]
		with open(filepath, mode='r', newline='\r',encoding='utf-8') as f:
			for text in f:
				if text == '\n':
					continue
				sp = text.split(',')[4:-2]
				tweet = " ".join(sp)
				rows.append(tweet)
		return rows
	except:
		return []

@app.task
def countWord(filepath):
	try:
		countmap={}
		textrows=readCsvData(filepath)
		for text in textrows:
			text=text.split(' ')
			for word in text:
				if word not in countmap:
					countmap[word]=1
				else:
					countmap[word]=countmap[word]+1
		with open('red/'+filepath.split("/")[-1]+'.json','w') as w:
			json.dump(countmap,w)
		return filepath
	except:
		return ""

@app.task
def printop(op):
	return

@app.task
def gatherOp(file1,file2):
	try:
		path='red/'

		f1= open(path+file1)
		f2= open(path+file2)

		data1=json.load(f1)
		data2=json.load(f2)

		for k in data2:
			if k in data1:
				data1[k]=data1[k]+data2[k]
			else:
				data1[k]=data2[k]

		with open(path+file1,'w') as w:
			json.dump(data1,w)

		os.remove(path+file2)
		return file1
	except:
		return ""

