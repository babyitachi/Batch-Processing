from celery import chord,group,chain
from tasks import countWord,gatherOp,printop
from os import listdir,rename
from os.path import isfile, join
import os,math,shutil,pathlib,sys,json

def checkparams():
	try:
		if len(sys.argv)<2:
			print("Use the command: python3 client.py <data_dir>")

		DIR=sys.argv[1]
		no=0
		if len(sys.argv)>=3:
			no=int(sys.argv[2])
		return DIR,no
	except:
		return "",0

def map(DIR,no):
	try:
		os.makedirs('red',exist_ok=True)
		tweetfiles=[join(DIR,f) for f in listdir(DIR) if isfile(join(DIR,f))]
		if no!=0:
			tweetfiles=tweetfiles[:no]
		y=chord(countWord.s(tweetfiles[i]) for i in range(len(tweetfiles)))(printop.s())
		y.get()
		return len(tweetfiles)
	except:
		print("Error occured during MAP operation")
		return 0

def reduce(nooffiles):
	try:
		steps = int(math.log(nooffiles,2))

		for s in range(steps+1):
			filelist=listdir('red')
			if len(filelist)==1:
				break
			x = chord(gatherOp.s(filelist[2*i],filelist[2*i+1]) for i in range(int(len(filelist)/2)))(printop.s())
			x.get()

		filelist=listdir('red')
		shutil.move('red/'+filelist[0],filelist[0])
		rename(filelist[0],'output.json')
		path=pathlib.Path('red')
		path.rmdir()
	except:
		print("Error occured during REDUCE operation")

def printresult():
	try:
		o={}
		with open('output.json','r') as p:
			o=json.load(p)
		print(o)
	except:
		print("Error occured while printing results")

if __name__ == "__main__":
	dir,no = checkparams()
	nooffiles =  map(dir,no)
	reduce(nooffiles)
	printresult()
