import pandas as pd
import numpy as np
from pprint import pprint
from random import randrange

# df = pd.DataFrame(np.random.randint(0,100,size=(20,5)), index=np.arange(100,120))
# pprint(df)
# df.to_csv('demo-data', index=False, sep='\t', header=False)

rsize=1000
csize=5

batch_no = 0
l = batch_no*100
r = (batch_no+1)*100


for i in range(l,r):
	filepath = "input/data-"+str(i).rjust(6,'0')
	rng = randrange(100,1000,10)
	df = pd.DataFrame(np.random.randint((rng-100),rng,size=(rsize,csize)), index=np.arange(i*rsize+1,(i+1)*rsize+1))
	df.to_csv(filepath, sep='\t', header=False) # , index=False

