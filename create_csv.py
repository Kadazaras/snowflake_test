import numpy as np
import os

filenames = ['file{}.csv'.format(i) for i in range(10)]

if os.path.isdir(os.path.join(os.getcwd(),'data')):
    pass
else:
    os.mkdir(os.path.join(os.getcwd(),'data'))

fileDir = os.path.join(os.getcwd(),'data')

for i in filenames:
    with open(os.path.join(fileDir,i),'w') as f:
        f.write('a,b,c,d,e,f,g\n')
        for i in range(4000000):
            string_complete = ','.join([str(i) for i in np.random.rand(7)])
            string_complete = string_complete + '\n'
            f.write(string_complete)
        f.close()
