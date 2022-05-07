import numpy as np
import os

filenames = ['file1.csv','file2.csv','file3.csv','file4.csv']


for i in filenames:
    with open(os.path.join(os.getcwd(),i),'w') as f:
        f.write('a,b,c,d,e,f,g\n')
        for i in range(1000000):
            string_complete = ','.join([str(i) for i in np.random.rand(7)])
            string_complete = string_complete + '\n'
            f.write(string_complete)
        f.close()
