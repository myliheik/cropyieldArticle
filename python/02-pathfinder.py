

"""

PathFinder creates paths to all files between start- and enddate 

"""
import os
import userinput

def makefilepaths(userinput):

    tilepath = userinput.datadir
    filepaths = []
    
    #for tilepath in tilepaths:
    for filename in os.listdir(tilepath):
        print(filename)
        date = filename.split('_')[2].split('T')[0]
        if not userinput.enddate is None and not userinput.startdate is None:
            if date <= userinput.enddate and date >= userinput.startdate:
                filepath = os.path.join(tilepath,filename)
                filepaths.append(filepath)
        else:
            filepath = os.path.join(tilepath,filename)
            filepaths.append(filepath)
    
    return filepaths

def makebandname(userinput):
    
    bandnames = []
    
    for band in userinput.bandlist:
        if band > 10:
            if band == 13:
                bandname = 'B8A_20m'
            else:
                bandname = 'B'+str(band) + '_20m'
        elif band in [9,1]:
            bandname = 'B0'+str(band) +'_60m'
        elif band in [2,3,4,8]:
            bandname = 'B0'+str(band) +'_10m'
        else:
            bandname = 'B0'+str(band) + '_20m'
        
        bandnames.append(bandname)
    return bandnames
    
def makebandpaths():

    ui = userinput.UserInput()
    
    #from all filepaths, extend to matching band paths
    
    filepaths = makefilepaths(ui)
    bandpaths = []
    
    for filepath in filepaths:
        granulepath = os.path.join(filepath,'GRANULE')
        betweenpath = os.path.join(granulepath,os.listdir(granulepath)[0])
        imgpath = os.path.join(betweenpath,'IMG_DATA')
        r10 = os.path.join(imgpath,'R10m')
        r20 = os.path.join(imgpath,'R20m')
        r60 = os.path.join(imgpath,'R60m')
        bandlist = makebandname(ui)
        #print(bandlist)
        for rdir in [r10,r20,r60]:
            mylist = [os.path.join(rdir,bandfile) for bandfile in os.listdir(rdir) if bandfile.split('_')[-2] +'_' + bandfile.split('_')[-1][:3] in bandlist]
            bandpaths.extend(mylist)
            #print(mylist)
    
    to_txt(bandpaths)
            
def to_txt(paths):

    with open('bandpaths.txt', 'w') as f:
        for item in paths:
            f.write("%s\n" % item)

        
if __name__ == "__main__":
    makebandpaths()


