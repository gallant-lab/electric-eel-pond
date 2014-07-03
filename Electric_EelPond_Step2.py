import os
import argparse
import sys
import subprocess
import glob
import datetime
import shutil
import itertools
import filecmp
import gzip

###---------------###
### Main function ###
###---------------###
def main():
	print '****************************************************'
	print '------------- ELECTRIC EEL POND STEP 2 -------------'
	print '****************************************************'

	print "DATE: " + str(datetime.datetime.today())
	#get input arguments
	args = get_args()
	
	#Check proper file input indication
	if args.inputdirs and args.inputfiles:
		print "***ERROR***\n Both types of file input selected"
		print "Please choose:"
		print "  -d  Input directories"
		print "  -i  Input files"
		exit()

	if not args.inputdirs and not args.inputfiles:
		print "***ERROR***\n No input type selected"
		print "Please choose one:"
		print "  -d  Input directories"
		print "  -i  Input files\n"
		exit()

	if not args.outputdir:
		print "***ERROR***\n No output directory specified"
		print "Please indicate output directory using -o\n"
		exit()

	##Check input and output folders ##
	#check if all input directories exists, error if one does not exist
	if args.inputdirs:
		for i,p in enumerate(args.inputdirs):
			if p == '.':
				path = os.getcwd()
				args.inputdirs[i] = '../'+ os.path.basename(path) + '/'
			if p[-1] == '/':
				args.inputdirs[i] = p[:-1]
			if os.path.exists(os.path.join(p)) == False:
				print "***ERROR***\n"+p+"\n*Input path does not exist*"
				exit()

	#check if output directory exists, create one if does not exists
	if os.path.exists(os.path.join(args.outputdir)) == False:
		os.mkdir(os.path.join(args.outputdir))
		print "-Making output directory '" + args.outputdir +"'\n"

	#print info to receipt
	receipt = open(args.outputdir + "/receipt.txt", "w+")
	receipt.write("----BEGIN RECEIPT----\n")
	receipt.write("DATE: " + str(datetime.datetime.today())+"\n")
	receipt.write("-----------------------------------------\n")

	#Default walltime if not specified
	if args.walltime: #check formatting of walltime.  should be hh:mm:ss.
		if len(args.walltime) < 3:
			print "***ERROR***"
			print "Walltime not specified correctly"
			print "Use hh:mm:ss"
			exit()
		if not args.walltime[-3] == ':' or not args.walltime[-6] == ':':
			print "***ERROR***"
			print "Walltime not specified correctly"
			print "Use hh:mm:ss"
			exit()
		if not args.walltime[-2:].isdigit() or not args.walltime[-5:-4].isdigit() or not args.walltime[-7].isdigit() or not args.walltime[0].isdigit():
			print "***ERROR***"
			print "Walltime not specified correctly"
			print "Use hh:mm:ss"
			exit()
	if not args.walltime:
		args.walltime = '20:00:00'
	print '-Walltime: ' + args.walltime

	#Check for normalized flag
	if args.normalize:
		print "-Normalize: Yes"
	else:
		print "-Normalize: No"

	#Prompt for parameters if not given in program command
	#-Prompt for threads
	if not args.threads: #user has not provided threads.
		while True:
			print 'Number of threads to use?'
			threads = raw_input('-->')
			if threads.isdigit():
				args.threads = threads
				break
			else:
				print "Invalid response."
	else:
		if args.threads.isdigit(): #user provided threads in command.
			print '-Threads: ' + args.threads
		else:
			print "***ERROR***\nThreads requires a number"
			print "You input: " + args.threads
			exit()
	#-Prompt for reqmem
	estimate = False
	if not args.reqmem:
		while True:
			print "You have not provided required memory. Enter '?' to use recommendation, or Gb number:"
			reqmem = raw_input('-->')
			if reqmem.isdigit(): #user has indicated how much memory to use.
				args.reqmem = reqmem
				break
			if reqmem == '?': #user has asked for recommendation.
				estimate = True
				print "Calculation of recommended memory will commence upon confirmation of input files."
				break
			else:
				print "Invalid response."
	else:
		if args.reqmem.isdigit(): #user has provided reqmem in command.
			print '-Requested memory for Trinity: ' + args.reqmem + "Gb"
		else:
			print "***ERROR***\nRequired Memory requires a number"
			print "You input: " + args.reqmem
			exit()

	#write parameters to receipt.
	receipt.write("-Walltime: " + args.walltime + '\n')
	receipt.write("-Normalize: ")
	if args.normalize:
		receipt.write("Yes\n")
	else:
		receipt.write("No\n")
	receipt.write("-Threads: " + args.threads + '\n')
	if not estimate:
		receipt.write("-Reqested Memory for Trinity: " + args.reqmem + 'Gb\n')
	if estimate:
		receipt.write("-Calculation of recommended memory will commence upon confirmation of input files.\n\n")


	filelist = {} #dictionary of key = input directories and value = the files for analysis

	#if input is list of directories, find all the files by walking the directories
	if args.inputdirs:
		#generate the list of .qc.fq.gz files for analysis
		for d in args.inputdirs:
			for root,dirs,files in os.walk(d):
				for f in files:
					add = True
					if f.endswith('.qc.fq.gz'):
						#check if file already in filelist
						for k,i in filelist.items():
							for c in i:
								if filecmp.cmp(root+'/'+f,k+'/'+c):
									add = False
						if add:
							if root in filelist.keys():
								if f not in filelist[root]:
									filelist[root].append(f)
							else:
								filelist[root] = [f]

	#if input is list of files, generate the filelist from the filenames/paths
	if args.inputfiles:
		for f in args.inputfiles:
			if os.path.dirname(f) in filelist.keys():
				filelist[os.path.dirname(f)].append(os.path.basename(f))
			else:
				filelist[os.path.dirname(f)]=[os.path.basename(f)]

	#Separate the files into left, right, and single files
	leftlist,rightlist,singlelist = ClassifyFiles(filelist, receipt, args)
	
	#calculate required memory if estimation chosen
	if estimate:
		print "\nCalculating memory estimation now."
		print "Memory estimation is based on 1Gb to 1 million reads ratio."
		args.reqmem = (estimate_memory(leftlist)*2) + estimate_memory(singlelist)
		while True:
			print "\nEstimated memory:", args.reqmem, "Gb.  Continue with this estimation (y/n)?"
			confirm = raw_input("-->")
			if confirm == "y":
				print '-Memory:', args.reqmem, "Gb"
				receipt.write('Using estimated memory: ' + str(args.reqmem) + "Gb")
				receipt.write("\nMemory estimation based on 1Gb to 1 million reads ratio.")
				break
			elif confirm == "n":
				while True:
					print "Enter required memory in Gb: "
					reqmem = raw_input('-->')
					if reqmem.isdigit():
						args.reqmem = reqmem
						print "-Memory:", args.reqmem + "Gb"
						receipt.write('-Memory: ' + str(args.reqmem) + "Gb")
						break
					else:
						print "Invalid input."
				break
				
	#Create the qsub file and submit to hpcc
	CreateQsub(leftlist,rightlist,singlelist,args)
###------------###
###end function###

###------------------###
### Estimate threads ###
###------------------###
def estimate_memory(leftlist): #1Gb/10^6 reads
	mem = 0
	for directory in leftlist:
		for f in leftlist[directory]:
			mem += int(subprocess.check_output("zcat " + directory + "/" + f + " | wc -l", shell=True))
	return mem/4000000
###------------###
###end function###

			
###------------------###
### Get Confirmation ###
###------------------###
def get_confirmation(rlist, direction, receipt,args): #returns correct list.

	newlist = {} #delete all directories in rlist without files
	for d in rlist:
		if rlist[d]:
			newlist[d] = rlist[d]
	rlist = newlist
	
	#Ask user if using single-ended files
	if direction.title() == "Single" and rlist:
		while True:
			print "\nUse single files? (y/n)"
			response = raw_input("-->")
			if response == 'n':
				rlist = {}
				break
			elif response == 'y':
				break
			else:
				print "Invalid response."

	#while the list is not empty
	while rlist:	
		print "\n", direction.title(), "files:"
		count = 0 #number of files in list
		#Print out files in list
		for p in sorted(rlist):
			if rlist[p]:
				print '- ' + os.path.basename(p) + '/'
			for f in rlist[p]:
				print "\t" + str(count) + ".",  f
				#increment count of file number				
				count += 1
		
		#if no files
		if not rlist:		
			print "--No", direction, "files--"
		#if there are files, confirm with user
		else:
			print "Are these the correct", direction, "reads? (y/n)"
			permission = raw_input('-->')
			#check for valid response
			if permission != "y" and permission != "n":
				print "Invalid response."
			#if yes
			if permission == "y":
				#write final files to receipt
				receipt.write("\n\n" + direction.title()+" files:\n")
				for p in sorted(rlist):
					if rlist[p]:
						if p[0:2] == '..':		
							receipt.write('- ' + p[2:]+'/\n') #disregard ".." if used in command.
						else:
							receipt.write('- ' + p+'/\n') #print directory
					for f in rlist[p]:
						receipt.write("\t-"+ f + '\n') #print files in directory
				receipt.write('\n')
				break
			#if no, correct list with fix_list.
			if permission == "n":
				rlist = fix_list(rlist, direction, count)
	return rlist
###------------###
###end function###


###--------------###
### Fix filelist ###
###--------------###
def fix_list(rlist, direction, count): #takes in flawed list, returns corrected list.
	remlist = {} #list of files to delete
	while True:
		#count files in dictionaries
		remcount = 0
		rlistcount = 0
		for d in remlist:
			for f in remlist[d]:
				remcount += 1
		for d in sorted(rlist):
			for f in rlist[d]:
				rlistcount += 1
		if remcount < rlistcount: #while there are still files to be selected from rlist
			print "Please enter incorrect file number. Input 'd' when done: "
			index = raw_input("-->")
			#check for non-numeric response
			if index.isalpha():
				#if user indicates done, remove all indicated files
				if index == 'd':
					if remlist: #if there are files in remlist
						print "\n"+ direction.title()+ " files to remove:"
						for r in sorted(remlist):	
							print "- " + os.path.basename(r)
							for f in remlist[r]:
								print "\t" + f
						while True:	#prompt for confirmation of deletion.						
							print("Are you sure you want to delete these (y/n)?")
							confirm = raw_input("-->")
							if confirm == "y":
								for r in remlist:
									for f in remlist[r]:
										rlist[r].remove(f)
								break
							elif confirm == "n":
								return rlist #retry selection of rlist files to use.
							else:
								print "Not a valid response."
					else: #there are no files in remlist.
						print "No file selected for deletion!"
						return rlist #retry selection of rlist files to use.
					break
				else: #not a valid response.  not a number and not "d"
					print "Not a number."
			
			#check non-numeric responses
			elif not index.isdigit():
				print "Not a valid response."
			
			#if number not in range
			elif int(index) >= int(count):
				print "Number out of range!"
			
			#input is valid.  add index to remlist
			else:
				index = int(index)
				for p in sorted(rlist):
					if index < len(rlist[p]):
						print "---" + rlist[p][index]
						#add file to remove to removelist
						if p in remlist.keys():
							if rlist[p][index] in remlist[p]:
								print "File already selected."
							else:
								remlist[p].append(rlist[p][index])
						else:
							remlist[p] = [rlist[p][index]]
						break
					else:
						index = index - len(rlist[p])

		else: #user has selected every file for deletion.
			while True: #prompt for confirmation of deletion.
				print "Delete all", direction, "reads (y/n)?"
				confirm = raw_input("-->")
				if confirm != "y" and confirm != "n":
					print "Invalid response."
				if confirm == "y": #delete all reads.
					for r in remlist:
						for f in remlist[r]:
							rlist[r].remove(f)
					break
				if confirm == "n": #start over.
					break
			break

	#remove all empty list entries from dictionary.
	returnlist = {}
	for d in rlist:
		if rlist[d]:
			returnlist[d] = rlist[d]
	return returnlist

###------------###
###end function###

###------------------------------------###
### Classify files into left and right ###
###------------------------------------###
def ClassifyFiles(filelist, receipt,args):

	leftlist = {}  #dictionary of key = input directory, value = R1 files
	rightlist = {}	 #dictionary of key = input directory, value = R2 files
	singlelist = {}  #dictionary of key = input directory, value = single-ended files
	
	#sort files in dictionary
	for p in filelist:
		leftlist[p] = []	#list of left files (R1)
		rightlist[p] = []	#list of right files (R2)
		singlelist[p] = []	#list of single-ended files
		for f in filelist[p]:
			if "R1" in f:
				leftlist[p].append(f)
			if "R2" in f:
				rightlist[p].append(f)
			if ".se.qc.fq.gz" in f:
				singlelist[p].append(f)
	
	#get confirmation from the user, and allow for file deletion from lists
	leftlist = get_confirmation(leftlist, "left", receipt,args)
	rightlist = get_confirmation(rightlist, "right", receipt,args)
	singlelist = get_confirmation(singlelist, "single", receipt,args)

	return leftlist, rightlist, singlelist

###------------###
###end function###


###----------------------###			
### Create the qsub file ###
###----------------------###
def CreateQsub(leftlist,rightlist,singlelist,args):
	left = ''
	right = ''

	#create strings to pass into qsub of right and left files.
	for directory in leftlist:
		for fil in leftlist[directory]:
			left += directory + '/' + fil + ' '
	#concatenate left and single files together.
	for directory in singlelist:
		for fil in singlelist[directory]:
			left += directory + '/' + fil + ' '

	for directory in rightlist:
		for fil in rightlist[directory]:
			right += directory + '/' + fil + ' '
	
	totalmem = int(args.reqmem)+2
	jobname = os.path.basename(args.outputdir)
	command = "Trinity.pl"
	if args.normalize:
		command += " --normalize_reads"	

#write qsub file
	cli = '''
### define resources needed:
### walltime - how long you expect the job to run
#PBS -l walltime={2}

### nodes:ppn - how many nodes & cores per node (ppn) that you require
#PBS -l nodes=1:ppn={3}

### mem: amount of memory that the job will need
#PBS -l mem={6}gb

### you can give your job a name for easier identification
#PBS -N {7}

###outputs
#PBS -e {5}/error.txt
#PBS -o {5}/output.txt

#load necessary modules
module load trinity/20140413p1
module load bowtie
module load samtools
module load jellyfish

#call Trinity
{8} --seqType fq --left {0} --right {1} --CPU {3} --JM {4}G --output {5} >> {5}/receipt.txt

'''.format(left, right, args.walltime, args.threads, args.reqmem, args.outputdir, totalmem, jobname, command)

	cli += '''
qstat -f ${PBS_JOBID}
'''
	#create and write to qsub file       
	filename='{}/trinity.qsub'.format(os.path.join(args.outputdir))
	f= open(filename,'w+')
	f.write(cli)
	f.close()

	#create command line option and call subprocess
	cli_parts=["qsub",filename]

	ft = subprocess.call(cli_parts)

	print  "\n---Job has been submitted to the hpcc---"
###------------###
###end function###

###------------------------###
### Custom argument action ###
###------------------------###
class FullPaths(argparse.Action):
    """Expand user- and relative-paths"""
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, os.path.abspath(os.path.expanduser(values)))
###---------###
###end class###

###-----------------------------------###
### Get arguments passed into program ###
###-----------------------------------###
def get_args():
	parser = argparse.ArgumentParser(description='Submit trimmed sequence files to Trinity')
	parser.add_argument('-d', '--inputdirs', nargs='+', help = 'The input directory/directories for semiautomated running')
	parser.add_argument('-i', '--inputfiles', nargs='+', help = 'The input file(s)')
	parser.add_argument('-o', '--outputdir', help='The output directory', action = FullPaths)
	parser.add_argument('-t', '--threads', help='Number of threads to use', action ="store")
	parser.add_argument('-r', '--reqmem', help='Amount of memory to request for Trinity (in gb)', action="store")
	parser.add_argument('-w', '--walltime', help='Walltime to request (hh:mm:ss) (default=20:00:00)', action ="store")
	parser.add_argument('-n', '--normalize', help='Apply normalization to reads before submission to Trinity.', action = "store_true")
	
	return parser.parse_args()
###------------###
###end function###

#call main function upon program start
if __name__ == '__main__':
	main()
