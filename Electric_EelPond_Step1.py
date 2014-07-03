import os
import sys
import glob
import shutil
import argparse
import itertools
import subprocess
import datetime
import getpass

###---------------###
### Main function ###
###---------------###
def main():
	print '**************************************************'
	print '------------ ELECTRIC EEL POND STEP 1 ------------'
	print '**************************************************'
	
	fastqfiles=[] #list of fastq files in input directory
	
	print "DATE: " + str(datetime.datetime.today())
	#get input arguments
	args = get_args()

	#check for input and output specification
	if not args.inputdir:
		print "***ERROR***\nNo input directory specified"
		print "Indicate input directory using -i"
		exit()

	if not args.outputdir:
		print "***ERROR***\nNo output directory specified"
		print "Indicate output directory using -o"
		exit()

	##Check input and output folders ##
	#check if input directory exists, error if does not exist
	if os.path.exists(os.path.join(args.inputdir)) == False:
		print "***ERROR***\nInput path does not exist"
		exit()

	#check if output directory exists, create one if does not exists
	if os.path.exists(os.path.join(args.outputdir)) == False:
		os.mkdir(os.path.join(args.outputdir))
		print "-Making output directory '" + args.outputdir +"'\n"

	#create receipt
	receipt = open(args.outputdir+"/receipt.txt", 'a+') #write receipt text file to output directory
	receipt.write("----BEGIN RECEIPT----\n")
	receipt.write("DATE: " + str(datetime.datetime.today())+"\n")
	receipt.write("-----------------------------------------\n")
	
	## Analyze input arguments ##
	#check if both naming conventions defined, error if both defined
	if args.msu and args.cornell:
		print "***ERROR***"
		print "Two naming conventions indicated"
		print "Please choose one:"
		print "  -m  MSU naming convention"
		print "  -c  Cornell naming convention"
		exit()
	#check if no convention defined, default to msu
	if not args.msu and not args.cornell:
		args.msu = True
	#indicate msu convention
	if args.msu:
		print "-Naming convention: MSU"
		receipt.write("-Naming convention: MSU\n")
	#indicate cornell convention
	if args.cornell:
		print "-Naming convention: Cornell"
		receipt.write("-Naming convention: Cornell\n")
	
	#indicate thread number	and check for proper input
	if not args.threads:
		args.threads = '1'
	else:
		if not args.threads.isdigit():
			print "***ERROR***\nThreads requires a number"
			print "You input: " + args.threads
			exit()
	print '-Threads: ' + args.threads
	receipt.write('-Threads: ' + args.threads + '\n')	
	
	#indicate requested memory and check for proper input
	if not args.reqmem:
		args.reqmem = '12'
	else:
		if not args.reqmem.isdigit():
			print "***ERROR***\nRequired Memory requires a number"
			print "You input: " + args.reqmem
			exit()
	print '-Requested memory: ' + args.reqmem + "Gb"
	receipt.write("-Requested memory: " + args.reqmem + 'gb\n')

	#indicate requested walltime and check for proper input
	if not args.walltime:
		args.walltime = '04:00:00'
	else:
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
	print "-Walltime: " + args.walltime
	receipt.write("-Walltime: " + args.walltime + '\n')

	#indicate if split chosen
	if args.split:
		print "-Split: Yes\n"
		receipt.write("-Split: Yes\n\n")
	else:
		print "-Split: No\n"
		receipt.write("-Split: No\n\n")


	##grab fastq files from input directory
	for f in os.listdir(args.inputdir):
		if f.endswith('.fastq.gz'):
			fastqfiles.append(f)
	
	#combine all lanes
	comfastqfiles, comlist, names = combine_lanes(args,fastqfiles, receipt)


	#create list of files to pass to qsub file
	filelist, read1files, read2files = create_filelist(comfastqfiles, receipt)

	#manage file concatenation and movement
	manage_files(args, comlist, names, read1files, read2files, receipt, filelist)

	#create qsub file
	qsub = create_qsub(filelist,args)
	
	receipt.close()
###------------###
###end function###

###------------------------------------###
### Combine all lanes into single file ###
###------------------------------------###
def combine_lanes(args,fastqfiles, receipt):
	names = {} #dictionary of original names and their shortened names
	comlist = {} #dictionary containing list of files concatenated
	comfastqfiles = []  #list of files to do work on
	

	###if msu naming convention###
	if args.msu:
		#go through each fastq file
		for f in fastqfiles:
			#if a lane file
			if "L00" in f:
				#determine what its shortened name would be
				shortname = f[0:f.index('L00')] + f[f.index('L00')+5:]

				filename = f
				#look for similar shortened names
				for n in names:
					#if match, cat the files
					if shortname == names[n]:
						#dictionary key will be shortname
						filename = shortname
						#temporary name of matching file
						tempname = n
						#delete matching file from dictionary
						del names[n]
						
						#cat the files
						if tempname == shortname:
							#if file already combined before, add entry in list of combined files
							comlist[shortname].append(f)
						else:
							#if first time file is combined, create new key with shortened name
							#add entry to list of combined files
							comlist[shortname] = []
							comlist[shortname].append(f)
							comlist[shortname].append(tempname)
						break
				
				#add the original name and shortened name to the dictionary
				names[filename] = shortname
		#create list of all files post-concatenation

		dellist = []		#list of files to delete
		for n in names:		
			#print the files that each concatenated file is composed of
			if n in comlist:

				print '--' + n + ' composed of:'
				receipt.write('--' + n + ' composed of:\n')
				for f in comlist[n]:
					print '  -' + f	
					receipt.write('  -' + f + '\n')
			#if file doesn't have a match
			else:
				print "-" + names[n] + ' composed of:'
				print "  -" + n
				receipt.write("-" + names[n] + ' composed of:\n')
				receipt.write("  -" + n + '\n')
			#get response from user for confirmation	
			while True:
				print 'Is this correct (y/n)? (CTRL-C to quit) '
				receipt.write('Is this correct (y/n)? (CTRL-C to quit) \n')
				resp = raw_input('-->')
				receipt.write('User response: ' + resp + '\n')
				#if not correct
				if resp == 'n':
					#drop files for consideration
					print '***File ignored\n'
					receipt.write('***File ignored\n')
					if n in comlist:
						del comlist[n]
					dellist.append(n)
					break
				#if correct
				elif resp == 'y':
					#add to list of files to do work on
					comfastqfiles.append(names[n])
					print ' '
					break
				#if invalid response
				else:
					print '***INVALID RESPONSE***'
			receipt.write('\n')
		for n in dellist:
			del names[n]	
	###end msu naming convention

	###if cornell naming convention###
	if args.cornell:
		#go through all fastq files
		for f in fastqfiles:
				#check if first character is numeric
				if f[0].isdigit():
					#create shortname for file
					for i,c in enumerate(f):
						if c.isalpha():
							shortname = f[i:]
							filename = f
							#look if other lane already in names
							for n in names:
								if names[n] == shortname:
									#dictionary key will be shortname
									filename = shortname
									#temporary name of matching file
									tempname = n
									#delete matching file from dictionary
									del names[n]
						
									#cat the files
									if tempname == shortname:
										#if file already combined before, add entry in list of combined files
										comlist[shortname].append(f)
									else:
										#if first time file is combined, create new key with shortened name
										#add entry to list of combined files
										comlist[shortname] = []
										comlist[shortname].append(f)
										comlist[shortname].append(tempname)
									break
				
							#add the original name and shortened name to the dictionary
							names[filename] = shortname
							break;
	
		#create list of all files post-concatenation
		dellist = []		#list of files to delete
		for n in names:		
			#print the files that each concatenated file is composed of
			if n in comlist:

				print '--' + n + ' composed of:'
				receipt.write('--' + n + ' composed of:\n')
				for f in comlist[n]:
					print '  -' + f	
					receipt.write('  -' + f + '\n')
			#if file doesn't have a match
			else:
				print "-" + names[n] + ' composed of:'
				print "  -" + n
				receipt.write("-" + names[n] + ' composed of: \n')
				receipt.write("  -" + n + '\n')
			receipt.write('\n')
			#get response from user for confirmation	
			while True:
				print 'Is this correct (y/n)? (CTRL-C to quit) '
				receipt.write('Is this correct (y/n)? (CTRL-C to quit)\n')
				resp = raw_input('-->')
				receipt.write('User response: '+ resp + '\n')
				#if not correct
				if resp == 'n':
					#drop files for consideration
					print '***File ignored\n'
					receipt.write('***File ignored\n')
					if n in comlist:
						del comlist[n]
					dellist.append(n)
					break
				#if correct
				elif resp == 'y':
					#add to list of files to do work on
					comfastqfiles.append(names[n])
					print ' '
					break
				#if invalid response
				else:
					print '***INVALID RESPONSE***'
					receipt.write('***INVALID RESPONSE***\n')
			receipt.write('\n')
		for n in dellist:
			del names[n]	
						
	###end cornell naming convention

	#return list of files to do work on
	return comfastqfiles, comlist, names
###------------###
###end function###


####----------------------------------------------###
### Create the file list to pass to the qsub file ###
###-----------------------------------------------###
def create_filelist(comfastqfiles, receipt):
	names = {} #dictionary of files and their shortened names
	read1files = [] #list of R1 files
	read2files = [] #list of R2 files
	snames = []  #list of shortened names of R files
	#go through each file to do work on
	for n in comfastqfiles:
		#find the type of end read
		for i, ltr in enumerate(n):
			if ltr == 'R'and n[i+1].isdigit():
				#generate shortname
				shortname = n[:i]+n[i+3:]
				#check if shortname already in dictionary
				if shortname in names:
					num = n[i+1]
					#if R1 file
					if n[i+1] == '1':
						print 'R1: ' + n
						receipt.write('R1: ' + n + '\n')
						print 'R2: ' + names[shortname]
						receipt.write('R2: ' + names[shortname] + '\n')
						
					#if R2 file
					else:
						print 'R1: ' + names[shortname]
						receipt.write('R1: ' + names[shortname] + '\n')
						print 'R2: ' + n
						receipt.write('R2: ' + n + '\n')
						
					#get user confirmation
					while True:
						print 'Do these files go together (y/n)? (CTRL-C to quit)'
						receipt.write('Do these files go together (y/n)? (CTRL-C to quit) \n')
						resp = raw_input('-->')
						receipt.write('User response: ' + resp + '\n')
						#if no
						if resp == 'n':
							#ignore files
							print '***Files ignored\n'
							receipt.write('***Files ignored\n')
							break
						#if yes
						elif resp == 'y':
							print ' '
							receipt.write('\n')
							#check type of read, and add to lists
							if num == '1':
								read1files.append(n)
								read2files.append(names[shortname])	
							else:
								read2files.append(n)
								read1files.append(names[shortname])	
							break			
						else:
							print '***INVALID RESPONSE***'
							receipt.write('***INVALID RESPONSE*** \n')				
					snames.append(shortname)
					del names[shortname]
				#if shortname not in dictionary, add it
				else:
					names[shortname] = n

	if len(snames)>1:
		while True:
			print "More than one sample detected."
			receipt.write("More than one sample detected.\n")
			print "Choose sample number to send to hpcc:"
			receipt.write("Choose sample number to send to hpcc:\n")
			for i,s in enumerate(snames):
				print str(i)+". "+s
				receipt.write(str(i)+". "+s+"\n")
			resp = raw_input('-->')
			if not resp.isdigit():
				print "Invalid response!\n"
			elif int(resp) > len(snames)-1:
				print "Invalid response!\n"
			else:
				r_int=int(resp)
				receipt.write("--> " + resp + '\n\n')
				print ''
				filelist=zip([snames[r_int]],[read1files[r_int]],[read2files[r_int]])
				break
	elif len(snames)==1:
		filelist=zip(snames,read1files,read2files)	

	else:
		print "No file matches detected!\n ---Exiting program---"
		exit()

	#create zip of file names
	print 'Unmatched files:'
	receipt.write('Unmatched files:\n')
	empty = True
	for n in names:
		print '-' + names[n]
		receipt.write('-' + names[n] + '\n')
		empty = False
	if empty:
		print '-No unmatched files'	
		receipt.write('-No unmatched files\n')
	print ' '
	receipt.write('\n')
	return filelist, read1files, read2files			
###------------###
###end function###	


###--------------###
### Manage files ###
###--------------###
def manage_files(args, comlist, names, read1files, read2files, receipt, filelist):
	clean = True
	print "--STARTING FILE MANAGEMENT--"
	receipt.write("--STARTING FILE MANAGEMENT--\n")
	#go through list of combined files
	for n in comlist:
		#if files have matching end read, i.e. are used in qsub
		if n in filelist[0]:
			#check if already in output folder to avoid unnecessary duplication
			if n not in os.listdir(args.outputdir):
				#concatenate files
				print "concatenating:"
				receipt.write("concatenating:\n")
				command = 'cat'
				comlist[n].sort()
				for f in comlist[n]:
					print " -" + f
					receipt.write(" -" + f)
					command = command + ' ' + args.inputdir + '/' + f
				print " -->" + n
				receipt.write(" from " + args.inputdir)
				receipt.write(" --> " + n + " in " + args.outputdir + '\n')
				command =  command + ' > ' + args.outputdir + '/' + n 
				os.system(command)
				clean = False
				print ' '
				receipt.write('\n')
	#if files didn't have multiple lanes
	for n in names:
		#if files have matching end read, i.e. are used in qsub
		if names[n] in filelist[0]:
			#check if already in output folder to avoid unnecessary duplication
			if names[n] not in os.listdir(args.outputdir):
				#copy the file over to avoid altering original
				print "copying:\n -"+n+"\n -->"+names[n]
				receipt.write("copying:\n -"+n+"\n -->"+names[n] + '\n')
				os.system('cp ' + args.inputdir + '/' +n + ' ' + args.outputdir + '/' + names[n])
				clean = False
				print ' '
				receipt.write('\n')
	
	#if no file management, indicate it
	if clean:
		print '-No file management necessary'
		receipt.write('-No file management necessary\n')
			

###------------###
###end function###


###------------------###
### Create qsub file ###
###------------------###
def create_qsub(filelist,args):
	#create contents of qsub file
	for sname,r1,r2 in filelist:
		cli = """
### define resources needed:
### walltime - how long you expect the job to run
#PBS -l walltime={7}

### nodes:ppn - how many nodes & cores per node (ppn) that you require
#PBS -l nodes=1:ppn={5}

### mem: amount of memory that the job will need
#PBS -l mem={6}gb

### you can give your job a name for easier identification
#PBS -N {2}_analysis

###outputs
#PBS -e {3}/error.txt
#PBS -o {3}/output.txt

### load necessary modules, e.g.
module load screed
module load khmer/0.8
module load Trimmomatic/0.32
module load FASTX

### change to the working directory where your code is located
cd {3}

# make a temp directory
mkdir {2}_trim

echo "---------------" >> receipt.txt
echo "Trimmomatic for {2}:" >> receipt.txt
echo "---------------" >> receipt.txt

# run trimmomatic
java -jar /opt/software/Trimmomatic/0.30/trimmomatic PE -threads {5} {0} {1} ./{2}_trim/{2}_s1_pe ./{2}_trim/{2}_s1_se ./{2}_trim/{2}_s2_pe ./{2}_trim/{2}_s2_se ILLUMINACLIP:/opt/software/Trimmomatic/0.30/adapters/TruSeq3-PE.fa:2:30:10 2>> receipt.txt

echo "--------------" >> receipt.txt
echo "Interleave for {2}:" >> receipt.txt
echo "--------------" >> receipt.txt
# interleave the remaining paired-end files
/opt/software/khmer/0.8--GCC-4.4.5/bin/interleave-reads.py ./{2}_trim/{2}_s1_pe ./{2}_trim/{2}_s2_pe 2>> receipt.txt | /mnt/home/{4}/bin/pigz  -p {5} -9c > ./{2}.pe.fq.gz 

# combine the single-ended files
cat ./{2}_trim/{2}_s1_se ./{2}_trim/{2}_s2_se | /mnt/home/{4}/bin/pigz -p {5} -9c > ./{2}.se.fq.gz

echo "-----------------------------" >> receipt.txt
echo "Paired-End Quality Filter for {2}:" >> receipt.txt
echo "-----------------------------" >> receipt.txt
#quality filter paired-end reads
/mnt/home/{4}/bin/unpigz -p {5} -c {2}.pe.fq.gz | fastq_quality_filter -Q33 -q 30 -p 50 -v 2>> receipt.txt | /mnt/home/{4}/bin/pigz -p {5} -9c > {2}.pe.qc.fq.gz 

echo "-----------------------------" >> receipt.txt
echo "Single-End Quality Filter for {2}:" >> receipt.txt
echo "-----------------------------" >> receipt.txt
#quality filter single-end reads
/mnt/home/{4}/bin/unpigz -p {5} -c {2}.se.fq.gz | fastq_quality_filter -Q33 -q 30 -p 50 -v 2>> receipt.txt | /mnt/home/{4}/bin/pigz -p {5} -9c > {2}.se.qc.fq.gz

#remove orphans
extract-paired-reads.py {2}.pe.qc.fq.gz

#clean up output directory
rm *.fastq.gz
rm -r *_trim
rm *.pe.fq.gz *.se.fq.gz
rm *.pe.qc.fq.gz
rm *.qsub

#rename paired-end file
for i in *.pe.qc.fq.gz.pe; do mv $i $"$(basename $i .pe.qc.fq.gz.pe).pe.qc.fq"; done
         """.format(r1,r2,sname,args.outputdir,getpass.getuser(),args.threads,args.reqmem,args.walltime)

		if args.split:
			cli = cli + """
#split the paired reads
echo "--------------------------" >> receipt.txt
echo "Splitting Paired Reads for {2}:" >> receipt.txt
echo "--------------------------" >> receipt.txt
split-paired-reads.py {2}.pe.qc.fq 2>> receipt.txt
mv {2}.pe.qc.fq.1 {0}.pe.qc.fq
mv {2}.pe.qc.fq.2 {1}.pe.qc.fq
rm {2}.pe.qc.fq
         """.format(r1,r2,sname,args.outputdir,getpass.getuser(),args.threads,args.reqmem,args.walltime)
		
		cli = cli + """
for i in *.pe.qc.fq; do gzip $i; done

#combine single-end files
for i in *.pe.qc.fq.gz.se
do
	otherfile="$(basename $i .pe.qc.fq.gz.se).se.qc.fq.gz"
	gunzip -c $otherfile > combine
	cat $i >> combine
	gzip -c combine > $otherfile
	rm $i combine

done
        """.format(r1,r2,sname,args.outputdir,getpass.getuser(),args.threads,args.reqmem,args.walltime)

		cli += """
qstat -f ${PBS_JOBID}
"""
 
		#create and write to qsub file       
		filename='{1}/{0}_analysis.qsub'.format(sname,os.path.join(args.outputdir))
		f= open(filename,'w+')
		f.write(cli)
		f.close()
		
		#create command line option and call subprocess
		cli_parts=["qsub",filename]
		#ft = subprocess.call(cli_parts)

		
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
	parser = argparse.ArgumentParser(description='Submit Raw Reads to Trimmomatic and QC Pipeline.')
	parser.add_argument('-m', '--msu', help='Use MSU naming convention (default)', action ="store_true")
	parser.add_argument('-c', '--cornell', help='Use Cornell naming convention', action ="store_true")
	parser.add_argument('-t', '--threads', help='Number of threads to use (default=1)', action ="store")
	parser.add_argument('-r', '--reqmem', help='Amount of memory to request (in gb) (default=12gb)', action="store")
	parser.add_argument('-w', '--walltime', help='Walltime to request (hh:mm:ss) (default=04:00:00)', action ="store")
	parser.add_argument('-s', '--split', help='Split paired reads', action="store_true")	
	parser.add_argument('-i', '--inputdir', help = 'The Input directory', action = FullPaths)
	parser.add_argument('-o', '--outputdir', help='The Output directory', action = FullPaths)
	return parser.parse_args()
###------------###
###end function###	


#call main function upon program start
if __name__ == '__main__':
    main()
