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

def main():
	print '****************************************************'
	print '------------- ELECTRIC EEL POND STEP 4 -------------'
	print '****************************************************'

	print "DATE: " + str(datetime.datetime.today())
	#get input arguments
	args = get_args()
	
	#Check if input path exists
	if os.path.exists(os.path.join(args.input)) == False:
		print "***ERROR***\n"+args.input+"\n*Input query does not exist*"
		exit()
	#Check if output path exists
	if os.path.exists(os.path.join(args.outputdir)) == False:
		print "***ERROR***\n"+args.outputdir+"\n*Output path does not exist*"
		exit()

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

	if not args.reqmem:
		while True:
			print "Memory to request? (in Gb):"
			reqmem = raw_input('-->')
			if reqmem.isdigit(): #user has indicated how much memory to use.
				args.reqmem = reqmem
				break
			else:
				print "Invalid response."
	else:
		if args.reqmem.isdigit(): #user has provided reqmem in command.
			print '-Requested memory: ' + args.reqmem + "Gb"
		else:
			print "***ERROR***\nRequired Memory requires a number"
			print "You input: " + args.reqmem
			exit()
	
	#check nodes argument
	if not args.nodes:
		while True:
			print "Nodes to request?"
			reqnodes = raw_input("-->")
			if reqnodes.isdigit():
				args.nodes=reqnodes
			else:
				print "Invalid response."
	else:
		if args.nodes.isdigit():
			print '-Nodes: ' + args.nodes
		else:
			print "***ERROR***\nNodes requires a number"
			print "You input: " + args.nodes
			exit()

	if not args.blasttype:
		while True:
			print "Please enter type of blast to perform ('blastp', 'blastn', 'blastx', 'tblastn', or 'tblastx'):"
			blast = raw_input("-->")
			if blast in ["blastp", "blastn", "blastx", "tblastn", "tblastx"]:
				args.blasttype = blast
				break
			else:
				print "Invlid response."
	else:
		if args.blasttype not in ["blastp", "blastn", "blastx", "tblastn", "tblastx"]:
			print "***ERROR***\nBlast type must be one of the following: 'blastp', 'blastn', 'blastx', 'tblastn', 'tblastx'."
			print "You input: " + args.blasttype
			exit()

#get file confirmation before submitting to qsub
	print "-Input query:   " + args.input
	print "-Database:  " + args.database
	print "-Output directory: " + args.outputdir
	print "-Blast type: " + args.blasttype
	while True:	
		print "Are these the correct inputs? (y/n)"
		user_resp=raw_input("-->")
		if user_resp == 'n':
			print "Incorrect inputs. Exiting program."
			exit()
		elif user_resp == 'y':
			break
		else:
			"*Invalid response*"
			

	receipt = open(args.outputdir + "/receipt.txt", "w+")
	receipt.write("----BEGIN RECEIPT----\n")
	receipt.write("DATE: " + str(datetime.datetime.today())+"\n")
	receipt.write("-----------------------------------------\n")

	#write parameters to receipt.
	receipt.write("-Walltime: " + args.walltime + '\n')
	receipt.write("-Required Memory: " + args.reqmem + '\n')
	receipt.write("-Threads: " + args.threads + '\n')
	receipt.write("-Nodes: " + args.nodes + '\n')
	receipt.write("-Database: " + args.database + '\n')
	receipt.write("-Query: " + args.input + '\n')

	receipt.close()

	jobname = os.path.basename(args.outputdir)
	#write the qsub file.
	cli = '''
### define resources needed:
### walltime - how long you expect the job to run
#PBS -l walltime={0}

### nodes:ppn - how many nodes & cores per node (ppn) that you require
#PBS -l nodes={1}:ppn={2}

### mem: amount of memory that the job will need
#PBS -l mem={3}gb

### you can give your job a name for easier identification
#PBS -N {4}

###outputs
#PBS -e {5}/error.txt
#PBS -o {5}/output.txt

echo "---------------------" >> {5}/receipt.txt
echo "mpiBLAST Output Below:" >> {5}/receipt.txt
echo "---------------------" >> {5}/receipt.txt

###cd /mnt/research/efish

#load necessary modules
module load trinity/20140413p1
module load mpiblast

#call mpiBLAST exec command
mpiexec -n {1} mpiblast -p {6} -d {7} -i {8} -o {5}/results.txt --use-parallel-write --time-profile={5}/time.txt --removedb --copy-via=mpi -e 1e-10 -K 75


'''.format(args.walltime, args.nodes, args.threads, args.reqmem, jobname, args.outputdir, args.blasttype, args.database, args.input)

	cli += '''
qstat -f ${PBS_JOBID}
'''

	#create and write to qsub file       
	filename='{}/BLAST.qsub'.format(os.path.join(args.outputdir))
	f= open(filename,'w+')
	f.write(cli)
	f.close()

	#create command line option and call subprocess
	cli_parts=["qsub",filename]

	ft = subprocess.call(cli_parts)

	print  "\n---Job has been submitted to the hpcc---"

###------------------------###
### Custom argument action ###
###------------------------###
class FullPaths(argparse.Action):
    """Expand user- and relative-paths"""
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, os.path.abspath(os.path.expanduser(values)))
###---------###
###end class###

def get_args():
	parser = argparse.ArgumentParser(description='')
	parser.add_argument('-o', '--outputdir', help='The output directory', action = FullPaths)
	parser.add_argument('-t', '--threads', help='Number of threads to use', action ="store")
	parser.add_argument('-r', '--reqmem', help='Amount of memory to request for BLAST (in gb)', action="store")
	parser.add_argument('-w', '--walltime', help='Walltime to request (hh:mm:ss) (default=20:00:00)', action ="store")
	parser.add_argument('-i', '--input', help='The query file', action ="store")
	parser.add_argument('-d', '--database', help='The database', action="store")
	parser.add_argument('-n', '--nodes', help='Number of nodes (default=1)', action="store")
	parser.add_argument('-b', '--blasttype', help='Type of blast performed', action="store")
	
	return parser.parse_args()

#call main function upon program start
if __name__ == '__main__':
	main()
