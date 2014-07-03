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
	print '------------- ELECTRIC EEL POND STEP 3 -------------'
	print '****************************************************'

	print "DATE: " + str(datetime.datetime.today())
	#get input arguments
	args = get_args()
	
	if os.path.basename(args.ref) != 'Trinity.fasta':
		print "***ERROR***"
		print "Reference file not 'Trinity.fasta' "
		exit()
	
	#Check if ref file exists
	if os.path.exists(os.path.join(args.ref)) == False:
		print "***ERROR***\n"+args.ref+"\n*Ref file does not exist*"
		exit()
	#Check if left file exist
	if os.path.exists(os.path.join(args.left)) == False:
		print "***ERROR***\n"+args.left+"\n*Left file does not exist*"
		exit()
	#Check if right file exist
	if os.path.exists(os.path.join(args.right)) == False:
		print "***ERROR***\n"+args.right+"\n*Right file does not exist*"
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
			print "Memory to allocate: (in Gb)"
			reqmem = raw_input('-->')
			if reqmem.isdigit(): #user has indicated how much memory to use.
				args.reqmem = reqmem
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

	receipt = open(args.outputdir + "/receipt.txt", "w+")
	receipt.write("----BEGIN RECEIPT----\n")
	receipt.write("DATE: " + str(datetime.datetime.today())+"\n")
	receipt.write("-----------------------------------------\n")
	#write parameters to receipt.
	receipt.write("-Walltime: " + args.walltime + '\n')
	receipt.write("-Required Memory: " + args.reqmem + '\n')
	receipt.write("-Threads: " + args.threads + '\n')

	#get file confirmation before submitting to qsub
	print "-Ref file:   " + args.ref
	print "-Left file:  " + args.left
	print "-Right file: " + args.right
	print "-Output dir: " + args.outputdir
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
	
	receipt.close()

	jobname = os.path.basename(args.outputdir)
	#write the qsub file.
	cli = '''
### define resources needed:
### walltime - how long you expect the job to run
#PBS -l walltime={2}

### nodes:ppn - how many nodes & cores per node (ppn) that you require
#PBS -l nodes=1:ppn={3}

### mem: amount of memory that the job will need
#PBS -l mem={4}gb

### you can give your job a name for easier identification
#PBS -N {6}

###outputs
#PBS -e {5}/error.txt
#PBS -o {5}/output.txt

echo "---------------------" >> {5}/receipt.txt
echo "Trinity Output Below:" >> {5}/receipt.txt
echo "---------------------" >> {5}/receipt.txt

#load necessary modules
module load trinity/20140413p1
module load bowtie
module load RSEM
module load samtools

#call Trinity
align_and_estimate_abundance.pl --transcripts {7} --seqType fq --left {0} --right {1} --thread_count {3} --output_dir {5} --est_method RSEM --aln_method bowtie >> {5}/receipt.txt --prep_reference

'''.format(args.left, args.right, args.walltime, args.threads, args.reqmem, args.outputdir, jobname, args.ref)

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
	parser.add_argument('--ref',  help = 'The reference file')
	parser.add_argument('--left', help = 'The left read file')
	parser.add_argument('--right', help = 'The right read file')
	parser.add_argument('-o', '--outputdir', help='The output directory', action = FullPaths)
	parser.add_argument('-t', '--threads', help='Number of threads to use', action ="store")
	parser.add_argument('-r', '--reqmem', help='Amount of memory to request for Trinity (in gb)', action="store")
	parser.add_argument('-w', '--walltime', help='Walltime to request (hh:mm:ss) (default=20:00:00)', action ="store")
	
	return parser.parse_args()


#call main function upon program start
if __name__ == '__main__':
	main()
