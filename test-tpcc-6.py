import os, sys, re, os.path
import platform
import subprocess, datetime, time, signal
import csv
import pathlib

dir = pathlib.Path(__file__).stem
os.makedirs(dir,exist_ok=True)

csv_header = 'WORKLOAD,CC_ALG,THREAD_CNT,CENTRAL_INDEX,MAX_TXN_PER_PART,NUM_WH,txn_cnt,abort_cnt,run_time,time_wait,time_ts_alloc,time_man,time_index,time_abort,time_cleanup,latency,deadlock_cnt,cycle_detect,dl_detect_time,dl_wait_time,time_query'.split(',')
csv_data = [csv_header]
log_file = os.path.join(dir,'stdout.log')

def replace(filename, pattern, replacement):
	f = open(filename)
	s = f.read()
	f.close()
	s = re.sub(pattern,replacement,s)
	f = open(filename,'w')
	f.write(s)
	f.close()

dbms_cfg = ["config-std.h", "config.h"]
algs = ['DL_DETECT', 'NO_WAIT', 'HEKATON', 'SILO', 'TICTOC']
n_repeat = 5

def insert_jobs_5a(jobs):
	for alg in ['SILO','TICTOC']:
		for core in [1,2,4,8,12,16,20,28,42,56,84,112,140,168,196,224]:
			jobs.append({
				'WORKLOAD'			: 'TPCC',
				'THREAD_CNT'		: core,
				'CC_ALG'			: alg,
				'CENTRAL_INDEX'		: 'true',
                'MAX_TXN_PER_PART'  : 100000,
                'NUM_WH'            : 1,
				'CPU_FREQ'			: 2.095,
			})

def insert_jobs_5b(jobs):
	for alg in ['SILO','TICTOC']:
		for core in [1,2,4,8,12,16,20,28,42,56,84,112,140,168,196,224]:
			jobs.append({
				'WORKLOAD'			: 'TPCC',
				'THREAD_CNT'		: core,
				'CC_ALG'			: alg,
				'CENTRAL_INDEX'		: 'true',
                'MAX_TXN_PER_PART'  : 100000,
                'NUM_WH'            : 4,
				'CPU_FREQ'			: 2.095,
			})

def insert_jobs_5c(jobs):
	for alg in ['SILO','TICTOC']:
		for core in [1,2,4,8,12,16,20,28,42,56,84,112,140,168,196,224]:
			jobs.append({
				'WORKLOAD'			: 'TPCC',
				'THREAD_CNT'		: core,
				'CC_ALG'			: alg,
				'CENTRAL_INDEX'		: 'true',
                'MAX_TXN_PER_PART'  : 100000,
                'NUM_WH'            : core,
				'CPU_FREQ'			: 2.095,
			})

def test_compile(job):
	os.system("cp "+ dbms_cfg[0] +' ' + dbms_cfg[1])
	for (param, value) in job.items():
		pattern = r"\#define\s*" + re.escape(param) + r'.*'
		replacement = "#define " + param + ' ' + str(value)
		replace(dbms_cfg[1], pattern, replacement)
	os.system("make clean > temp.out 2>&1")
	ret = os.system("make -j8 > temp.out 2>&1")
	if ret != 0:
		print("ERROR in compiling job=")
		print(job)
		exit(0)
	print("PASS Compile\t\talg=%s,\tworkload=%s" % (job['CC_ALG'], job['WORKLOAD']))

def test_run(test = '', job=None):
	with open(log_file,'a') as f:
		f.write(str(job)+"\n")
		f.write(str(datetime.datetime.now())+"\n\n")
	app_flags = ""
	if test == 'read_write':
		app_flags = "-Ar -t1"
	if test == 'conflict':
		app_flags = "-Ac -t4"
	cmd = "time numactl --interleave=all ./rundb %s" % (app_flags)
	start = datetime.datetime.now()
	process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
	timeout = 1800 # in second
	while process.poll() is None:
		time.sleep(1)
		now = datetime.datetime.now()
		if (now - start).seconds > timeout:
			os.kill(process.pid, signal.SIGKILL)
			os.waitpid(-1, os.WNOHANG)
			print("ERROR. Timeout cmd=%s" % cmd)
			exit(0)
	result = process.stdout.read().decode()
	with open(log_file,'a') as f:
		f.write(result)
		f.write(str(datetime.datetime.now())+"\n\n")
	if "PASS" in result:
		if test != '':
			print("PASS execution. \talg=%s,\tworkload=%s(%s),\tcore=%d" % \
				(job["CC_ALG"], job["WORKLOAD"], test, job["THREAD_CNT"],))
		else :
			print("PASS execution. \talg=%s,\tworkload=%s,\tcore=%d" % \
				(job["CC_ALG"], job["WORKLOAD"], job["THREAD_CNT"]))
		for line in result.splitlines():
			l,n = re.subn(r'^\[summary\] *','',line)
			if n == 1:
				d = dict([ x.split('=') for x in re.split(', *',l) ])
				d.update(job)
				r = [ d.get(x) for x in csv_header ]
				csv_data.append(r)
		return
	print("FAILED execution. cmd = %s" % cmd)
	exit(0)

def write_csv(data,fname):
	with open(fname,'w') as f:
		w = csv.writer(f)
		w.writerows(data)
	print('wrote '+fname)

def run_all_test(jobs,tag) :
	for job in jobs:
		test_compile(job)
		if job['WORKLOAD'] == 'TEST':
			test_run('read_write', job)
			#test_run('conflict', job)
		else :
			#os.system("./clock_test | tee -a "+log_file)
			for i in range(n_repeat):
				print(job)
				test_run('', job)
				write_csv(csv_data,os.path.join(dir,'bench_'+tag+'.csv'))
	csv_data.clear()
	csv_data.append(csv_header)
	jobs.clear()


with open(log_file,'w') as f:
	f.write('')

os.system('make clock_test > temp.out 2>&1')
os.system("./clock_test | tee -a "+log_file)

jobs = []

# run YCSB tests

insert_jobs_5a(jobs)
run_all_test(jobs,'5a')

insert_jobs_5b(jobs)
run_all_test(jobs,'5b')

insert_jobs_5c(jobs)
run_all_test(jobs,'5c')

os.system("./clock_test | tee -a "+log_file)
os.system('cp config-std.h config.h')
os.system('make clean > temp.out 2>&1')
os.system('rm temp.out')
