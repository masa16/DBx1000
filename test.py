import os, sys, re, os.path
import platform
import subprocess, datetime, time, signal
import csv
import pathlib

dir = pathlib.Path(__file__).stem
os.makedirs(dir,exist_ok=True)

csv_header = 'WORKLOAD,CC_ALG,THREAD_CNT,CENTRAL_INDEX,MAX_TUPLE_SIZE,SYNTH_TABLE_SIZE,ZIPF_THETA,READ_PERC,WRITE_PERC,REQ_PER_QUERY,txn_cnt,abort_cnt,run_time,time_wait,time_ts_alloc,time_man,time_index,time_abort,time_cleanup,latency,deadlock_cnt,cycle_detect,dl_detect_time,dl_wait_time,time_query'.split(',')
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
n_repeat = 10

def insert_jobs_2a(jobs,workload):
	for alg in ['SILO']:
		#for core in [4]:
		for core in [28, 56, 84, 112, 140, 168, 196, 224]:
			jobs.append({
				'WORKLOAD'			: workload,
				'THREAD_CNT'		: core,
				'CC_ALG'			: alg,
				'CENTRAL_INDEX'		: 'true',
				'MAX_TUPLE_SIZE'	: 4,
				'SYNTH_TABLE_SIZE'	: 50,
				'ZIPF_THETA'		: 0,
				'READ_PERC'			: 1,
				'WRITE_PERC'		: 0,
				'REQ_PER_QUERY'		: 10,
				'MAX_ROW_PER_TXN'	: 10,
				'MAX_TXN_PER_PART'	: 3000000,
				'CPU_FREQ'			: 2.095,
			})

def insert_jobs_2b(jobs,workload):
	for alg in ['SILO']:
		for w in range(11):
			jobs.append({
				'WORKLOAD'			: workload,
				'THREAD_CNT'		: 224,
				'CC_ALG'			: alg,
				'CENTRAL_INDEX'		: 'true',
				'MAX_TUPLE_SIZE'	: 4,
				'SYNTH_TABLE_SIZE'	: 50,
				'ZIPF_THETA'		: 0,
				'READ_PERC'			: (10-w)*0.1,
				'WRITE_PERC'		: w*0.1,
				'REQ_PER_QUERY'		: 10,
				'MAX_ROW_PER_TXN'	: 10,
				'MAX_TXN_PER_PART'	: 100000,
				'CPU_FREQ'			: 2.095,
			})

def insert_jobs_3a(jobs,workload):
	for alg in ['SILO','TICTOC']:
		for n_thread in [1,10,20,30,40,50,60,70,80]:
			jobs.append({
				'WORKLOAD'			: workload,
				'THREAD_CNT'		: n_thread,
				'CC_ALG'			: alg,
				'CENTRAL_INDEX'		: 'true',
				'MAX_TUPLE_SIZE'	: 1000,
				'SYNTH_TABLE_SIZE'	: 10000000,
				'ZIPF_THETA'		: 0,
				'READ_PERC'			: 1,
				'WRITE_PERC'		: 0,
				'REQ_PER_QUERY'		: 2,
				'MAX_ROW_PER_TXN'	: 2,
				'MAX_TXN_PER_PART'	: 2000000,
				'CPU_FREQ'			: 2.095,
			})

def insert_jobs_3b(jobs,workload):
	for alg in ['SILO','TICTOC']:
		for n_thread in [1,10,20,30,40,50,60,70,80]:
			jobs.append({
				'WORKLOAD'			: workload,
				'THREAD_CNT'		: n_thread,
				'CC_ALG'			: alg,
				'CENTRAL_INDEX'		: 'true',
				'MAX_TUPLE_SIZE'	: 1000,
				'SYNTH_TABLE_SIZE'	: 10000000,
				'ZIPF_THETA'		: 0.8,
				'READ_PERC'			: 0.9,
				'WRITE_PERC'		: 0.1,
				'REQ_PER_QUERY'		: 16,
				'MAX_ROW_PER_TXN'	: 16,
				'MAX_TXN_PER_PART'	: 200000,
				'CPU_FREQ'			: 2.095,
			})

def insert_jobs_4a(jobs,workload):
	for alg in ['SILO','TICTOC']:
		for n_thread in [1,5,10,15,20,25]:
			jobs.append({
				'WORKLOAD'			: workload,
				'THREAD_CNT'		: n_thread,
				'CC_ALG'			: alg,
				'CENTRAL_INDEX'		: 'true',
				'MAX_TUPLE_SIZE'	: 100,
				'SYNTH_TABLE_SIZE'	: 10000000,
				'ZIPF_THETA'		: 0.99,
				'READ_PERC'			: 0.5,
				'WRITE_PERC'		: 0.5,
				'REQ_PER_QUERY'		: 16,
				'MAX_ROW_PER_TXN'	: 16,
				'MAX_TXN_PER_PART'	: 100000,
				'CPU_FREQ'			: 2.095,
			})

def insert_jobs_4b(jobs,workload):
	for alg in ['SILO','TICTOC']:
		for skew in [0,1,2,3,4,5,6,7,8,9,9.5,9.7,9.9]:
			jobs.append({
				'WORKLOAD'			: workload,
				'THREAD_CNT'		: 28,
				'CC_ALG'			: alg,
				'CENTRAL_INDEX'		: 'true',
				'MAX_TUPLE_SIZE'	: 100,
				'SYNTH_TABLE_SIZE'	: 10000000,
				'ZIPF_THETA'		: skew*0.1,
				'READ_PERC'			: 0.5,
				'WRITE_PERC'		: 0.5,
				'REQ_PER_QUERY'		: 16,
				'MAX_ROW_PER_TXN'	: 16,
				'MAX_TXN_PER_PART'	: 100000,
				'CPU_FREQ'			: 2.095,
			})

def insert_jobs_4c(jobs,workload):
	for alg in ['SILO','TICTOC']:
		for skew in [0,1,2,3,4,5,6,7,8,9,9.5,9.7,9.9]:
			jobs.append({
				'WORKLOAD'			: workload,
				'THREAD_CNT'		: 28,
				'CC_ALG'			: alg,
				'CENTRAL_INDEX'		: 'true',
				'MAX_TUPLE_SIZE'	: 100,
				'SYNTH_TABLE_SIZE'	: 10000000,
				'ZIPF_THETA'		: skew*0.1,
				'READ_PERC'			: 0.95,
				'WRITE_PERC'		: 0.05,
				'REQ_PER_QUERY'		: 16,
				'MAX_ROW_PER_TXN'	: 16,
				'MAX_TXN_PER_PART'	: 500000,
				'CPU_FREQ'			: 2.095,
			})

def insert_jobs_4d(jobs,workload):
	for alg in ['SILO','TICTOC']:
		for n_thread in [1,5,10,15,20,25]:
			jobs.append({
				'WORKLOAD'			: workload,
				'THREAD_CNT'		: n_thread,
				'CC_ALG'			: alg,
				'CENTRAL_INDEX'		: 'true',
				'MAX_TUPLE_SIZE'	: 100,
				'SYNTH_TABLE_SIZE'	: 10000000,
				'ZIPF_THETA'		: 0.99,
				'READ_PERC'			: 0.95,
				'WRITE_PERC'		: 0.05,
				'REQ_PER_QUERY'		: 1,
				'MAX_ROW_PER_TXN'	: 1,
				'MAX_TXN_PER_PART'	: 5000000,
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
n_repeat = 10

insert_jobs_3a(jobs,'YCSB')
run_all_test(jobs,'3a')

insert_jobs_3b(jobs,'YCSB')
run_all_test(jobs,'3b')

insert_jobs_4a(jobs,'YCSB')
run_all_test(jobs,'4a')

insert_jobs_4b(jobs,'YCSB')
run_all_test(jobs,'4b')

insert_jobs_4c(jobs,'YCSB')
run_all_test(jobs,'4c')

insert_jobs_4d(jobs,'YCSB')
run_all_test(jobs,'4d')

insert_jobs_2a(jobs,'YCSB')
run_all_test(jobs,'2a')

n_repeat = 3

insert_jobs_2b(jobs,'YCSB')
run_all_test(jobs,'2b')

os.system("./clock_test | tee -a "+log_file)
os.system('cp config-std.h config.h')
os.system('make clean > temp.out 2>&1')
os.system('rm temp.out')
